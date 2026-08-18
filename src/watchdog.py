"""The watchdog: a dead-man switch for a main loop that stopped making progress.

`src/checker.py` resets the timeout at the top of every iteration. A background
thread counts it down, and when it reaches zero the loop has not come round in
five minutes — which in this service means one of the outbound HTTP calls is
wedged, not that there is nothing to do (an idle loop still resets the timeout
every ten seconds). At that point the only useful move is to kill the process and
let `restart: unless-stopped` bring it back.
"""

import os
import sys
import threading
import time
from typing import Optional

import requests

from src.health import HealthCheckHandler
from src.redact import redact

# Seconds the loop may go without resetting the timeout before the process kills
# itself. The countdown thread ticks once per STEP seconds.
WATCHDOG_TIMEOUT = 300
WATCHDOG_STEP = 10

# With less than this left, /health starts answering 503. The point is to give the
# deployment's auto-heal a chance to restart the container BEFORE the watchdog has
# to take the process down itself — a restart driven by docker's health verdict is
# visible in `docker inspect`, an os._exit() is only visible in the log.
UNHEALTHY_BELOW = 60

# The notification is a courtesy on the way out, not a step that may delay the
# exit: at this point the process is already committed to dying.
TELEGRAM_TIMEOUT = 10

# How much of a refused answer's body goes into the log line. Long enough to
# recognise what answered — an envelope without `ok`, a gateway's own JSON error
# page — short enough that a body somebody else chose the size of cannot push the
# rest of the log out of the reader's screen.
BODY_EXCERPT_LIMIT = 200


def _body_excerpt(payload, limit=BODY_EXCERPT_LIMIT):
    """`payload` rendered for one log line, bounded, for ANY decoded JSON value.

    Total on purpose, because of WHERE it runs: this is on the path to
    `os._exit(1)`, in the branch that reports a refusal, and a renderer that threw
    would replace the explanation with a second failure. `response.json()` returns
    whatever the body decoded to — an object, a list, a string, a number, `null` —
    not the dict the Bot API documents, so nothing here may index, `.get()` or
    assume a length. `repr` is defined for every one of those and slicing a string
    is defined for every length, which is the whole of the implementation.

    `repr` rather than `str` for the same reason it is total: a body that decoded
    to `""` or to `"   "` shows up as `''` and `'   '` rather than as nothing at
    all, and the failure this excerpt exists to end is precisely a log line that
    reads as though the code had nothing to say.
    """
    text = repr(payload)
    return text if len(text) <= limit else text[:limit] + "..."


class GristWatchdog:
    def __init__(self, logger, telegram_bot_token=None, telegram_chat_id=None):
        self._timeout = WATCHDOG_TIMEOUT
        self._lock = threading.Lock()
        self._running = True
        self._thread: Optional[threading.Thread] = None
        self.logger = logger
        self.telegram_bot_token = telegram_bot_token
        self.telegram_chat_id = telegram_chat_id
        self.reset_timeout()
        self.logger.info(f"Watchdog initialized with timeout: {self._timeout} seconds")

    def start(self):
        """Start the countdown thread.

        Deliberately separate from `__init__`: constructing the object and
        spawning a thread that can call `os._exit()` are two different things, and
        the tests exercise the countdown by calling `tick()` directly rather than
        by racing a real thread.
        """
        self._thread = threading.Thread(target=self.decrease_timeout_thread, daemon=True)
        self._thread.start()
        self.logger.info("Watchdog thread started")

    def send_telegram_notification(self, message):
        try:
            if self.telegram_bot_token and self.telegram_chat_id:
                telegram_url = f"https://api.telegram.org/bot{self.telegram_bot_token}/sendMessage"
                response = requests.post(telegram_url, json={
                    "chat_id": self.telegram_chat_id,
                    "text": message
                }, timeout=TELEGRAM_TIMEOUT)
                # The answer is CHECKED — status AND body — because the failure
                # most likely to happen here does not raise. A bot token that no
                # longer authenticates — rotated, revoked, or still carrying the
                # whitespace it arrived with (see `OptionalSecret` in
                # src/settings.py) — is a completely successful HTTP round trip
                # that carries a rejection, 4xx rather than a raised exception; so
                # is a chat id this bot cannot post to. `requests` returns both
                # like any other response, so throwing the result away and logging
                # "sent" below was a claim of delivery with nothing behind it — in
                # the one log line guaranteed to be read, written as the watchdog
                # takes the container down.
                #
                # Raised rather than branched, so the failure lands in the handler
                # below that ALREADY redacts and already guarantees nothing escapes
                # this method: `raise_for_status()` builds its message out of the
                # URL ("401 Client Error: Unauthorized for url:
                # https://api.telegram.org/bot<token>/sendMessage"), so it carries
                # the bot token exactly the way a transport error does.
                response.raise_for_status()
                # The status alone is ALMOST enough — the Bot API mirrors its
                # error_code into the HTTP status, so the two normally agree — and
                # "almost" is why the body is read too. `ok` is the field the API
                # documents as the verdict; the status is a convenience on top of
                # it. Anything that answers 200 while the envelope says no (an
                # intercepting proxy, a gateway with its own error page, a future
                # partial-success shape) would otherwise be written into the log as
                # a delivery, and this is the log line the operator reads to decide
                # whether anybody was told the container died.
                #
                # Both `.json()` on a non-JSON body and this `raise` are new ways
                # for the block to throw, which is exactly why they sit inside the
                # same `try`: see the handler's note — this method runs on the way
                # to os._exit(1) and may not raise at all.
                payload = response.json()
                if not (isinstance(payload, dict) and payload.get("ok")):
                    # The STATUS and an EXCERPT OF THE BODY, both, because between
                    # them they are the whole of what this branch knows and either
                    # one alone leaves the operator guessing.
                    #
                    # It used to report `payload.get("description")` and nothing
                    # else. That is the API's own wording for a refusal ("chat not
                    # found", "bot was blocked by the user"), it is the most useful
                    # thing in the body when it is there, and it survives inside the
                    # excerpt — but it is present only in an answer the Bot API
                    # itself wrote. The answers that get HERE are largely the ones
                    # it did not: a 200 whose envelope has no `ok` at all, a
                    # refusal with no `description` field. Both rendered the line as
                    # "Telegram refused the notification: None", which names neither
                    # the status nor what arrived — in the single log line written
                    # while the watchdog takes the container down, and about exactly
                    # the intercepting proxy / gateway case the body is read for.
                    #
                    # The status is in the text rather than left to the reader
                    # because `raise_for_status()` did NOT fire on these: reaching
                    # this line means 2xx, and "refused" under a success code is the
                    # part that needs saying out loud.
                    #
                    # None of this is about the token — the body is the API's
                    # message about the addressee — and it goes through `redact()`
                    # in the handler regardless.
                    raise RuntimeError(
                        "Telegram refused the notification: HTTP {}, body: {}".format(
                            response.status_code, _body_excerpt(payload)))
                self.logger.info("Telegram notification sent about watchdog timeout")
            else:
                self.logger.warning("Telegram notification skipped - missing bot token or chat ID")
        except Exception as e:
            # A failed notification must never be the reason the process survives a
            # watchdog timeout: this runs on the path to os._exit(). Everything
            # above sits inside the `try` for that reason, the status and body
            # checks included — each is a new way for this method to raise, and
            # this method may not raise at all.
            #
            # The type is named separately and the text is redacted, because the
            # URL above carries TELEGRAM_BOT_TOKEN as a PATH SEGMENT and both
            # `requests` and `raise_for_status()` quote the whole url back inside
            # the text of the error ("Max retries exceeded with url:
            # /bot<token>/sendMessage"). This is also the single log line most
            # likely to be read: it is written at the moment the watchdog fires.
            #
            # Only the token is redacted. The chat id used to be passed here too
            # and is not a credential: it is the addressee, it does not appear in
            # any of these texts, and blind-replacing a short numeric identifier
            # can only damage the message — cutting a matching run of digits out of
            # a request id, a status code or a timestamp. Nothing is bought for
            # that, so it is not done.
            self.logger.error("Failed to send Telegram notification: {}: {}".format(
                type(e).__name__, redact(e, self.telegram_bot_token)))

    def reset_timeout(self):
        with self._lock:
            previous = self._timeout
            self._timeout = WATCHDOG_TIMEOUT
            self.logger.info(f"Watchdog timeout reset from {previous} to {self._timeout} seconds")

    def tick(self):
        """One step of the countdown. Never returns once the timeout is spent."""
        with self._lock:
            self._timeout = max(0, self._timeout - WATCHDOG_STEP)
            self.logger.warning(f"Watchdog timeout decreased: {self._timeout} seconds")
            if self._timeout < UNHEALTHY_BELOW:
                HealthCheckHandler.set_health(False)
            if self._timeout < 1:
                self._die()

    def _die(self):
        """Take the WHOLE PROCESS down, not just this thread.

        This is the bug this module was fixed for. `sys.exit(1)` raises SystemExit
        in the calling thread and nothing else: in a non-main thread it kills that
        thread quietly, the process carries on running, and the watchdog simply
        ceases to exist — while its own log line says it fired. The container then
        stays up forever with no dead-man switch at all, which is the exact state
        the watchdog exists to prevent. `os._exit()` is what actually ends the
        process from a background thread.
        """
        reason = (f"Watchdog timeout reached. No activity detected for {self._timeout} "
                  f"seconds. Restarting application...")
        self.logger.error(reason)
        self.send_telegram_notification(
            f"Watchdog timeout reached. No activity detected for {self._timeout} seconds. "
            f"Application will be restarted.")
        # os._exit() skips every exit hook AND the interpreter's own buffer flush,
        # so without this the reason above can die inside a buffer and the
        # container's log shows a process that vanished for no stated cause. The
        # Dockerfile sets PYTHONUNBUFFERED=1 for the same reason; this covers the
        # runs that do not have it (a hand-run `python main.py`) and the logging
        # handlers' own buffering, which PYTHONUNBUFFERED does not touch.
        for handler in list(getattr(self.logger, "handlers", ())):
            try:
                handler.flush()
            except Exception:
                pass
        sys.stdout.flush()
        sys.stderr.flush()
        os._exit(1)

    def decrease_timeout_thread(self):
        while self._running:
            self.tick()
            time.sleep(WATCHDOG_STEP)

    def get_timeout(self):
        with self._lock:
            return self._timeout
