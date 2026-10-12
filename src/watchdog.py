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

# With less than this left, /health answers 503, giving auto-heal a chance to
# restart the container (visible in `docker inspect`) before os._exit() does.
UNHEALTHY_BELOW = 60

# The notification is a courtesy on the way out, not a step that may delay the
# exit: at this point the process is already committed to dying.
TELEGRAM_TIMEOUT = 10

# How much of a refused answer's body goes into the log line: enough to recognise
# what answered, bounded because the body's size is someone else's choice.
BODY_EXCERPT_LIMIT = 200


def _body_excerpt(payload, limit=BODY_EXCERPT_LIMIT):
    """`payload` rendered for one log line, bounded, for ANY decoded JSON value.

    Total on purpose: it runs on the path to `os._exit(1)`, and `response.json()`
    may return any JSON value, so nothing here indexes or assumes a dict. `repr`
    so an empty or blank string body is still visible.
    """
    text = repr(payload)
    return text if len(text) <= limit else text[:limit] + "..."


class GristWatchdog:
    def __init__(self, logger, telegram_bot_token=None, telegram_chat_id=None,
                 telegram_bot_api_server=None):
        self._timeout = WATCHDOG_TIMEOUT
        self._lock = threading.Lock()
        self._running = True
        self._thread: Optional[threading.Thread] = None
        self.logger = logger
        self.telegram_bot_token = telegram_bot_token
        self.telegram_chat_id = telegram_chat_id
        # A self-hosted Bot API server; None means the public api.telegram.org.
        self.telegram_bot_api_server = telegram_bot_api_server
        self.reset_timeout()
        self.logger.info(f"Watchdog initialized with timeout: {self._timeout} seconds")

    def start(self):
        """Start the countdown thread.

        Separate from `__init__` so tests can drive the countdown with `tick()`
        instead of a real thread that can call `os._exit()`.
        """
        self._thread = threading.Thread(target=self.decrease_timeout_thread, daemon=True)
        self._thread.start()
        self.logger.info("Watchdog thread started")

    def send_telegram_notification(self, message):
        try:
            if self.telegram_bot_token and self.telegram_chat_id:
                server = (self.telegram_bot_api_server or "https://api.telegram.org").rstrip("/")
                telegram_url = f"{server}/bot{self.telegram_bot_token}/sendMessage"
                response = requests.post(telegram_url, json={
                    "chat_id": self.telegram_chat_id,
                    "text": message
                }, timeout=TELEGRAM_TIMEOUT)
                # The answer is checked (status and body): a bad token or chat id
                # is a normal 4xx response, not an exception. Raised so the handler
                # below redacts it: `raise_for_status()` quotes the URL, bot token
                # included.
                response.raise_for_status()
                # `ok` is the API's documented verdict; a 200 whose envelope says
                # no (a proxy or gateway page) must not be logged as delivered.
                # Inside the `try` because this method may not raise.
                payload = response.json()
                if not (isinstance(payload, dict) and payload.get("ok")):
                    # Status and a body excerpt: answers reaching here often lack
                    # `description`, and the status (2xx) shows "refused under a
                    # success code". Redacted in the handler regardless.
                    raise RuntimeError(
                        "Telegram refused the notification: HTTP {}, body: {}".format(
                            response.status_code, _body_excerpt(payload)))
                self.logger.info("Telegram notification sent about watchdog timeout")
            else:
                self.logger.warning("Telegram notification skipped - missing bot token or chat ID")
        except Exception as e:
            # This runs on the path to os._exit(), so it must never raise. The text
            # is redacted because the URL carries TELEGRAM_BOT_TOKEN as a path
            # segment and `requests` quotes it. The chat id is not a credential and
            # is not redacted.
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

        `sys.exit(1)` in a non-main thread only ends that thread; `os._exit()` ends
        the process.
        """
        reason = (f"Watchdog timeout reached. No activity detected for {self._timeout} "
                  f"seconds. Restarting application...")
        self.logger.error(reason)
        self.send_telegram_notification(
            f"Watchdog timeout reached. No activity detected for {self._timeout} seconds. "
            f"Application will be restarted.")
        # os._exit() skips exit hooks and buffer flushes. PYTHONUNBUFFERED in the
        # Dockerfile does not cover hand runs or the logging handlers' buffers.
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
