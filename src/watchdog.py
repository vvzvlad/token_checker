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
                requests.post(telegram_url, json={
                    "chat_id": self.telegram_chat_id,
                    "text": message
                }, timeout=TELEGRAM_TIMEOUT)
                self.logger.info("Telegram notification sent about watchdog timeout")
            else:
                self.logger.warning("Telegram notification skipped - missing bot token or chat ID")
        except Exception as e:
            # A failed notification must never be the reason the process survives a
            # watchdog timeout: this runs on the path to os._exit().
            #
            # The type is named separately and the text is redacted, because the
            # URL above carries TELEGRAM_BOT_TOKEN as a PATH SEGMENT and `requests`
            # quotes the whole url back inside the text of any transport error
            # ("Max retries exceeded with url: /bot<token>/sendMessage"). This is
            # also the single log line most likely to be read: it is written at the
            # moment the watchdog fires.
            self.logger.error("Failed to send Telegram notification: {}: {}".format(
                type(e).__name__, redact(e, self.telegram_bot_token, self.telegram_chat_id)))

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
