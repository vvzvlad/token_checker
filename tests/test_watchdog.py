"""The watchdog: the countdown, the health flag, and the way it kills the process.

The last of those is why this file exists. `decrease_timeout_thread` used to call
`sys.exit(1)`, which in a NON-MAIN thread raises SystemExit in that thread only:
the countdown thread died quietly, the process kept running, and the watchdog
stopped existing — after logging that it had fired. The container then ran
forever with no dead-man switch and nothing anywhere said so.

So `os._exit` is not an implementation detail here, it is the fix, and it is
pinned: called exactly once, with code 1, and only after the reason has been
logged. No thread is started in this file — `tick()` is driven by hand, which is
what makes the countdown testable at all instead of a five-minute wait.
"""

import os

import pytest

import src.watchdog
from src.health import HealthCheckHandler
from src.watchdog import UNHEALTHY_BELOW, WATCHDOG_STEP, WATCHDOG_TIMEOUT, GristWatchdog


class FakeExit:
    """Stands in for os._exit, which would otherwise end the test run itself.

    It also takes a SNAPSHOT of whatever the test asks for, at the moment it is
    called. That is the difference between this double and the real thing, and it
    is the whole point: `os._exit` never returns, so everything the implementation
    does after it is code that will never run in production. A test that inspects
    the log after `_run_to_death()` returns is inspecting state that only exists
    because this fake returned — and it would pass an implementation that exits
    first and logs afterwards, i.e. one whose reason for dying is lost forever.
    """

    def __init__(self):
        self.calls = []
        self.snapshots = []
        # Set by a test that needs the state at call time; left None by the tests
        # that only count the calls.
        self.snapshot = None

    def __call__(self, code):
        self.calls.append(code)
        self.snapshots.append(self.snapshot() if self.snapshot is not None else None)


@pytest.fixture
def no_exit(monkeypatch):
    fake = FakeExit()
    # Patched on the os module the watchdog imported. Restored by monkeypatch, so
    # the rest of the suite gets the real os._exit back.
    monkeypatch.setattr(src.watchdog.os, "_exit", fake)
    return fake


@pytest.fixture
def no_telegram(monkeypatch):
    """Nothing in this suite may reach api.telegram.org."""
    posts = []

    def fake_post(url, json=None, timeout=None):
        posts.append((url, json, timeout))
        return None

    monkeypatch.setattr(src.watchdog.requests, "post", fake_post)
    return posts


def run_down(watchdog, ticks):
    for _ in range(ticks):
        watchdog.tick()


# --- the countdown -----------------------------------------------------------

def test_a_fresh_watchdog_starts_at_the_full_timeout(logger):
    assert GristWatchdog(logger).get_timeout() == WATCHDOG_TIMEOUT


def test_each_tick_takes_one_step_off(logger):
    watchdog = GristWatchdog(logger)
    watchdog.tick()
    assert watchdog.get_timeout() == WATCHDOG_TIMEOUT - WATCHDOG_STEP
    watchdog.tick()
    assert watchdog.get_timeout() == WATCHDOG_TIMEOUT - 2 * WATCHDOG_STEP


def test_reset_puts_the_full_timeout_back(logger):
    # The main loop calls this at the top of every iteration; it is the "I am
    # still going round" signal the whole mechanism rests on.
    watchdog = GristWatchdog(logger)
    run_down(watchdog, 5)
    watchdog.reset_timeout()
    assert watchdog.get_timeout() == WATCHDOG_TIMEOUT


def test_no_thread_is_started_until_start_is_called(logger):
    # Constructing the object must not spawn something that can call os._exit:
    # `start()` is the separate, deliberate step.
    watchdog = GristWatchdog(logger)
    assert watchdog._thread is None


# --- the health flag ---------------------------------------------------------

def test_the_endpoint_stays_healthy_while_there_is_time_left(logger, health_flag):
    watchdog = GristWatchdog(logger)
    # Down to exactly the threshold — still healthy, the comparison is strict.
    while watchdog.get_timeout() > UNHEALTHY_BELOW:
        watchdog.tick()
    assert watchdog.get_timeout() == UNHEALTHY_BELOW
    assert HealthCheckHandler.is_healthy is True


def test_dropping_below_the_threshold_turns_the_endpoint_unhealthy(logger, health_flag):
    # This is the point of the threshold: the deployment's auto-heal gets to
    # restart the container off docker's health verdict BEFORE the watchdog has
    # to take the process down itself, and a restart is visible where an
    # os._exit() is only a log line.
    watchdog = GristWatchdog(logger)
    while watchdog.get_timeout() >= UNHEALTHY_BELOW:
        watchdog.tick()
    assert watchdog.get_timeout() < UNHEALTHY_BELOW
    assert HealthCheckHandler.is_healthy is False


# --- the exit ----------------------------------------------------------------

def _run_to_death(watchdog):
    """Tick exactly as many times as the timeout allows, and no more.

    Exactly, because the timeout floors at 0 and every FURTHER tick would satisfy
    `< 1` again — in production the real os._exit ends the process on the first
    one, so a test that ticked past it would be counting calls that can never
    happen. Safe only with os._exit patched out.
    """
    run_down(watchdog, WATCHDOG_TIMEOUT // WATCHDOG_STEP)
    assert watchdog.get_timeout() == 0


def test_the_spent_timeout_kills_the_whole_process(logger, health_flag, no_exit, no_telegram):
    watchdog = GristWatchdog(logger)
    _run_to_death(watchdog)
    # Exactly once, with code 1 — and os._exit, not sys.exit: in a background
    # thread sys.exit kills only that thread, leaving a live process with no
    # watchdog and a log line claiming otherwise.
    assert no_exit.calls == [1]


def test_the_reason_is_logged_before_the_process_dies(logger, health_flag, no_exit, no_telegram):
    # os._exit skips every buffer flush, so a reason logged too late is a reason
    # nobody ever reads: the container just vanishes and comes back. Asserted
    # against the snapshot taken INSIDE os._exit rather than against the logger
    # afterwards — after is a moment that does not exist in production.
    no_exit.snapshot = lambda: list(logger.error_messages)
    watchdog = GristWatchdog(logger)
    _run_to_death(watchdog)
    assert no_exit.calls == [1]
    assert any("Watchdog timeout reached" in message for message in no_exit.snapshots[0])


def test_a_logger_handler_is_flushed_on_the_way_out(logger, health_flag, no_exit, no_telegram):
    class RecordingHandler:
        def __init__(self):
            self.flushed = 0

        def flush(self):
            self.flushed += 1

    handler = RecordingHandler()
    logger.handlers.append(handler)
    # Same reason as above: a flush that happens after os._exit is a flush that
    # never happens, and the reason dies inside a buffer.
    no_exit.snapshot = lambda: handler.flushed
    watchdog = GristWatchdog(logger)
    _run_to_death(watchdog)
    assert no_exit.calls == [1]
    assert no_exit.snapshots[0] >= 1


# --- the telegram notification ----------------------------------------------

def test_telegram_is_notified_when_both_variables_are_set(logger, health_flag, no_exit,
                                                          no_telegram):
    watchdog = GristWatchdog(logger, telegram_bot_token="123:abc", telegram_chat_id="-100")
    _run_to_death(watchdog)
    assert len(no_telegram) == 1
    url, payload, timeout = no_telegram[0]
    assert url == "https://api.telegram.org/bot123:abc/sendMessage"
    assert payload["chat_id"] == "-100"
    assert "Watchdog timeout reached" in payload["text"]
    # Without a timeout the notification could hang the dying process forever,
    # and the container would never actually restart.
    assert timeout == src.watchdog.TELEGRAM_TIMEOUT


@pytest.mark.parametrize("token,chat_id", [
    (None, None),
    ("123:abc", None),
    (None, "-100"),
    ("", ""),
])
def test_nothing_is_sent_unless_both_variables_are_set(logger, health_flag, no_exit,
                                                       no_telegram, token, chat_id):
    # A half-configured pair is a supported deployment, not an error: the
    # container restart and the log line are the primary signal either way.
    watchdog = GristWatchdog(logger, telegram_bot_token=token, telegram_chat_id=chat_id)
    _run_to_death(watchdog)
    assert no_telegram == []
    assert any("Telegram notification skipped" in message
               for message in logger.warning_messages)
    # The process still dies. A missing notification must never be the reason a
    # wedged loop is left running.
    assert no_exit.calls == [1]


def test_a_failing_notification_does_not_stop_the_exit(logger, health_flag, no_exit,
                                                       monkeypatch):
    """A dead telegram must not keep a wedged process alive — and must not leak the token.

    The exception text is the real shape `requests` produces: the bot token is a
    PATH SEGMENT of the URL, and urllib3 quotes the whole url back. This is also
    the single most-read line in the log, because it is written at the moment the
    watchdog fires and takes the container down with it.
    """
    def exploding_post(url, json=None, timeout=None):
        raise src.watchdog.requests.exceptions.ConnectionError(
            "HTTPSConnectionPool(host='api.telegram.org', port=443): Max retries "
            "exceeded with url: {} (Caused by NewConnectionError('refused'))".format(url))

    # A REALISTIC token rather than the short "123:abc" the other tests use, and
    # the length is the point: `redact()` leaves values below MIN_SECRET_LENGTH
    # alone (a four-character chat id would otherwise pepper every message with
    # placeholders), so a token shorter than that guard would make this test pass
    # while proving nothing about the real one — which is over forty characters.
    token = "123456789:AA-this-is-not-a-real-bot-token"
    monkeypatch.setattr(src.watchdog.requests, "post", exploding_post)
    watchdog = GristWatchdog(logger, telegram_bot_token=token, telegram_chat_id="-100")
    _run_to_death(watchdog)
    assert no_exit.calls == [1]
    assert any("Failed to send Telegram notification" in message
               for message in logger.error_messages)
    # Not one message anywhere may carry the token.
    every_message = (logger.info_messages + logger.warning_messages + logger.error_messages)
    assert not any(token in message for message in every_message)
    # The failure still has to be diagnosable: the class of the error survives.
    assert any("ConnectionError" in message for message in logger.error_messages)


def test_os_exit_is_the_real_thing_when_it_is_not_patched():
    # Guards the patch itself: if `os._exit` were renamed or the fixture stopped
    # applying, every test above would still pass while nothing killed anything.
    assert src.watchdog.os._exit is os._exit
