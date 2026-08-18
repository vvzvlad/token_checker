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
import requests
from conftest import long_enough

import src.watchdog
from src.health import HealthCheckHandler
from src.watchdog import UNHEALTHY_BELOW, WATCHDOG_STEP, WATCHDOG_TIMEOUT, GristWatchdog

# The bot token every test here that cares about REDACTION uses. Padded from
# `MIN_SECRET_LENGTH` via the conftest helper rather than written at a length that
# clears today's floor: `redact()` leaves anything below the floor alone, so a
# token underneath it would make those tests pass by being SKIPPED rather than by
# being hidden — silently, and only after somebody raised the constant.
REDACTABLE_TOKEN = long_enough("123456789:AA-this-is-not-a-real-bot-token")


def telegram_answer(status_code, url, reason="OK", body=b'{"ok": true}'):
    """A REAL `requests.Response`, not a hand-rolled double.

    The whole point of the checks in `send_telegram_notification` is that a
    refused notification is an ordinary response rather than an exception, and
    that `raise_for_status()` builds its message out of the URL — which carries
    the bot token as a path segment. A double would let this file decide both of
    those; the real class is what pins them to what `requests` actually does, and
    it is also what makes `.json()` behave the way it does in production, body and
    all.
    """
    response = requests.Response()
    response.status_code = status_code
    response.url = url
    response.reason = reason
    response._content = body
    return response


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
        # A 200 AND `{"ok": true}`, because that is what "the notification went
        # through" looks like and the code now reads both. Returning None here
        # would make every test in this file exercise the FAILURE path instead,
        # quietly — and so would a 200 with an empty body.
        return telegram_answer(200, url)

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
    # Telegram accepted it, so — and only so — the log may say it was sent. The
    # companion test below is the half that gives this line its meaning.
    assert any("Telegram notification sent" in message for message in logger.info_messages)


def test_a_notification_telegram_refuses_is_not_logged_as_sent(logger, health_flag, no_exit,
                                                               monkeypatch):
    """The claim that used to be stronger than the check.

    401 is exactly how a broken bot token arrives — rotated, revoked, or still
    carrying the whitespace it was configured with — and it does NOT raise:
    `requests` treats "the server answered, and the answer is no" as a completely
    successful call. The result was thrown away and the next line logged "Telegram
    notification sent about watchdog timeout" unconditionally, so the single log
    line written at the moment the watchdog kills the container asserted a
    delivery that never happened.
    """
    token = REDACTABLE_TOKEN
    posted = []

    def refusing_post(url, json=None, timeout=None):
        posted.append(url)
        return telegram_answer(401, url, reason="Unauthorized")

    monkeypatch.setattr(src.watchdog.requests, "post", refusing_post)
    watchdog = GristWatchdog(logger, telegram_bot_token=token, telegram_chat_id="-100")
    _run_to_death(watchdog)

    assert len(posted) == 1
    # The claim is gone...
    assert not any("Telegram notification sent" in message
                   for message in logger.info_messages)
    # ...and the refusal is in the log in its place, with the status in it: "it
    # failed" without the status leaves the operator no way to tell a dead network
    # from a key that needs replacing.
    assert any("Failed to send Telegram notification" in message and "401" in message
               for message in logger.error_messages)
    # `raise_for_status()` quotes the url back and the url carries the token as a
    # path segment, so this path leaks exactly the way a transport error does.
    every_message = (logger.info_messages + logger.warning_messages + logger.error_messages)
    assert not any(token in message for message in every_message)
    # And the process still dies. This runs on the way to os._exit(), so a
    # notification that fails — in any way, this new one included — may never be
    # the reason a wedged loop is left running.
    assert no_exit.calls == [1]


@pytest.mark.parametrize("body,excerpt", [
    # The Bot API's own refusal envelope: `description` is the wording the operator
    # actually needs, and it has to survive into the line.
    (b'{"ok": false, "description": "chat not found"}', "chat not found"),
    # An envelope with no `ok` at all. Nothing here is a refusal in the API's
    # vocabulary, which is exactly why the body has to be quoted rather than
    # mined for one field.
    (b'{"result": "queued"}', "'result': 'queued'"),
    # A LIST, and `null`. Neither is a shape the Bot API can produce, and that is
    # the point: the answer that gets this far is by definition one it did not
    # write. JSON's top level is not obliged to be an object, so `payload.get`
    # exists only because something checked the type first.
    (b'[1, 2, 3]', "[1, 2, 3]"),
    (b'null', "None"),
])
def test_a_200_that_is_not_an_ok_envelope_is_not_logged_as_sent(logger, health_flag, no_exit,
                                                                monkeypatch, body, excerpt):
    """The gap the status check alone leaves, and why the body is read too.

    The Bot API mirrors its `error_code` into the HTTP status, so status and body
    normally agree and this is a narrow case — but it is not an empty one: an
    intercepting proxy, a gateway with its own 200 error page, or any future
    partial-success shape answers 200 while the envelope says no. `ok` is the
    field the API documents as the verdict; checking only the status meant such an
    answer was written into the log as a delivery, in the single line the operator
    reads to find out whether anybody was told the container died.

    Parametrised over the shapes that reach this branch, because they differ in
    what the line can be built OUT OF. Only the first carries a `description`;
    reporting that field alone rendered the other three as "Telegram refused the
    notification: None", which tells the reader neither the status nor what
    arrived — in the case the body is read for. The last two are not dicts at all,
    and they are what makes the type check load-bearing: without it `payload.get`
    raises, the same handler still catches it and the process still dies, so the
    only casualty is the line itself, which degrades from a quoted body to
    `AttributeError: 'list' object has no attribute 'get'`.
    """
    def refusing_post(url, json=None, timeout=None):
        return telegram_answer(200, url, body=body)

    monkeypatch.setattr(src.watchdog.requests, "post", refusing_post)
    watchdog = GristWatchdog(logger, telegram_bot_token=REDACTABLE_TOKEN,
                             telegram_chat_id="-100")
    _run_to_death(watchdog)

    assert not any("Telegram notification sent" in message
                   for message in logger.info_messages)
    failure = [message for message in logger.error_messages
               if "Failed to send Telegram notification" in message]
    assert len(failure) == 1
    # Deliberately the CLASS as well as the text: `RuntimeError` is this module's
    # own verdict on the answer, and it is what separates "the body was read and
    # rejected" from "the body was read and blew up on the way", which the handler
    # would otherwise report in the same shape.
    assert "RuntimeError" in failure[0]
    # The status, which `raise_for_status()` did not object to — "refused" under a
    # 200 is the part that has to be said out loud.
    assert "200" in failure[0]
    # And the body itself, so the operator sees what actually answered instead of
    # the word `None`.
    assert excerpt in failure[0]
    assert no_exit.calls == [1]


def test_a_body_too_long_for_the_log_is_cut_down(logger, health_flag, no_exit, monkeypatch):
    # A gateway's error page is as long as the gateway feels like, and this line is
    # written while the container is being taken down — one answer may not push the
    # rest of the log off the reader's screen. Bounded, but not to nothing: what is
    # kept has to be enough to recognise what answered.
    filler = "A" * 5000

    def flooding_post(url, json=None, timeout=None):
        return telegram_answer(
            200, url, body='{{"nonsense": "{}"}}'.format(filler).encode())

    monkeypatch.setattr(src.watchdog.requests, "post", flooding_post)
    watchdog = GristWatchdog(logger, telegram_bot_token=REDACTABLE_TOKEN,
                             telegram_chat_id="-100")
    _run_to_death(watchdog)

    failure = [message for message in logger.error_messages
               if "Failed to send Telegram notification" in message]
    assert len(failure) == 1
    assert filler not in failure[0]
    assert len(failure[0]) < 2 * src.watchdog.BODY_EXCERPT_LIMIT
    assert "nonsense" in failure[0]
    assert no_exit.calls == [1]


def test_a_200_with_a_body_that_is_not_json_does_not_stop_the_exit(logger, health_flag,
                                                                   no_exit, monkeypatch):
    # Reading the body is a NEW way for this method to raise, and this method runs
    # on the way to os._exit(1) — a notification that fails, in any way, may never
    # be the reason a wedged loop is left running. A gateway answering 200 with an
    # HTML error page is the shape that gets there: the status passes, `.json()`
    # does not.
    def html_post(url, json=None, timeout=None):
        return telegram_answer(200, url, body=b"<html>502 Bad Gateway</html>")

    monkeypatch.setattr(src.watchdog.requests, "post", html_post)
    watchdog = GristWatchdog(logger, telegram_bot_token=REDACTABLE_TOKEN,
                             telegram_chat_id="-100")
    _run_to_death(watchdog)

    assert not any("Telegram notification sent" in message
                   for message in logger.info_messages)
    assert any("Failed to send Telegram notification" in message
               for message in logger.error_messages)
    assert no_exit.calls == [1]


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
    # alone (it will not blind-replace anything short enough to turn up in the
    # text by coincidence), so a token shorter than that guard would make this
    # test pass by being SKIPPED rather than by being hidden. `OptionalSecret` in
    # src/settings.py refuses such a token at startup, which is what lets this
    # test be about the redaction rather than about the length.
    token = REDACTABLE_TOKEN
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


def test_the_chat_id_is_not_redacted_out_of_the_failure_message(logger, health_flag, no_exit,
                                                                monkeypatch):
    """The addressee is not a secret, and redacting it could only damage the line.

    The chat id used to be passed to `redact()` here alongside the bot token. It
    is not a credential — it never appears in these texts as one — so the
    replacement buys nothing, while a chat id long enough to be replaced at all
    matches any run of characters that happens to equal it. This one is a
    substring of the request id in the very message being written, which is the
    whole failure mode `MIN_SECRET_LENGTH` exists to describe: below the guard the
    damage is silently declined, above it, it is silently done.

    So the fix is not a bigger guard, it is not handing a non-credential to a
    blind replacer at all — and the guard goes back to being a rule about
    credentials rather than an accident about this variable's length.
    """
    # Long enough for `redact()` to actually replace it, which is the premise of
    # the whole test: below the floor the damage is declined and this would pass
    # for the wrong reason. Padded with digits so it still reads as a chat id.
    chat_id = long_enough("12345678", pad="0")
    token = REDACTABLE_TOKEN

    def exploding_post(url, json=None, timeout=None):
        raise src.watchdog.requests.exceptions.ConnectionError(
            "HTTPSConnectionPool(host='api.telegram.org', port=443): request "
            "{}90 failed, max retries exceeded with url: {}".format(chat_id, url))

    monkeypatch.setattr(src.watchdog.requests, "post", exploding_post)
    watchdog = GristWatchdog(logger, telegram_bot_token=token, telegram_chat_id=chat_id)
    _run_to_death(watchdog)

    failure = [message for message in logger.error_messages
               if "Failed to send Telegram notification" in message]
    assert len(failure) == 1
    # The request id survives intact. Redacting the chat id would have cut it down
    # to "***90" and left the operator with a broken identifier.
    assert "{}90".format(chat_id) in failure[0]
    # The token, which IS a credential, is still gone.
    assert token not in failure[0]
    assert no_exit.calls == [1]


def test_os_exit_is_the_real_thing_when_it_is_not_patched():
    # Guards the patch itself: if `os._exit` were renamed or the fixture stopped
    # applying, every test above would still pass while nothing killed anything.
    assert src.watchdog.os._exit is os._exit
