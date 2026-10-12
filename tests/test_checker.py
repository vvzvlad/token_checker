"""The main loop: what it reads, what it writes, and the health flag it hangs on.

Three things are pinned here:

* the four `set_health()` calls: auto-heal and the auto-update rollback act on
  the flag, so a missing one restarts a working service and a spurious one hides
  a broken one;
* the names `Wallets` / `Settings` / `Chains` and `Chain` / `Token` / `Divider`,
  which belong to somebody else's document;
* that a failure reading Grist never writes to the previous iteration's wallet.

The loop is driven with every collaborator replaced (see the `loop` fixture) and
stopped by a `StopLoop` raised from one of them: `run()` has no exit of its own.
"""

import pytest

import src.balances
import src.checker
from src.checker import CHAINS_TABLE, DEFAULT_DIVIDER, NODES_TABLE, SETTINGS_TABLE, parse_divider
from src.health import HealthCheckHandler


class StopLoop(BaseException):
    """Ends `run()`'s `while True` from inside a fake.

    A BaseException on purpose. `run()` catches `Exception` twice over — that is
    the whole point of the loop — so anything else would be swallowed by the very
    thing it is meant to stop, and the test would hang instead of failing.
    """


class Wallet:
    def __init__(self, id, Address=None, Value=None):
        self.id = id
        self.Address = Address
        self.Value = Value


class Loop:
    """A recorder plus two scripts, one per collaborator that answers the loop.

    Both scripts are lists consumed one entry per iteration, and an entry may be:
      * a value  -> returned,
      * an Exception instance -> raised,
      * a callable -> called with the real arguments (used to run the REAL
        `check_balance` against a patched `requests`).
    Running past the end of a script raises StopLoop, so a test declares how many
    iterations it wants simply by how much it scripts.
    """

    def __init__(self, logger):
        self.logger = logger
        self.events = []            # ordered names of everything that happened
        self.updates = []           # (row_id, payload, health flag at the time)
        self.update_tables = []     # table of every grist.update call
        self.settings_asked = []
        self.chain_lookups = []
        self.wallet_tables = []
        self.sleeps = []            # (seconds, health flag at the time)
        self.balance_calls = []     # (address, chain_id, token, divider, health flag)
        self.grist_init = None
        self.watchdog_init = None
        self.health_port = None
        self.wallets = []
        self.balances = []
        # Exceptions for `grist.update` to raise, one per call, consumed like the
        # other two scripts. Empty means every write succeeds.
        self.update_errors = []
        self.settings_values = {"Chain": 7, "Token": "eth", "Divider": 18}

    def next(self, script, what):
        if not script:
            raise StopLoop(what)
        item = script.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    def messages(self):
        """Every line the loop logged, whatever the level."""
        return (self.logger.info_messages + self.logger.warning_messages
                + self.logger.error_messages)


@pytest.fixture
def loop(monkeypatch, logger):
    """Install fakes for everything `run()` talks to, and hand back the recorder.

    `build_logger` is replaced too, for two reasons: the real one adds a handler to
    a module-level logger on every call, so repeated `run()`s would multiply the
    output, and the recording logger is what lets a test assert on what was said.
    """
    state = Loop(logger)

    class FakeWatchdog:
        def __init__(self, logger, telegram_bot_token=None, telegram_chat_id=None,
                     telegram_bot_api_server=None):
            state.events.append("watchdog_init")
            state.watchdog_init = (telegram_bot_token, telegram_chat_id)

        def start(self):
            state.events.append("watchdog_start")

        def reset_timeout(self):
            state.events.append("reset_timeout")

    class FakeGrist:
        def __init__(self, server, doc_id, api_key):
            state.events.append("grist_init")
            state.grist_init = (server, doc_id, api_key)

        def update(self, table, row_id, updates):
            state.update_tables.append(table)
            if state.update_errors:
                # Nothing is recorded in `state.updates`: a write that raised is a
                # write that did not land, and recording it would let a test
                # assert on a cell Grist never received.
                state.events.append("update_failed")
                raise state.update_errors.pop(0)
            state.events.append("update")
            state.updates.append((row_id, dict(updates), HealthCheckHandler.is_healthy))

    def fake_start_health_server(port, logger):
        state.events.append("health_server")
        state.health_port = port

    def fake_setting(grist, name):
        state.settings_asked.append(name)
        return state.settings_values[name]

    def fake_find_chain(grist, target_id, table):
        state.chain_lookups.append((target_id, table))
        return 1

    def fake_find_none_value(grist, table):
        state.events.append("find_none_value")
        state.wallet_tables.append(table)
        item = state.next(state.wallets, "wallet script exhausted")
        return item(grist) if callable(item) else item

    def fake_check_balance(address, chain_id, api_key, token, logger, divider):
        state.events.append("check_balance")
        state.balance_calls.append(
            (address, chain_id, token, divider, HealthCheckHandler.is_healthy))
        item = state.next(state.balances, "balance script exhausted")
        if callable(item):
            return item(address, chain_id, api_key, token, logger, divider)
        return item

    def fake_sleep(seconds):
        state.events.append("sleep")
        state.sleeps.append((seconds, HealthCheckHandler.is_healthy))

    monkeypatch.setattr(src.checker, "GristWatchdog", FakeWatchdog)
    monkeypatch.setattr(src.checker, "GristClient", FakeGrist)
    monkeypatch.setattr(src.checker, "setting", fake_setting)
    monkeypatch.setattr(src.checker, "find_chain", fake_find_chain)
    monkeypatch.setattr(src.checker, "start_health_server", fake_start_health_server)
    monkeypatch.setattr(src.checker, "find_none_value", fake_find_none_value)
    monkeypatch.setattr(src.checker, "check_balance", fake_check_balance)
    monkeypatch.setattr(src.checker.time, "sleep", fake_sleep)
    monkeypatch.setattr(src.checker, "build_logger", lambda: logger)

    def go():
        with pytest.raises(StopLoop):
            src.checker.run()

    state.go = go
    return state


# --- parse_divider -----------------------------------------------------------

@pytest.mark.parametrize("cell,expected", [
    # The cell is hand-authored, so it reads like a label as often as like a
    # number. The number is looked for INSIDE the string for that reason.
    ("18 (ETH)", 18),
    ("18", 18),
    ("6 (USDC)", 6),
    ("6", 6),
])
def test_the_divider_is_read_out_of_a_hand_authored_cell(cell, expected, logger):
    assert parse_divider(cell, logger) == expected


def test_an_unreadable_divider_warns_and_falls_back_instead_of_stopping_the_loop(logger):
    # A wrong divider is a wrong number in one cell; an exception here is a
    # service that checks nothing at all. The warning is the part that has to
    # survive — it is the only trace that a cell needs fixing.
    assert parse_divider("eighteen", logger) == DEFAULT_DIVIDER
    assert any("eighteen" in message for message in logger.warning_messages)


@pytest.mark.parametrize("cell", [18, 6, 8])
def test_a_numeric_divider_passes_through_untouched(cell, logger):
    # Grist hands back a real int when the column is numeric, and the string
    # sniffing above must not touch it.
    assert parse_divider(cell, logger) == cell
    assert logger.warning_messages == []


# --- what the loop reads: the document's own names ---------------------------

def test_the_table_names_are_the_grist_documents_own(loop):
    # Not configuration and not ours: renaming one here renames nothing there.
    # An empty wallet script stops the loop at the first wallet lookup, so exactly
    # one iteration's worth of reads is recorded.
    loop.wallets = []
    loop.go()
    assert loop.wallet_tables == ["Wallets"]
    assert (NODES_TABLE, SETTINGS_TABLE) == ("Wallets", "Settings")
    assert loop.chain_lookups == [(loop.settings_values["Chain"], "Chains")]
    assert CHAINS_TABLE == "Chains"


def test_the_settings_row_is_read_by_the_documents_own_column_names(loop):
    loop.wallets = []
    loop.go()
    assert loop.settings_asked == ["Chain", "Token", "Divider"]


# --- the order things are brought up in --------------------------------------

def test_the_watchdog_is_armed_before_the_grist_client_is_built(loop):
    # Deliberate: a client construction that somehow blocks is covered by a
    # watchdog that already exists.
    loop.wallets = []
    loop.go()
    assert loop.events.index("watchdog_init") < loop.events.index("grist_init")
    assert loop.events.index("watchdog_start") < loop.events.index("grist_init")


def test_the_health_server_comes_up_after_the_grist_client(loop):
    loop.wallets = []
    loop.go()
    assert loop.events.index("grist_init") < loop.events.index("health_server")
    assert loop.events.index("health_server") < loop.events.index("find_none_value")


# --- the health flag: all four transitions -----------------------------------

def test_the_service_is_unhealthy_only_while_the_balance_call_is_in_flight(loop, health_flag):
    # False before the outbound call — a lookup that never returns is exactly what
    # /health exists to report — and True again before the result is written.
    loop.wallets = [Wallet(1, Address="0xabc", Value="")]
    loop.balances = [("1.5", "")]
    loop.go()
    *_, health_during_call = loop.balance_calls[0]
    assert health_during_call is False
    row_id, payload, health_at_write = loop.updates[0]
    assert (row_id, payload) == (1, {"Value": "1.5", "Comment": ""})
    assert health_at_write is True
    assert loop.update_tables == ["Wallets"]


def test_a_failed_balance_call_puts_the_health_flag_back(loop, health_flag):
    # The inner handler's own set_health(True). Without it the flag stays down
    # from the call that failed, /health answers 503 and auto-heal restarts a
    # container whose only problem was one bad Etherscan reply.
    loop.wallets = [Wallet(1, Address="0xabc", Value="")]
    loop.balances = [RuntimeError("etherscan said no")]
    loop.go()
    row_id, payload, health_at_write = loop.updates[0]
    assert row_id == 1
    assert payload["Value"] == "--"
    assert health_at_write is True
    assert loop.update_tables == ["Wallets"]


def test_the_reason_is_logged_even_when_grist_cannot_be_written_to(loop, health_flag):
    """The inner handler logs BEFORE it writes, and the order is the finding.

    If Grist is down, `grist.update` raises to the outer handler; logging first
    keeps the balance-check reason.
    """
    loop.wallets = [Wallet(1, Address="0xabc", Value="")]
    loop.balances = [RuntimeError("etherscan said no")]
    loop.update_errors = [RuntimeError("grist is unreachable")]
    loop.go()

    # The write was attempted and failed, so nothing landed in Grist...
    assert loop.updates == []
    assert "update_failed" in loop.events
    # ...and the reason for the ORIGINAL failure survives anyway, in the log.
    assert any("Error occurred: etherscan said no" in message for message in loop.messages())
    # The outer handler still reports the write failure on its own account: the
    # two are different facts and a run has to show both.
    assert any("grist is unreachable" in message for message in loop.messages())


def test_a_failure_outside_the_wallet_step_puts_the_health_flag_back(loop, health_flag):
    """The outer handler's own set_health(True), and why it is not redundant.

    The watchdog thread can lower the flag mid-iteration (below 60 s remaining).
    If that iteration fails on a Grist call, this handler puts it back; otherwise
    /health would answer 503 through the ten-second sleep.

    The fake below stands in for the watchdog thread; the real one would need a
    five-minute countdown to reach the same state. Asserted at the sleep, which is
    still inside the failing iteration.
    """
    def the_watchdog_lowers_the_flag_then_grist_fails(grist):
        HealthCheckHandler.set_health(False)
        raise RuntimeError("grist is unreachable")

    loop.wallets = [the_watchdog_lowers_the_flag_then_grist_fails]
    loop.go()
    assert loop.sleeps[0] == (10, True)
    assert any("Error occurred, sleep 10s" in message for message in loop.messages())


def test_an_idle_iteration_puts_the_health_flag_back(loop, health_flag):
    """The idle branch must raise a flag the watchdog lowered.

    "All wallets have values" is where a settled deployment spends its time, and
    it passes through neither the balance call nor either handler.
    """
    HealthCheckHandler.set_health(False)
    loop.wallets = [None]
    loop.go()
    assert loop.sleeps[0] == (10, True)
    assert any("All wallets have values" in message for message in loop.messages())


# --- the defect: a stale wallet reference must never be written to -----------

def test_a_failed_grist_read_never_writes_to_the_previous_wallet(loop, health_flag):
    """Two iterations. The second one cannot read Grist. The first one's wallet is
    NOT allowed to be touched.

    `none_value_wallet` outlives the iteration. The read lives in the outer
    `try`, whose handler only logs and sleeps; under the inner handler a read
    failure would overwrite the previous wallet's correct value with "--".
    """
    loop.wallets = [
        Wallet(1, Address="0xabc", Value=""),      # iteration 1: processed fine
        RuntimeError("grist read failed"),         # iteration 2: cannot even choose
    ]
    loop.balances = [("1.5", "")]
    loop.go()

    # Exactly one write, and it is the correct value from the first iteration.
    assert loop.updates == [(1, {"Value": "1.5", "Comment": ""}, True)]
    assert not any(payload.get("Value") == "--" for _, payload, _ in loop.updates)
    # ...and the second iteration did what a failure to read Grist should do:
    # said so and slept, rather than writing anything anywhere.
    assert loop.sleeps == [(10, True)]


# --- the api key on the error path -------------------------------------------

def test_neither_handler_writes_a_credential_into_grist_or_the_log(loop, health_flag):
    """The loop redacts on its own account, not because `balances` happened to.

    Both of `run()`'s handlers put the text of an exception somewhere it outlives
    the process — the inner one into a Grist cell AND the log, the outer one into
    the log — and neither of them knows where the exception came from. So the
    exceptions here are synthetic and carry the key openly: `src/balances.py`
    cleaning up after itself must not be the only reason this comes out clean.
    """
    key = src.checker.settings.etherscan_api_key
    leaky = "connection failed: https://api.etherscan.io/v2/api?apikey={}".format(key)

    loop.wallets = [
        Wallet(1, Address="0xabc", Value=""),   # iteration 1: the inner handler
        RuntimeError(leaky),                    # iteration 2: the outer one
    ]
    loop.balances = [RuntimeError(leaky)]
    loop.go()

    _, payload, _ = loop.updates[0]
    assert payload["Value"] == "--"
    assert key not in payload["Comment"]
    assert "connection failed" in payload["Comment"]
    assert not any(key in message for message in loop.messages())


def test_the_etherscan_key_never_reaches_the_grist_comment(loop, health_flag, monkeypatch):
    """The end of the leak path, through the REAL `check_balance`.

    `requests` quotes the URL, with ETHERSCAN_API_KEY, in transport errors. Both
    destinations (the `Comment` cell and the log) are checked against the
    configured key.
    """
    key = src.checker.settings.etherscan_api_key
    assert key, "the suite's own conftest is supposed to configure one"

    def exploding_get(url, timeout=None):
        raise src.balances.requests.exceptions.ConnectionError(
            "HTTPSConnectionPool(host='api.etherscan.io', port=443): Max retries "
            "exceeded with url: {}".format(url))

    monkeypatch.setattr(src.balances.requests, "get", exploding_get)

    loop.wallets = [Wallet(1, Address="0xabc", Value="")]
    # The real thing, called with the real arguments: this test is worth nothing
    # against a mock that never builds the URL.
    loop.balances = [src.balances.check_balance]
    loop.go()

    row_id, payload, _ = loop.updates[0]
    assert row_id == 1
    assert payload["Value"] == "--"
    assert key not in payload["Comment"]
    assert not any(key in message for message in loop.messages())
    # Still diagnosable: the wallet and the class of failure survive redaction.
    assert "0xabc" in payload["Comment"]
    assert "Max retries exceeded" in payload["Comment"]
