"""The main loop: what it reads, what it writes, and the health flag it hangs on.

`src/checker.py` is the one module the refactor put NEW code into, and it is the
one place where a mistake is not a wrong number in a cell but a wrong number
written OVER a right one. Three things are pinned here and none of them is
cosmetic:

* the four `set_health()` calls. The deployment acts on that flag twice over —
  auto-heal restarts a container docker calls unhealthy, auto-update rolls an
  image back that never reaches healthy — so a missing one is a restart loop on a
  service that works, and a spurious one hides a service that does not;
* the names `Wallets` / `Settings` / `Chains` and `Chain` / `Token` / `Divider`.
  They are somebody else's document's names. Renaming one here renames nothing
  there and simply stops the loop;
* that a failure reading Grist can never write to the wallet of a PREVIOUS
  iteration. That defect corrupted data rather than losing it, which is the kind
  nobody notices.

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
        self.settings_asked = []
        self.chain_lookups = []
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
        def __init__(self, logger, telegram_bot_token=None, telegram_chat_id=None):
            state.events.append("watchdog_init")
            state.watchdog_init = (telegram_bot_token, telegram_chat_id)

        def start(self):
            state.events.append("watchdog_start")

        def reset_timeout(self):
            state.events.append("reset_timeout")

    class FakeGrist:
        def __init__(self, server, doc_id, api_key, nodes_table, settings_table, logger):
            state.events.append("grist_init")
            state.grist_init = (server, doc_id, api_key, nodes_table, settings_table)

        def find_settings(self, setting):
            state.settings_asked.append(setting)
            return state.settings_values[setting]

        def find_chain(self, target_id, table):
            state.chain_lookups.append((target_id, table))
            return 1

        def update(self, row_id, updates, table=None):
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

    def fake_find_none_value(grist):
        state.events.append("find_none_value")
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
    monkeypatch.setattr(src.checker, "GRIST", FakeGrist)
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
    _, _, _, nodes_table, settings_table = loop.grist_init
    assert (nodes_table, settings_table) == ("Wallets", "Settings")
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


def test_the_reason_is_logged_even_when_grist_cannot_be_written_to(loop, health_flag):
    """The inner handler logs BEFORE it writes, and the order is the finding.

    `grist.update` is a network call, and errors arriving in a batch is exactly
    the situation where Grist itself is the thing that is down. With the write
    first, its failure leaves the inner handler for the outer one, the log line
    never runs, and the only surviving record of the iteration is "Grist is
    unreachable" — the reason the balance check failed reached neither the cell
    (the write failed) nor the log (it never happened), and is lost outright.
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

    Something CAN lower the flag in the middle of an iteration that never reaches
    the balance call: the watchdog thread, which sets it False on its own account
    below 60 s remaining. If that iteration then fails on a Grist call, this
    handler is what puts the flag back — without it the service answers 503
    through the ten-second sleep and recovers only at the top of the next
    iteration, and that window is exactly what auto-heal watches.

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
    """The branch that used to leave the flag down forever.

    "All wallets have values" is where a settled deployment spends all of its
    time, and it passes through neither the balance call nor either handler. So a
    flag the watchdog had lowered (it does that below 60 s remaining) stayed
    False for the rest of the process's life once the loop recovered into this
    branch: /health answers 503, docker marks the container unhealthy, auto-heal
    restarts it, and the fresh process settles into exactly the same state.
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

    `none_value_wallet` is a local of `run()` while the `while True` is inside
    `run()`, so the binding outlives the iteration. With the Grist read under the
    inner handler, a transient read failure — a 502 from the API, a dropped
    connection, anything — sent that handler to
    `grist.update(none_value_wallet.id, {"Value": "--", ...})` still holding the
    PREVIOUS wallet, and the write succeeded: a balance computed correctly seconds
    earlier was replaced by "--" and an error message about a failure that had
    nothing to do with that wallet. Silent, and indistinguishable afterwards from
    a wallet that genuinely failed.

    The fix is that the read lives in the outer `try`, whose handler only logs and
    sleeps. This test is the reason it may not move back.
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

    `requests` puts the full request URL into the text of any transport error, and
    that URL carries ETHERSCAN_API_KEY as a query parameter. The caller writes the
    text of that exception into the wallet's `Comment` cell — a Grist document
    that people open, share by link and put into backups — and into `docker logs`
    on the way. Both destinations are checked here, against the key the process is
    actually configured with.
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
