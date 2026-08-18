"""Reading the Etherscan answer, and picking the wallet to ask about.

Nothing here opens a socket: `requests.get` is replaced inside `src.balances`.
What is pinned is the part that decides what lands in the Grist `Value` cell —
which URL is called, which responses count as success, and how the raw integer is
turned into the string that is written.
"""

import traceback

import pytest
import requests
from conftest import long_enough

import src.balances
from src.balances import check_balance, find_none_value

# The stand-in for ETHERSCAN_API_KEY in the redaction tests below. Padded from
# `MIN_SECRET_LENGTH` via the conftest helper, because `redact()` leaves anything
# under that floor alone: a shorter stand-in would let those tests pass by having
# nothing replaced, which is the opposite of what they check. Deriving it is what
# keeps that true after somebody raises the constant.
API_KEY = long_enough("super-secret-key")


class FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def json(self):
        # An exception AS the payload is how a test spells a body that is not JSON
        # at all — a proxy's own page, an error page from whatever sits in front of
        # the API. That arrives in the code as `response.json()` raising, not as a
        # value, so it has to arrive here the same way.
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


class RecordingGet:
    """Stands in for requests.get and remembers the URL it was called with."""

    def __init__(self, payload):
        self.payload = payload
        self.urls = []
        self.timeouts = []

    def __call__(self, url, timeout=None):
        self.urls.append(url)
        self.timeouts.append(timeout)
        return FakeResponse(self.payload)


@pytest.fixture
def etherscan(monkeypatch):
    """Install a fake requests.get; the test fills in the payload it answers."""

    def install(payload):
        fake = RecordingGet(payload)
        monkeypatch.setattr(src.balances.requests, "get", fake)
        return fake

    return install


class Wallet:
    def __init__(self, id, Address=None, Value=None):
        self.id = id
        self.Address = Address
        self.Value = Value


class FakeGrist:
    def __init__(self, rows):
        self.rows = rows

    def fetch_table(self, table=None):
        return self.rows


# --- the ETH branch ----------------------------------------------------------

def test_eth_balance_uses_the_balance_action_and_formats_at_18(etherscan, logger):
    fake = etherscan({"status": "1", "result": "1500000000000000000"})
    value, msg = check_balance("0xabc", 1, "secret-key", "eth", logger, 18)
    assert (value, msg) == ("1.5", "")
    # `action=balance`, not `tokenbalance`: asking Etherscan for a token balance
    # of the native coin answers zero for a wallet that is not empty at all.
    assert "action=balance" in fake.urls[0]
    assert "action=tokenbalance" not in fake.urls[0]


def test_eth_branch_is_chosen_case_insensitively(etherscan, logger):
    # The token comes from a hand-edited Grist cell, so "ETH" and "eth" both
    # have to mean the native coin.
    fake = etherscan({"status": "1", "result": "1000000000000000000"})
    check_balance("0xabc", 1, "k", "ETH", logger, 18)
    assert "action=balance" in fake.urls[0]


def test_the_etherscan_call_carries_an_explicit_timeout(etherscan, logger):
    # Without it a stalled Etherscan connection hangs the single-threaded loop
    # until the watchdog kills the process.
    fake = etherscan({"status": "1", "result": "1000000000000000000"})
    check_balance("0xabc", 1, "k", "eth", logger, 18)
    assert fake.timeouts[0] == src.balances.ETHERSCAN_TIMEOUT


# --- the token branch --------------------------------------------------------

def test_token_balance_uses_the_tokenbalance_action_and_the_contract(etherscan, logger):
    fake = etherscan({"status": "1", "result": "2500000"})
    value, msg = check_balance("0xabc", 42161, "k", "0xdeadbeef", logger, 6)
    assert (value, msg) == ("2.5", "")
    assert "action=tokenbalance" in fake.urls[0]
    assert "contractaddress=0xdeadbeef" in fake.urls[0]


# --- formatting --------------------------------------------------------------

@pytest.mark.parametrize("divider,raw,expected", [
    # Trailing zeros are stripped, and so is a bare trailing dot: the value goes
    # into a TEXT cell, so "1.000000000000000000" and "1" are different strings
    # to everything reading that column.
    (18, "1000000000000000000", "1"),
    (18, "1500000000000000000", "1.5"),
    # 18 decimals printed with %.18f — the branch exists so a small balance does
    # not arrive in scientific notation, which nothing downstream parses back.
    (18, "1", "0.000000000000000001"),
    (18, "0", "0"),
    # PINNED AS IT IS, NOT AS IT SHOULD BE. The raw integer is divided in
    # floating point, and 1.2345 has no exact double, so printing 18 decimals
    # exposes the representation error and the cell reads
    # "1.234499999999999931". The value is right to ~16 significant digits and
    # every consumer of this column is a human reading it, which is why it has
    # never been worth an exact-decimal rewrite — but it IS what the column
    # holds, so a change here would be a change to production data.
    (18, "1234500000000000000", "1.234499999999999931"),
    (6, "1000000", "1"),
    (6, "1234500", "1.2345"),
    # Six decimals round the same error away, which is why the 6-divider branch
    # shows none of it.
    (6, "1", "0.000001"),
    (6, "0", "0"),
])
def test_token_values_are_formatted_by_divider(etherscan, logger, divider, raw, expected):
    etherscan({"status": "1", "result": raw})
    value, _ = check_balance("0xabc", 1, "k", "0xtoken", logger, divider)
    assert value == expected


@pytest.mark.parametrize("token", ["eth", "0xtoken"])
def test_both_branches_format_the_same_way(etherscan, logger, token):
    etherscan({"status": "1", "result": "1500000000000000000"})
    value, _ = check_balance("0xabc", 1, "k", token, logger, 18)
    assert value == "1.5"


# --- the two failure shapes --------------------------------------------------

@pytest.mark.parametrize("token", ["eth", "0xtoken"])
def test_no_transactions_found_is_a_zero_not_an_error(etherscan, logger, token):
    # An address Etherscan has never seen is a legitimate answer: the caller
    # writes 0 into the cell and the note into Comment, and the wallet is done.
    etherscan({"status": "0", "message": "No transactions found", "result": "0"})
    assert check_balance("0xabc", 1, "k", token, logger, 18) == (0, "No transactions found")


@pytest.mark.parametrize("token", ["eth", "0xtoken"])
def test_any_other_non_success_status_raises(etherscan, logger, token):
    etherscan({"status": "0", "message": "NOTOK", "result": "Invalid API Key"})
    with pytest.raises(Exception) as excinfo:
        check_balance("0xabc", 1, "k", token, logger, 18)
    # The message ends up in the wallet's Comment cell, so it has to name the
    # address it is about.
    assert "0xabc" in str(excinfo.value)
    # ...and what Etherscan actually said. "NOTOK" is the status; `result` is the
    # half that tells the operator whether to fix a key or wait out a rate limit.
    assert "Invalid API Key" in str(excinfo.value)


@pytest.mark.parametrize("token,ran,other", [
    ("eth", "ETH", "token"),
    ("ETH", "ETH", "token"),
    ("0xtoken", "token", "ETH"),
])
def test_the_message_names_the_branch_that_actually_ran(etherscan, logger, token, ran, other):
    """One branch, named once, in the cell and in the log.

    The wrapper used to say "token" on BOTH branches while the inner raise said
    "ETH", so an ETH failure reached the wallet's `Comment` cell as two sentences
    contradicting each other — "Error while checking token transactions for
    address 0x...: Exception: Error while checking ETH transactions for address
    0x...". Nothing downstream could settle it either: `from None` suppresses the
    context, so the chain that named the real branch is gone by the time anybody
    reads the cell.
    """
    etherscan({"status": "0", "message": "NOTOK", "result": "Invalid API Key"})
    with pytest.raises(Exception) as excinfo:
        check_balance("0xabc", 1, "k", token, logger, 18)

    message = str(excinfo.value)
    assert "checking {} transactions".format(ran) in message
    assert "checking {} transactions".format(other) not in message
    # The log is the other destination and gets the same single statement.
    assert any("checking {} transactions".format(ran) in logged
               for logged in logger.error_messages)
    assert not any("checking {} transactions".format(other) in logged
                   for logged in logger.error_messages)
    # And the class name in the middle carries meaning rather than saying
    # "Exception:" — this failure is an answer that said no, not a lost
    # connection and not a payload in an unexpected shape.
    assert "EtherscanError" in message


def test_the_branch_is_named_token_when_the_branch_was_never_chosen(logger):
    # `token.lower()` on a Grist `Token` cell that is not a string fails BEFORE
    # either branch is picked — and it is the first statement in `check_balance`
    # that can raise at all, which is what `subject`'s binding has to stay ahead
    # of. (Not "ahead of the `try`": moved to the first line inside it, the
    # binding would still be ahead of every raising statement and this test would
    # still be green. Above the block is simply the position a later edit cannot
    # push a raise in front of.)
    #
    # The handler still has to name something here, and it must not be a NameError
    # — that replaces the real failure with a second one about the code reporting
    # it, in the cell the operator is sent to read.
    with pytest.raises(Exception) as excinfo:
        check_balance("0xabc", 1, "k", None, logger, 18)
    assert "checking token transactions for address 0xabc" in str(excinfo.value)
    assert "AttributeError" in str(excinfo.value)


@pytest.mark.parametrize("payload,expected_class", [
    # A JSON list where an object was expected — `data['status']` on a list.
    ([], "TypeError"),
    # An answer that says success and then hands back something that is not a
    # number, so `int()` is what fails.
    ({"status": "1", "result": "not-a-number"}, "ValueError"),
    # A body that never was JSON: a proxy page, a CDN error page.
    (requests.exceptions.JSONDecodeError("Expecting value", "<html>", 0), "JSONDecodeError"),
])
def test_the_set_of_failures_reaching_the_comment_cell_is_open(etherscan, logger,
                                                               payload, expected_class):
    """Why `EtherscanError` is a type of its own, and why its docstring lists no total.

    The handler at the bottom of `check_balance` catches `Exception`, so what
    reaches it is open-ended: these three, plus the `KeyError` and `AttributeError`
    pinned by their own tests above, plus the transport family, plus whatever the
    next shape of a bad answer turns out to be. Anything that counts them is wrong
    the next time the API changes.

    All of it arrives at the operator as ONE line in a spreadsheet cell, which is
    why the class name is put in front of the text: it is the entire
    classification the reader gets. That is also the argument for
    `EtherscanError` — "the answer said no" needs a word of its own among these,
    and "Exception:" would be a word that distinguishes nothing.
    """
    etherscan(payload)
    with pytest.raises(Exception) as excinfo:
        check_balance("0xabc", 1, "k", "0xtoken", logger, 18)
    message = str(excinfo.value)
    assert expected_class in message
    # And the wallet is still named — the cell has to say what it is about.
    assert "0xabc" in message


def test_a_malformed_payload_raises_rather_than_writing_a_wrong_number(etherscan, logger):
    # `status` missing entirely — a gateway error page, a changed API. The
    # KeyError is wrapped into the same address-naming exception as everything
    # else, instead of escaping as a bare KeyError.
    etherscan({"unexpected": "shape"})
    with pytest.raises(Exception) as excinfo:
        check_balance("0xabc", 1, "k", "eth", logger, 18)
    assert "0xabc" in str(excinfo.value)
    # This is the path where the wrapped text is one quoted word: `str(KeyError)`
    # is `"'status'"` and nothing else. That word goes into the wallet's `Comment`
    # cell, which is where the operator is sent to find out what happened, so the
    # CLASS has to travel with it — exactly as the watchdog does it.
    assert "KeyError" in str(excinfo.value)
    assert any("KeyError" in message for message in logger.error_messages)


def test_the_api_key_never_reaches_stdout(etherscan, logger, capsys):
    # The URLs carry ETHERSCAN_API_KEY as a query parameter and used to be
    # print()ed on every call, which put the key in plain text into
    # `docker logs`. This is the regression guard for that.
    #
    # NOTE what this does NOT cover: the mock here never raises, so this is the
    # happy path only — and the happy path was never where the key escaped after
    # the print() was removed. The test below is the one about the real leak.
    etherscan({"status": "1", "result": "1000000000000000000"})
    check_balance("0xabc", 1, API_KEY, "eth", logger, 18)
    captured = capsys.readouterr()
    assert API_KEY not in captured.out
    assert API_KEY not in captured.err
    assert not any(API_KEY in message
                   for message in logger.info_messages + logger.error_messages)


@pytest.mark.parametrize("token", ["eth", "0xtoken"])
def test_a_network_error_does_not_carry_the_api_key_out_of_here(monkeypatch, logger, token):
    """The real leak: `requests` hands the whole URL back inside its exceptions.

    Not a contrived string — this is the shape of the message urllib3 produces for
    every proxy/connection failure, and both URLs this module builds carry
    ETHERSCAN_API_KEY as a query parameter. So a module that logs no URL at all
    still writes the key to `docker logs` the moment it logs `{e}` — and, through
    the caller, into the wallet's `Comment` cell in a Grist document that people
    open, share by link and back up.

    Both exits from this function are checked, because they go to different
    places: the log line ends up in `docker logs`, and the text of the raised
    exception is what `src/checker.py` writes into Grist.
    """
    def exploding_get(url, timeout=None):
        raise requests.exceptions.ConnectionError(
            "HTTPSConnectionPool(host='api.etherscan.io', port=443): Max retries "
            "exceeded with url: {} (Caused by ProxyError('Cannot connect'))".format(url))

    monkeypatch.setattr(src.balances.requests, "get", exploding_get)

    with pytest.raises(Exception) as excinfo:
        check_balance("0xabc", 1, API_KEY, token, logger, 18)

    assert API_KEY not in str(excinfo.value)
    assert not any(API_KEY in message
                   for message in logger.info_messages + logger.error_messages)
    # Redacted, not swallowed: the operator still has to be able to tell a proxy
    # failure from a wrong answer, and the wallet has to be named.
    assert "0xabc" in str(excinfo.value)
    assert "Max retries exceeded" in str(excinfo.value)


@pytest.mark.parametrize("token", ["eth", "0xtoken"])
def test_the_unredacted_original_is_not_left_chained_to_what_is_raised(monkeypatch, logger,
                                                                      token):
    """The other half of the same leak: the exception CHAIN, not the message.

    The `raise` that wraps the failure runs inside an `except`, so python
    implicitly attaches the original — the untouched `ConnectionError` whose text
    carries the api key in the URL — as `__context__`. `str(e)` never shows it,
    which is why a test written against `str(e)` is green by construction; the
    full chain is what `traceback.format_exc()`, `logger.error(...,
    exc_info=True)` and an unhandled exit to stderr all render, and `docker logs`
    is where those land.

    So this asserts against the rendered chain, and only against that.
    """
    # Bound to a name rather than written inline, and that is not style: a
    # traceback quotes the SOURCE LINE of every frame it walks, so a literal on
    # the call line below would appear in the render on its own account and fail
    # this test no matter what the code does. Production has no such literal —
    # the key arrives from `settings.etherscan_api_key`.
    key = API_KEY

    def exploding_get(url, timeout=None):
        raise requests.exceptions.ConnectionError(
            "HTTPSConnectionPool(host='api.etherscan.io', port=443): Max retries "
            "exceeded with url: {} (Caused by ProxyError('Cannot connect'))".format(url))

    monkeypatch.setattr(src.balances.requests, "get", exploding_get)

    with pytest.raises(Exception) as excinfo:
        check_balance("0xabc", 1, key, token, logger, 18)

    error = excinfo.value
    rendered = "".join(traceback.format_exception(type(error), error, error.__traceback__))
    assert key not in rendered
    # The redacted half still has to be there — a chain nobody renders because the
    # exception carries no message at all would pass the line above.
    assert "Max retries exceeded" in rendered
    assert "0xabc" in rendered
    # And the original IS still attached, carrying the key: what protects the key
    # is that the chain is suppressed, not that there is nothing to suppress. Kept
    # as an assertion so `from None` cannot be dropped as redundant.
    assert key in str(error.__context__)


# --- picking the wallet ------------------------------------------------------

def test_find_none_value_returns_the_first_wallet_that_is_due():
    # "First" is the table's own order: clearing a Value cell in Grist re-arms a
    # wallet, and they are picked up in the order the document shows.
    rows = [
        Wallet(1, Address="0xfilled", Value="1.5"),
        Wallet(2, Address="0xdue", Value=""),
        Wallet(3, Address="0xalso-due", Value=None),
    ]
    assert find_none_value(FakeGrist(rows)).id == 2


def test_find_none_value_treats_none_and_empty_string_alike():
    rows = [Wallet(1, Address="0xdue", Value=None)]
    assert find_none_value(FakeGrist(rows)).id == 1


@pytest.mark.parametrize("address", [None, ""])
def test_a_wallet_without_an_address_is_never_picked_up(address):
    # There is nothing to ask Etherscan about, and picking it would make the loop
    # spin on the same unusable row forever.
    rows = [Wallet(1, Address=address, Value="")]
    assert find_none_value(FakeGrist(rows)) is None


def test_no_due_wallet_returns_none():
    rows = [Wallet(1, Address="0xa", Value="1"), Wallet(2, Address="0xb", Value="2")]
    assert find_none_value(FakeGrist(rows)) is None


def test_an_empty_table_returns_none():
    assert find_none_value(FakeGrist([])) is None
