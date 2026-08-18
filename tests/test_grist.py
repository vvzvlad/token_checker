"""The Grist wrapper, against a recording double instead of a Grist server.

`GristDocAPI` is replaced wholesale, so nothing here opens a socket. What is
tested is the translation layer between this service and that client — the column
names it rewrites and the timestamps it converts, both silent failure modes: an
unrewritten "Chain id" is simply rejected by Grist, and a timestamp read in the
wrong zone looks like data.

`find_chain` gets the most attention because every one of its five exits is a
different way the Chains table can be wrong, and the loop turns each of them into
a wallet marked "Error: ..." rather than a crash.
"""

from datetime import datetime, timedelta, timezone

import pytest

import src.grist
from src.grist import GRIST


class FakeGristDocAPI:
    """Records what the real client would have been asked to do."""

    def __init__(self, doc_id, server=None, api_key=None):
        self.doc_id = doc_id
        self.server = server
        self.api_key = api_key
        self.updates = []
        self.tables = {}

    def update_records(self, table, records):
        self.updates.append((table, records))

    def fetch_table(self, table):
        return self.tables.get(table, [])


class Row:
    def __init__(self, **fields):
        self.__dict__.update(fields)


@pytest.fixture
def grist(monkeypatch, logger):
    monkeypatch.setattr(src.grist, "GristDocAPI", FakeGristDocAPI)
    return GRIST("http://grist.invalid", "doc-1", "key-1", "Wallets", "Settings", logger)


# --- names ------------------------------------------------------------------

def test_table_names_are_sanitised_at_construction(monkeypatch, logger):
    monkeypatch.setattr(src.grist, "GristDocAPI", FakeGristDocAPI)
    client = GRIST("s", "d", "k", "My Wallets", "Node Settings", logger)
    assert client.nodes_table == "My_Wallets"
    assert client.settings_table == "Node_Settings"


def test_update_rewrites_column_names_for_grist(grist):
    grist.update(7, {"Chain id": 1, "Value": "1.5"})
    table, records = grist.grist.updates[-1]
    assert table == "Wallets"
    assert records == [{"id": 7, "Chain_id": 1, "Value": "1.5"}]


def test_update_column_rewrites_a_single_column_name(grist):
    grist.update_column(1, "Chain id", 8453)
    assert grist.grist.updates[-1] == ("Wallets", [{"id": 1, "Chain_id": 8453}])


def test_update_targets_an_explicit_table_when_given_one(grist):
    grist.update(1, {"A": 1}, table="Other")
    assert grist.grist.updates[-1][0] == "Other"


def test_update_column_targets_an_explicit_table_when_given_one(grist):
    grist.update_column(1, "A", 1, table="Other")
    assert grist.grist.updates[-1][0] == "Other"


# --- timestamps --------------------------------------------------------------

def test_update_converts_datetimes_to_timestamps(grist):
    moment = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    grist.update(1, {"Checked": moment})
    _, records = grist.grist.updates[-1]
    assert records[0]["Checked"] == int(moment.timestamp())


def test_update_column_converts_datetimes_to_timestamps(grist):
    moment = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    grist.update_column(1, "Checked", moment)
    _, records = grist.grist.updates[-1]
    assert records[0]["Checked"] == int(moment.timestamp())


def test_naive_datetimes_are_read_as_utc_plus_three(grist):
    # The document stores Moscow time. Reading a naive datetime as the runner's
    # local zone instead would move every written timestamp by hours, and the
    # container's TZ is not the document's.
    naive = datetime(2026, 1, 2, 3, 4, 5)
    aware = naive.replace(tzinfo=timezone(timedelta(hours=3)))
    assert grist.to_timestamp(naive) == int(aware.timestamp())


def test_aware_datetimes_keep_their_own_offset(grist):
    aware = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    assert grist.to_timestamp(aware) == int(aware.timestamp())


# --- reads -------------------------------------------------------------------

def test_fetch_table_defaults_to_the_nodes_table(grist):
    grist.grist.tables["Wallets"] = [Row(id=1)]
    assert len(grist.fetch_table()) == 1


def test_find_settings_reads_the_named_column_of_the_first_settings_row(grist):
    # This document's Settings table is ONE row whose columns are the settings,
    # not a name/value list like the sibling projects'.
    grist.grist.tables["Settings"] = [Row(Chain=2, Token="eth", Divider="18")]
    assert grist.find_settings("Chain") == 2
    assert grist.find_settings("Token") == "eth"
    assert grist.find_settings("Divider") == "18"


# --- find_chain: every way the Chains table can be wrong ---------------------

@pytest.mark.parametrize("target_id", [None, "", 0, "0"])
def test_find_chain_rejects_an_empty_or_zero_chain(grist, target_id):
    # An empty `Chain` setting must not silently become chain 0 in every request.
    with pytest.raises(Exception) as excinfo:
        grist.find_chain(target_id, "Chains")
    assert "Chain is None!" in str(excinfo.value)


def test_find_chain_rejects_an_empty_chains_table(grist):
    grist.grist.tables["Chains"] = []
    with pytest.raises(Exception) as excinfo:
        grist.find_chain(1, "Chains")
    assert "Chains table is empty!" in str(excinfo.value)


def test_find_chain_rejects_a_chain_that_is_not_in_the_table(grist):
    grist.grist.tables["Chains"] = [Row(id=1, Chain_id=1)]
    with pytest.raises(Exception) as excinfo:
        grist.find_chain(99, "Chains")
    assert "Chain not found!" in str(excinfo.value)


@pytest.mark.parametrize("empty", [None, ""])
def test_find_chain_rejects_a_row_whose_chain_id_is_empty(grist, empty):
    # The row exists but its Chain_id cell was never filled: passing that on
    # would build an Etherscan URL with `chainid=None`.
    grist.grist.tables["Chains"] = [Row(id=1, Chain_id=empty)]
    with pytest.raises(Exception) as excinfo:
        grist.find_chain(1, "Chains")
    assert "ID is None!" in str(excinfo.value)


def test_find_chain_returns_the_chain_id_of_the_matching_row(grist):
    grist.grist.tables["Chains"] = [Row(id=1, Chain_id=1), Row(id=2, Chain_id=8453)]
    assert grist.find_chain(2, "Chains") == 8453
