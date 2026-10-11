"""The Grist helpers in src/checker.py, against a fake client instead of a Grist server.

`find_chain` gets the most attention because every one of its five exits is a
different way the Chains table can be wrong, and the loop turns each of them into
a wallet marked "Error: ..." rather than a crash.
"""

import pytest

from src.checker import find_chain, setting


class FakeClient:
    """Answers `fetch_table` from canned rows, keyed by table name."""

    def __init__(self):
        self.tables = {}

    def fetch_table(self, table):
        return self.tables.get(table, [])


class Row:
    def __init__(self, **fields):
        self.__dict__.update(fields)


@pytest.fixture
def grist():
    return FakeClient()


# --- reads -------------------------------------------------------------------

def test_setting_reads_the_named_column_of_the_first_settings_row(grist):
    # This document's Settings table is ONE row whose columns are the settings,
    # not a name/value list like the sibling projects'.
    grist.tables["Settings"] = [Row(Chain=2, Token="eth", Divider="18")]
    assert setting(grist, "Chain") == 2
    assert setting(grist, "Token") == "eth"
    assert setting(grist, "Divider") == "18"


# --- find_chain: every way the Chains table can be wrong ---------------------

@pytest.mark.parametrize("target_id", [None, "", 0, "0"])
def test_find_chain_rejects_an_empty_or_zero_chain(grist, target_id):
    # An empty `Chain` setting must not silently become chain 0 in every request.
    with pytest.raises(Exception) as excinfo:
        find_chain(grist, target_id, "Chains")
    assert "Chain is None!" in str(excinfo.value)


def test_find_chain_rejects_an_empty_chains_table(grist):
    grist.tables["Chains"] = []
    with pytest.raises(Exception) as excinfo:
        find_chain(grist, 1, "Chains")
    assert "Chains table is empty!" in str(excinfo.value)


def test_find_chain_rejects_a_chain_that_is_not_in_the_table(grist):
    grist.tables["Chains"] = [Row(id=1, Chain_id=1)]
    with pytest.raises(Exception) as excinfo:
        find_chain(grist, 99, "Chains")
    assert "Chain not found!" in str(excinfo.value)


@pytest.mark.parametrize("empty", [None, ""])
def test_find_chain_rejects_a_row_whose_chain_id_is_empty(grist, empty):
    # The row exists but its Chain_id cell was never filled: passing that on
    # would build an Etherscan URL with `chainid=None`.
    grist.tables["Chains"] = [Row(id=1, Chain_id=empty)]
    with pytest.raises(Exception) as excinfo:
        find_chain(grist, 1, "Chains")
    assert "ID is None!" in str(excinfo.value)


def test_find_chain_returns_the_chain_id_of_the_matching_row(grist):
    grist.tables["Chains"] = [Row(id=1, Chain_id=1), Row(id=2, Chain_id=8453)]
    assert find_chain(grist, 2, "Chains") == 8453
