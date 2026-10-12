"""Cutting credential values out of text that is about to leave the process.

Pins the three shapes that would break it silently: an absent secret, an empty
one, and one so short that replacing it shreds the message.
"""

import pytest
from conftest import long_enough

from src.redact import MIN_SECRET_LENGTH, PLACEHOLDER, redact

# Long enough to be replaced at all, derived from the constant (a shorter stand-in
# would pass because nothing was replaced). Tests about the guard itself stay literal.
SECRET = long_enough("SECRET-api-key")


def test_a_secret_is_replaced_wherever_it_appears():
    text = ("HTTPSConnectionPool(host='api.etherscan.io', port=443): Max retries exceeded "
            "with url: /v2/api?apikey={0}&chainid=1 (apikey={0})".format(SECRET))
    result = redact(text, SECRET)
    assert SECRET not in result
    assert result.count(PLACEHOLDER) == 2


def test_several_secrets_are_removed_in_one_pass():
    # Eight characters each, which is MIN_SECRET_LENGTH exactly: written as
    # literals rather than derived from the constant on purpose, so RAISING the
    # threshold — the change that quietly stops redacting something real — goes
    # red here instead of being absorbed by a test that follows the constant.
    result = redact("key=AAAAAAAA token=BBBBBBBB", "AAAAAAAA", "BBBBBBBB")
    assert result == "key={0} token={0}".format(PLACEHOLDER)


def test_an_exception_is_accepted_and_returned_as_text():
    # Callers pass the exception object itself, not str(e): every one of them would
    # otherwise be one forgotten str() away from a TypeError on the error path — the
    # path that is least exercised and worst to break.
    error = ConnectionError("failed at /v2/api?apikey={}".format(SECRET))
    result = redact(error, SECRET)
    assert isinstance(result, str)
    assert SECRET not in result


def test_an_absent_or_empty_secret_leaves_the_text_alone():
    # TELEGRAM_BOT_TOKEN is optional by design, so `None` here is a supported
    # deployment. And `"x".replace("", "***")` splices the placeholder between every
    # character, which would shred every error message in the service on behalf of a
    # variable that was never set.
    text = "nothing secret here"
    assert redact(text, None) == text
    assert redact(text, "") == text
    assert redact(text, None, "", SECRET) == text


def test_no_secrets_at_all_is_a_plain_stringification():
    assert redact("plain") == "plain"
    assert redact(42) == "42"


# --- the length guard --------------------------------------------------------

@pytest.mark.parametrize("length,replaced", [
    (MIN_SECRET_LENGTH - 1, False),
    (MIN_SECRET_LENGTH, True),
])
def test_a_value_below_the_minimum_length_is_left_in_place(length, replaced):
    """The boundary itself, from both sides.

    Replacement is blind, so a short value would put `***` into timestamps and
    addresses; the guard refuses on length alone. A credential below the line
    would leak, so `src/settings.py` uses the constant as `Secret`'s `min_length`
    (pinned in `tests/test_settings.py`).
    """
    secret = "x" * length
    text = "value={} end".format(secret)
    result = redact(text, secret)
    assert (secret not in result) is replaced
    assert (PLACEHOLDER in result) is replaced


def test_a_short_value_would_have_been_cut_out_of_text_that_is_not_the_secret():
    # A short value is a substring of ordinary text: replacing it here would turn
    # the request id into "***678" and hide nothing.
    text = "HTTP 500 for request 12345678, retries exceeded with url: /v2/api"
    assert redact(text, "12345") == text
