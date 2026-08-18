"""Cutting credential values out of text that is about to leave the process.

Small module, but it is the thing standing between an api key and `docker logs` /
a shared Grist document, so the three shapes that would break it silently — a
secret that is absent, a secret that is the empty string, and a "secret" so short
that replacing it shreds the message around it — are pinned here rather than
discovered in production.
"""

import pytest
from conftest import long_enough

from src.redact import MIN_SECRET_LENGTH, PLACEHOLDER, redact

# Long enough to be replaced at all — see the boundary test at the bottom. A
# stand-in shorter than the guard would make every test using it pass because
# nothing was replaced, which is not what any of them is checking, so the length
# is DERIVED from the constant rather than written at a value that clears it
# today. (The tests further down that deliberately go under the guard are about
# the guard itself and stay literal.)
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

    Replacement is blind — every occurrence of the value, anywhere in the text —
    so a value short enough to occur by coincidence puts `***` into timestamps,
    hex addresses and ordinary words instead of over a secret, damaging the
    message without hiding anything. That is what the guard refuses to do, and it
    refuses on LENGTH alone, knowing nothing about which variable it was handed.

    The other side of the same trade is that a credential below the line would be
    left in the text in full, so the constant is not allowed to be a lone
    convention: `src/settings.py` imports it as the `min_length` of its `Secret`
    alias, and `tests/test_settings.py` pins that a shorter credential is refused
    at startup.
    """
    secret = "x" * length
    text = "value={} end".format(secret)
    result = redact(text, secret)
    assert (secret not in result) is replaced
    assert (PLACEHOLDER in result) is replaced


def test_a_short_value_would_have_been_cut_out_of_text_that_is_not_the_secret():
    # What the guard actually protects, in the shape the damage takes: a short run
    # of characters is a SUBSTRING of things that occur naturally in an error
    # message. Here the "secret" sits inside a request id that has nothing to do
    # with it, and replacing it would leave the operator reading "***678" — the id
    # destroyed, and nothing hidden, because the value was never in this text as a
    # secret in the first place.
    #
    # No caller passes anything this short today, and that is the point of the
    # guard rather than an argument against it: it is what makes `redact()` safe
    # to hand a value without first proving where that value can appear.
    text = "HTTP 500 for request 12345678, retries exceeded with url: /v2/api"
    assert redact(text, "12345") == text
