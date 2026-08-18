"""Cutting credential values out of text that is about to leave the process.

Small module, but it is the thing standing between an api key and `docker logs` /
a shared Grist document, so the three shapes that would break it silently — a
secret that is absent, a secret that is the empty string, and a "secret" so short
that replacing it shreds the message around it — are pinned here rather than
discovered in production.
"""

import pytest

from src.redact import MIN_SECRET_LENGTH, PLACEHOLDER, redact

# Long enough to be replaced at all — see the boundary test at the bottom. Every
# secret in this file is at least MIN_SECRET_LENGTH characters, because the real
# ones are 32 and up and a placeholder shorter than the guard would make these
# tests pass for a reason that has nothing to do with what they check.
SECRET = "SECRET-api-key"


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

    Replacement is blind, so a short value matches text that is not the secret:
    `TELEGRAM_CHAT_ID` is passed to `redact()` by both callers and is a short,
    usually numeric identifier, and a four-character one would put `***` into
    timestamps, hex addresses and ordinary words in every message the service
    writes. Nothing that actually has to be hidden here is that short — the API
    keys are 32 characters and the bot token over 40.
    """
    secret = "x" * length
    text = "value={} end".format(secret)
    result = redact(text, secret)
    assert (secret not in result) is replaced
    assert (PLACEHOLDER in result) is replaced


def test_a_short_chat_id_does_not_pepper_the_message_with_placeholders():
    # The case the guard exists for, in the shape it arrives in: `safe_text()` in
    # src/checker.py passes TELEGRAM_CHAT_ID as its third secret, and a short
    # numeric id is a SUBSTRING of digits that occur naturally in an error message
    # — here it would cut the request id in half and leave "***678".
    text = "HTTP 500 for request 12345678, retries exceeded with url: /v2/api"
    assert redact(text, "12345") == text
