"""Strip credential VALUES out of text that is about to leave the process.

This exists because of how `requests` reports failures. Every credential this
service holds travels in a URL — `ETHERSCAN_API_KEY` as a query parameter of the
Etherscan call, `TELEGRAM_BOT_TOKEN` as a path segment of the Telegram one — and
on any transport-level failure `requests` puts the FULL url, query string
included, into the text of the exception it raises:

    ProxyError: HTTPSConnectionPool(host='api.etherscan.io', port=443): Max
    retries exceeded with url: /v2/api?apikey=<the real key>&chainid=1&...

So a program that never logs a URL still leaks the key the moment it logs `{e}`.
Two destinations make that expensive here: `docker logs`, readable by anyone who
can reach the daemon and kept for as long as the log is, and — worse — the
wallet's `Comment` cell in Grist, a document people open, share by link and put
into backups.

Every place where the text of an exception leaves this process therefore passes
it through `redact()` first.
"""

PLACEHOLDER = "***"

# Values shorter than this are NOT replaced. Replacement here is blind — every
# occurrence of the value anywhere in the text, with no notion of where a
# credential would plausibly sit — so a short value matches text that is not the
# secret at all: a timestamp, an exit code, a hex address, an ordinary word. Both
# callers pass `TELEGRAM_CHAT_ID` through here and that one is not a credential
# but a short, usually numeric identifier; a four-character chat id would put
# `***` in arbitrary places in every message the service writes.
#
# Nothing this service actually has to hide is that short: the Etherscan and
# Grist API keys are 32 characters, a Telegram bot token is over 40. The
# trade-off is stated rather than implied, because it IS one — a credential
# shorter than this would be left in the text — and a value that short cannot be
# redacted without destroying the message it is being redacted out of.
MIN_SECRET_LENGTH = 8


def redact(text, *secrets):
    """`text` as a string, with every secret value replaced by `***`.

    Secrets that are empty or absent are skipped rather than replaced, and that
    is not a nicety: `"anything".replace("", "***")` splices the placeholder
    between every single character, so one unset optional variable
    (`TELEGRAM_BOT_TOKEN` is optional by design) would turn every error message
    in the service into unreadable confetti. Anything shorter than
    `MIN_SECRET_LENGTH` is skipped for the same reason in a milder form.
    """
    result = str(text)
    for secret in secrets:
        if not secret:
            continue
        value = str(secret)
        if len(value) < MIN_SECRET_LENGTH:
            continue
        result = result.replace(value, PLACEHOLDER)
    return result
