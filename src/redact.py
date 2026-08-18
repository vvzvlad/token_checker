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
# credential would plausibly sit — so a short, non-random value matches text that
# is not the secret at all: a timestamp, an exit code, a slice of a hex address,
# an ordinary word. Replacing those hides nothing and damages the message, which
# is the only reason anybody is reading the line. The guard is drawn around that
# SHAPE and around nothing else: it refuses to blind-replace anything short enough
# to turn up in the text by coincidence, whatever variable it was handed. Only
# credentials are ever passed in — an addressee like `TELEGRAM_CHAT_ID` is not
# handed to `redact()` anywhere, precisely because replacing it could only cost.
#
# The trade-off is stated rather than implied, because it IS one — a credential
# shorter than this WOULD be left in the text, silently, and a value that short
# cannot be redacted without destroying the message it is being redacted out of.
# That is why the floor is not left as a convention: `src/settings.py` imports
# this constant as the `min_length` of its `Secret` alias — and of the
# `OptionalSecret` one, which is the same floor for a credential that is allowed
# to be absent — so a credential too short to redact fails validation at startup
# instead of travelling out in the clear for the life of the deployment. Whether
# the keys currently configured clear the floor is deliberately written down
# NOWHERE in this repository: the alias is the whole of the claim, it is checked
# on every start, and it holds for whatever key is configured next.
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
