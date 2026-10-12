"""Strip credential VALUES out of text that is about to leave the process.

This exists because of how `requests` reports failures. Every credential this
service holds travels in a URL — `ETHERSCAN_API_KEY` as a query parameter of the
Etherscan call, `TELEGRAM_BOT_TOKEN` as a path segment of the Telegram one — and
on any transport-level failure `requests` puts the FULL url, query string
included, into the text of the exception it raises:

    ProxyError: HTTPSConnectionPool(host='api.etherscan.io', port=443): Max
    retries exceeded with url: /v2/api?apikey=<the real key>&chainid=1&...

Every place where the text of an exception leaves this process (`docker logs`,
the wallet's `Comment` cell in Grist) passes it through `redact()` first.
"""

PLACEHOLDER = "***"

# Values shorter than this are not replaced: replacement is blind, and a short
# value would match timestamps, codes or address fragments. A credential that
# short would leak, so `src/settings.py` uses this constant as the `min_length` of
# its `Secret` and `OptionalSecret` aliases and rejects it at startup.
MIN_SECRET_LENGTH = 8


def redact(text, *secrets):
    """`text` as a string, with every secret value replaced by `***`.

    Empty or absent secrets are skipped (`"x".replace("", "***")` splices the
    placeholder between every character, and `TELEGRAM_BOT_TOKEN` is optional), as
    is anything shorter than `MIN_SECRET_LENGTH`.
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
