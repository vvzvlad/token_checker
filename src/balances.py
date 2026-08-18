"""Etherscan balance lookups and the wallet pick-up rule.

Everything about how the Etherscan response is read — which branch answers for
ETH and which for a token, what `status == '1'` means, why `No transactions
found` is not an error, how the raw integer is divided and formatted — decides
what lands in the Grist `Value` cell, so it is kept exactly as it was.

The formatting deserves a note, because it looks like cosmetics and is not: the
value is written into a Grist text cell, so `f"{value:.18f}".rstrip('0')` is what
keeps an 18-decimal token from arriving in scientific notation, which nothing
downstream parses back.
"""

import requests

from src.redact import redact

# One outbound call per balance check. The explicit timeout survives the global
# default installed by src/http_timeout.py (that one only fills in a MISSING
# timeout), so this number is the one in force here.
ETHERSCAN_TIMEOUT = 30


class EtherscanError(Exception):
    """Etherscan answered, but not with a balance.

    Its own type rather than a bare `Exception`, because the class NAME travels:
    the handler below puts `type(e).__name__` in front of the text that goes into
    the wallet's `Comment` cell, and "Exception:" there tells the operator nothing
    they did not already know.

    What that handler catches is `Exception`, so the set it has to distinguish
    between is open and nobody should write it down as closed. Everything below
    has been seen to reach it and the list is a SAMPLE, not an inventory:
    `ConnectionError` and the rest of the transport family (no answer arrived at
    all), `JSONDecodeError` (a body that is not JSON — a proxy's own page, an
    error page from whatever sits in front of the API), `KeyError` (JSON without
    the field the branch reads), `TypeError` (a JSON list where an object was
    expected), `ValueError` (a `result` that is not a number), `AttributeError` (a
    Grist `Token` cell that is not a string, which fails before either branch is
    chosen) — and whatever shape a bad answer takes next.

    The operator gets ONE line in a spreadsheet cell out of all of that, so the
    single word in front of the text is the whole of the classification. This type
    is what makes "Etherscan answered, and the answer was no" a distinct word
    among them instead of a fifth "Exception:".
    """


def check_balance(address, chain_id, api_key, token, logger, divider):
    token_url = f"https://api.etherscan.io/v2/api?apikey={api_key}&chainid={chain_id}&module=account&action=tokenbalance&address={address}&contractaddress={token}"
    eth_url = f"https://api.etherscan.io/v2/api?apikey={api_key}&chainid={chain_id}&module=account&action=balance&address={address}"
    # The two URLs above carry ETHERSCAN_API_KEY as a query parameter and are
    # deliberately NOT logged or printed. They used to be (`print(eth_url)` on
    # every call), which put the key in plain text into `docker logs` — readable
    # by anyone who can reach the daemon, and kept for as long as the log is.
    # Not printing them is only half of it: see the handler at the bottom, where
    # `requests` hands the same URL back inside the text of every network error.
    #
    # Which of the two branches this call is about, in one word, for every message
    # that names the operation — including the wrapper raised by the handler at
    # the bottom. That wrapper used to say "token" unconditionally, so an ETH
    # failure reached the wallet's `Comment` cell as two sentences contradicting
    # each other ("Error while checking token transactions ...: ... Error while
    # checking ETH transactions ..."), and `from None` had removed the chain that
    # could have settled which one was true.
    #
    # Bound ahead of the first statement in this function that can raise at all,
    # which is the `token.lower()` on the line below — a Grist `Token` cell that is
    # not a string fails exactly there. The handler at the bottom reads `subject`
    # on EVERY path out of here, so a binding that any raising statement could get
    # in front of would replace the real failure with a NameError about the code
    # reporting it.
    #
    # Being outside the `try` is not itself the invariant — moved to the first line
    # inside it, this would satisfy the rule just as well today. It is the position
    # that cannot drift: a statement added at the top of the block later cannot end
    # up ahead of a binding that is already above the block.
    #
    # "token" is the honest value there: the branch was never chosen.
    subject = "token"
    try:
        if token.lower() == 'eth':
            subject = "ETH"
            response = requests.get(eth_url, timeout=ETHERSCAN_TIMEOUT)
            data = response.json()
            if data['status'] == '1':
                eth_value = int(data['result']) / (10 ** divider)
                if divider == 18:
                    formatted_eth_value = f"{eth_value:.18f}".rstrip('0').rstrip('.')
                else:
                    formatted_eth_value = f"{eth_value:.6f}".rstrip('0').rstrip('.')
                logger.info(f"Address {address} holds {formatted_eth_value} ETH")
                return formatted_eth_value, ""
            else:
                if 'message' in data:
                    # An address Etherscan has never seen is a legitimate answer,
                    # not a failure: it means zero, and the caller writes that
                    # zero into Grist instead of an error string.
                    if data['message'] == 'No transactions found':
                        logger.error(f"No transactions found for address {address}")
                        return 0, "No transactions found"
                # WHAT Etherscan said, not what we were doing when it said it. The
                # operation and the address are the handler's job below, and this
                # text is quoted inside that one — saying it twice is how the two
                # halves came to disagree in the first place. The answer itself is
                # what the `Comment` cell was missing: "NOTOK" plus the `result`
                # field is where "Invalid API Key" and "Max rate limit reached"
                # live, and both of those are the operator's next action.
                raise EtherscanError(
                    "Etherscan answered status={!r}, message={!r}, result={!r}".format(
                        data.get('status'), data.get('message'), data.get('result')))
        else:
            response = requests.get(token_url, timeout=ETHERSCAN_TIMEOUT)
            data = response.json()
            if data['status'] == '1':
                tokens = int(data['result'])
                logger.info(f"Address {address} holds {tokens} tokens")
                token_value = tokens / (10 ** divider)
                if divider == 18:
                    formatted_token_value = f"{token_value:.18f}".rstrip('0').rstrip('.')
                else:
                    formatted_token_value = f"{token_value:.6f}".rstrip('0').rstrip('.')
                return formatted_token_value, ""
            else:
                if 'message' in data:
                    if data['message'] == 'No transactions found':
                        logger.error(f"No transactions found for address {address}")
                        return 0, "No transactions found"
                # Same as the ETH branch above, and deliberately the same wording:
                # the branch is named once, by `subject`, in the handler below.
                raise EtherscanError(
                    "Etherscan answered status={!r}, message={!r}, result={!r}".format(
                        data.get('status'), data.get('message'), data.get('result')))
    except Exception as e:
        # `redact` before ANYTHING is done with the text, because on a transport
        # failure `requests` puts the full request URL — query string and all —
        # into the exception it raises, and the URLs built above carry the api
        # key in that query string. Without this the key goes out twice over: to
        # `docker logs` on the line below, and to the wallet's `Comment` cell in
        # Grist, because the caller writes the text of this exception there.
        #
        # The class of the failure is named separately, the way the watchdog names
        # it. Redaction can leave a message that is one quoted word long — the
        # malformed-payload path raises `KeyError('status')` — and that word is
        # what the operator finds in the `Comment` cell this text is written into.
        # "KeyError: 'status'" says a response arrived in an unexpected shape;
        # "'status'" says nothing at all. This is also why the two `raise`s above
        # use a type of their own: the same slot reading "Exception:" would be
        # pure noise, while "EtherscanError:" separates an answer that says no
        # from a `ConnectionError` that is no answer at all.
        reason = f"{type(e).__name__}: {redact(e, api_key)}"
        # Re-wrapped rather than re-raised bare: the caller writes this message
        # into the wallet's `Comment` cell, so it has to name the address it is
        # about — the operator reads the cell, not the log.
        #
        # `from None` is what keeps the redaction from being undone. This `raise`
        # runs INSIDE an `except`, so python implicitly sets `__context__` to `e`
        # — the original, UNREDACTED exception, whose text carries the api key in
        # the URL `requests` quotes back. Nothing here prints it today, but the
        # whole chain is what `traceback.format_exc()`, `logger.error(...,
        # exc_info=True)` and an unhandled exit to stderr all render: adding
        # `exc_info=True` anywhere upstream — a change that reads as better
        # diagnostics in review — would put the key straight into `docker logs`.
        # Suppressing the context costs nothing: the wrapper below already names
        # the address, the class and the redacted text.
        #
        # `subject` and not a hardcoded "token": this is the ONLY place either
        # branch is named now, so the cell and the log carry one statement about
        # what was being checked instead of two that disagree.
        logger.error(f"Error while checking {subject} transactions for address {address}: "
                     f"{reason}")
        raise Exception(
            f"Error while checking {subject} transactions for address {address}: {reason}"
        ) from None


def find_none_value(grist):
    """The first wallet with an address and no value yet, or None.

    "First" is the table's own order and is deliberate: the loop processes one
    wallet per iteration, so clearing a `Value` cell in Grist is what re-arms a
    wallet, and it is picked up in the order the document shows.
    """
    wallets = grist.fetch_table()
    for wallet in wallets:
        if (wallet.Value is None or wallet.Value == ""):
            if (wallet.Address is not None and wallet.Address != ""):
                return wallet
    return None
