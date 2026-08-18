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


def check_balance(address, chain_id, api_key, token, logger, divider):
    token_url = f"https://api.etherscan.io/v2/api?apikey={api_key}&chainid={chain_id}&module=account&action=tokenbalance&address={address}&contractaddress={token}"
    eth_url = f"https://api.etherscan.io/v2/api?apikey={api_key}&chainid={chain_id}&module=account&action=balance&address={address}"
    # The two URLs above carry ETHERSCAN_API_KEY as a query parameter and are
    # deliberately NOT logged or printed. They used to be (`print(eth_url)` on
    # every call), which put the key in plain text into `docker logs` — readable
    # by anyone who can reach the daemon, and kept for as long as the log is.
    # Not printing them is only half of it: see the handler at the bottom, where
    # `requests` hands the same URL back inside the text of every network error.
    try:
        if token.lower() == 'eth':
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
                logger.error(f"Error while checking ETH transactions for address {address}")
                raise Exception(f"Error while checking ETH transactions for address {address}")
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
                logger.error(f"Error while checking token transactions for address {address}")
                raise Exception(f"Error while checking token transactions for address {address}")
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
        # "'status'" says nothing at all.
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
        logger.error(f"Error while checking token transactions for address {address}: {reason}")
        raise Exception(
            f"Error while checking token transactions for address {address}: {reason}"
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
