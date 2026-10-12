"""Etherscan balance lookups and the wallet pick-up rule.

How the Etherscan response is read decides what lands in the Grist `Value` cell.
The value goes into a text cell, so `f"{value:.18f}".rstrip('0')` keeps an
18-decimal token out of scientific notation, which nothing downstream parses.
"""

import requests

from src.redact import redact

# One outbound call per balance check.
ETHERSCAN_TIMEOUT = 30


class EtherscanError(Exception):
    """Etherscan answered, but not with a balance.

    Its own type because the class name travels: the handler below puts
    `type(e).__name__` in front of the text written to the wallet's `Comment` cell,
    and that one word is the operator's whole classification (ConnectionError,
    JSONDecodeError, KeyError, ... — an open set). This one says "Etherscan
    answered, and the answer was no".
    """


def check_balance(address, chain_id, api_key, token, logger, divider):
    token_url = f"https://api.etherscan.io/v2/api?apikey={api_key}&chainid={chain_id}&module=account&action=tokenbalance&address={address}&contractaddress={token}"
    eth_url = f"https://api.etherscan.io/v2/api?apikey={api_key}&chainid={chain_id}&module=account&action=balance&address={address}"
    # The two URLs carry ETHERSCAN_API_KEY as a query parameter: never log or print
    # them (and see the handler below: `requests` quotes them in network errors).
    #
    # `subject` names the branch in every message, including the handler's wrapper.
    # Bound above the `try`, ahead of `token.lower()` (the first statement that can
    # raise), because the handler reads it on every path. "token" until a branch is
    # chosen.
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
                    # An address Etherscan has never seen means zero, not a failure.
                    if data['message'] == 'No transactions found':
                        logger.error(f"No transactions found for address {address}")
                        return 0, "No transactions found"
                # What Etherscan said; the operation and address are added by the
                # handler below. `result` is where "Invalid API Key" and "Max rate
                # limit reached" live.
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
                # Same wording as the ETH branch; `subject` names the branch.
                raise EtherscanError(
                    "Etherscan answered status={!r}, message={!r}, result={!r}".format(
                        data.get('status'), data.get('message'), data.get('result')))
    except Exception as e:
        # `redact` first: on a transport failure `requests` quotes the full URL,
        # api key included, and this text goes to `docker logs` and the wallet's
        # `Comment` cell. The class name is prepended because a message can be a
        # single quoted word (`KeyError('status')`).
        reason = f"{type(e).__name__}: {redact(e, api_key)}"
        # Re-wrapped so the `Comment` cell names the address. `from None` drops
        # the implicit `__context__` (the unredacted original with the api key),
        # which any chain renderer (format_exc, exc_info=True, an unhandled exit)
        # would print.
        logger.error(f"Error while checking {subject} transactions for address {address}: "
                     f"{reason}")
        raise Exception(
            f"Error while checking {subject} transactions for address {address}: {reason}"
        ) from None


def find_none_value(grist, table):
    """The first wallet with an address and no value yet, or None.

    "First" is the table's own order and is deliberate: the loop processes one
    wallet per iteration, so clearing a `Value` cell in Grist is what re-arms a
    wallet, and it is picked up in the order the document shows.
    """
    wallets = grist.fetch_table(table)
    for wallet in wallets:
        if (wallet.Value is None or wallet.Value == ""):
            if (wallet.Address is not None and wallet.Address != ""):
                return wallet
    return None
