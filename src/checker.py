#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""The single-process loop: read wallets from Grist, ask Etherscan, write back.

One wallet per iteration. A wallet is "due" when its `Value` cell is empty and it
has an address, so an operator re-arms a wallet by clearing that cell. When there
is nothing due the loop sleeps ten seconds and looks again.

Three things run alongside the loop and all three matter to the deployment:
the /health server (src/health.py), which is what the Dockerfile's HEALTHCHECK
curls; the watchdog (src/watchdog.py), which kills the process if an iteration
never comes round; and the requests timeout patch (src/http_timeout.py), without
which a stalled Grist connection hangs the process for good.
"""

import logging
import time

import colorama

from src.balances import check_balance, find_none_value
from src.grist import GRIST
from src.health import HealthCheckHandler, start_health_server
from src.http_timeout import install_default_timeout
from src.redact import redact
from src.settings import settings
from src.watchdog import GristWatchdog

# grist_api issues requests without a timeout, so a stalled Grist connection
# would otherwise hang the process forever. Installed at import time, before any
# HTTP call can be made.
install_default_timeout()

# The three tables of the Grist document. They are that document's names, so they
# are not configuration and have no environment variable.
NODES_TABLE = "Wallets"
SETTINGS_TABLE = "Settings"
CHAINS_TABLE = "Chains"

# Fallback for a `Divider` cell that is neither 18 nor 6. 18 is the ERC-20 default
# and the value most of this document's chains use.
DEFAULT_DIVIDER = 18


def build_logger():
    colorama.init(autoreset=True)
    logger = logging.getLogger("Token checker")
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger


def parse_divider(divider, logger):
    """The Grist `Divider` cell as an integer power of ten.

    The cell is authored by hand and reads like "18 (ETH)" as often as it reads
    "18", so the number is looked for inside the string rather than parsed from
    it. An unrecognised value warns and falls back instead of stopping the loop:
    a wrong divider is a wrong number in one cell, an exception here is a service
    that checks nothing at all.
    """
    if isinstance(divider, str):
        if "18" in divider:
            return 18
        if "6" in divider:
            return 6
        logger.warning(f"Unknown divider format: {divider}, using default value {DEFAULT_DIVIDER}")
        return DEFAULT_DIVIDER
    return divider


def safe_text(error):
    """The text of `error` with every credential this process holds removed.

    Anything that reaches a log line or a Grist cell goes through here. The
    Etherscan key is the one that actually travels (it is a query parameter of
    the URL `requests` quotes back in its own exceptions, see `src/redact.py`),
    but the other two are listed because the rule worth keeping is "no
    credential value ever leaves the process", not "not this one".

    `telegram_chat_id` is in that set on principle and is not a credential at
    all: it is a short, usually numeric identifier, and blindly replacing one in
    arbitrary text is how `***` ends up in the middle of a request id or a
    timestamp. What keeps it harmless is `MIN_SECRET_LENGTH` in `src/redact.py`,
    below which nothing is replaced.
    """
    return redact(error, settings.etherscan_api_key, settings.grist_api_key,
                  settings.telegram_bot_token)


def run():
    logger = build_logger()

    # Order matters: the watchdog is armed BEFORE the Grist client is built, so a
    # construction that somehow blocks is covered too.
    watchdog = GristWatchdog(
        logger,
        telegram_bot_token=settings.telegram_bot_token,
        telegram_chat_id=settings.telegram_chat_id)
    watchdog.start()

    grist = GRIST(settings.grist_server, settings.grist_doc_id, settings.grist_api_key,
                  NODES_TABLE, SETTINGS_TABLE, logger)

    start_health_server(settings.health_port, logger)

    while True:
        try:
            watchdog.reset_timeout()
            # An iteration has begun, so the service is by definition making
            # progress: put the health flag back up. Every other branch below
            # either never lowers it or raises it again — except the "nothing to
            # do" one, and that is the branch a settled deployment spends all its
            # time in. Without this line a flag the watchdog lowered once (it
            # lowers it below 60 s remaining) would stay down FOREVER after the
            # loop recovered: /health answers 503, docker calls the container
            # unhealthy, and auto-heal restarts a service that is working.
            HealthCheckHandler.set_health(True)
            chain = grist.find_settings("Chain")
            chain_id = grist.find_chain(chain, CHAINS_TABLE)
            logger.info(f"Chain: {chain}/{chain_id}")
            token = grist.find_settings("Token")
            divider = parse_divider(grist.find_settings("Divider"), logger)

            # Reading Grist to CHOOSE the wallet is deliberately out here, in the
            # outer `try`, and not one line further down inside the inner one.
            # `none_value_wallet` is a local of `run()` while the loop is inside
            # `run()`, so the name stays bound across iterations: with this call
            # under the inner handler, a transient read failure sent that handler
            # to `grist.update(none_value_wallet.id, ...)` holding the PREVIOUS
            # iteration's wallet, and overwrote its freshly computed balance with
            # "--". Do not move it back for symmetry.
            none_value_wallet = find_none_value(grist)
            if none_value_wallet is None:
                logger.info("All wallets have values, sleep 10s")
                time.sleep(10)
                continue

            try:
                logger.info(f"Check wallet {none_value_wallet.Address}/{chain_id}...")
                # Unhealthy for the duration of the outbound call: a lookup that
                # never returns is exactly the state the health endpoint exists to
                # report, and the watchdog counts down through it.
                HealthCheckHandler.set_health(False)
                value, msg = check_balance(
                    none_value_wallet.Address, chain_id, settings.etherscan_api_key,
                    token, logger, divider)
                HealthCheckHandler.set_health(True)
                grist.update(none_value_wallet.id, {"Value": value, "Comment": msg})
            except Exception as e:
                HealthCheckHandler.set_health(True)
                # "--" rather than an empty cell: an empty `Value` is what marks a
                # wallet as due, so writing nothing here would put this wallet
                # straight back at the head of the queue and the loop would spin
                # on it forever. Everything this handler can catch happened WHILE
                # working on `none_value_wallet` — that is the whole reason the
                # wallet lookup above is not inside this `try` — so the wallet
                # named here is always the one the message is about.
                reason = safe_text(e)
                # LOGGED BEFORE IT IS WRITTEN, and that order is the whole point:
                # `grist.update` is itself a network call, and the moment errors
                # come in a batch is exactly the moment Grist is the thing that
                # is down. With the write first, its own failure jumps to the
                # outer handler, this line never runs, and the only thing anybody
                # ever sees is "Grist is unreachable" — the reason the balance
                # check failed reaches neither the cell nor the log and is gone.
                logger.error(f"Error occurred: {reason}")
                grist.update(none_value_wallet.id,
                             {"Value": "--", "Comment": f"Error: {reason}"})
        except Exception as e:
            HealthCheckHandler.set_health(True)
            logger.error(f"Error occurred, sleep 10s: {safe_text(e)}")
            time.sleep(10)
