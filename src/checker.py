#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""The single-process loop: read wallets from Grist, ask Etherscan, write back.

One wallet per iteration. A wallet is "due" when its `Value` cell is empty and it
has an address, so an operator re-arms a wallet by clearing that cell. When there
is nothing due the loop sleeps ten seconds and looks again.

Two things run alongside the loop and both matter to the deployment: the
/health server (src/health.py), which is what the Dockerfile's HEALTHCHECK
curls; and the watchdog (src/watchdog.py), which kills the process if an
iteration never comes round.
"""

import logging
import time

import colorama
from grist_client import GristClient

from src.balances import check_balance, find_none_value
from src.health import HealthCheckHandler, start_health_server
from src.redact import redact
from src.settings import settings
from src.watchdog import GristWatchdog

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

    Anything that reaches a log line or a Grist cell goes through here, with all
    three credentials of this process (Etherscan key, Grist key, Telegram bot
    token): no credential value ever leaves the process.

    `telegram_chat_id` is not a credential and is never redacted: blind
    replacement of a short number would put `***` inside ids and timestamps.
    """
    return redact(error, settings.etherscan_api_key, settings.grist_api_key,
                  settings.telegram_bot_token)


def setting(grist, name):
    """The `name` column of the one-row Settings table.

    That table's columns are the settings, so `GristClient.get_setting`, which reads
    a Setting/Value list, does not fit it.
    """
    return getattr(grist.fetch_table(SETTINGS_TABLE)[0], name)


def find_chain(grist, target_id, table):
    if target_id is None or target_id == "" or int(target_id) == 0:
        raise Exception("Chain is None!")
    data = grist.fetch_table(table)
    if len(data) == 0:
        raise Exception("Chains table is empty!")
    search_result = [row for row in data if row.id == target_id]
    if len(search_result) == 0:
        raise Exception("Chain not found!")
    chain_id = search_result[0].Chain_id
    if chain_id is None or chain_id == "":
        raise Exception("ID is None!")
    return chain_id


def run():
    logger = build_logger()

    # Order matters: the watchdog is armed BEFORE the Grist client is built, so a
    # construction that somehow blocks is covered too.
    watchdog = GristWatchdog(
        logger,
        telegram_bot_token=settings.telegram_bot_token,
        telegram_chat_id=settings.telegram_chat_id,
        telegram_bot_api_server=settings.telegram_bot_api_server)
    watchdog.start()

    grist = GristClient(settings.grist_server, settings.grist_doc_id, settings.grist_api_key)

    start_health_server(settings.health_port, logger)

    while True:
        try:
            watchdog.reset_timeout()
            # An iteration has begun, so the service is making progress. Without
            # this, a flag the watchdog lowered (below 60 s remaining) would stay
            # down in the idle "nothing to do" branch, and auto-heal would restart
            # a working service.
            HealthCheckHandler.set_health(True)
            chain = setting(grist, "Chain")
            chain_id = find_chain(grist, chain, CHAINS_TABLE)
            logger.info(f"Chain: {chain}/{chain_id}")
            token = setting(grist, "Token")
            divider = parse_divider(setting(grist, "Divider"), logger)

            # Chosen in the outer `try`, not the inner one: `none_value_wallet` stays
            # bound across iterations, so a read failure under the inner handler would
            # overwrite the PREVIOUS iteration's wallet with "--".
            none_value_wallet = find_none_value(grist, NODES_TABLE)
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
                grist.update(NODES_TABLE, none_value_wallet.id, {"Value": value, "Comment": msg})
            except Exception as e:
                HealthCheckHandler.set_health(True)
                # "--" rather than an empty cell: an empty `Value` marks a wallet
                # as due, and the loop would spin on it forever.
                reason = safe_text(e)
                # Logged before the write: if Grist is down, `grist.update` raises
                # to the outer handler and the reason would otherwise be lost.
                logger.error(f"Error occurred: {reason}")
                grist.update(NODES_TABLE, none_value_wallet.id,
                             {"Value": "--", "Comment": f"Error: {reason}"})
        except Exception as e:
            HealthCheckHandler.set_health(True)
            logger.error(f"Error occurred, sleep 10s: {safe_text(e)}")
            time.sleep(10)
