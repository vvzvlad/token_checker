# pylint: disable=broad-exception-raised,too-many-arguments
import subprocess
import time
import json
import logging
import sys
import random
import os
import re
import traceback
from datetime import datetime, timedelta, timezone

from grist_api import GristDocAPI
import colorama
import requests


class GRIST:
    def __init__(self, server, doc_id, api_key, nodes_table, settings_table, logger):
        self.server = server
        self.doc_id = doc_id
        self.api_key = api_key
        self.nodes_table = nodes_table.replace(" ", "_")
        self.settings_table = settings_table.replace(" ", "_")
        self.logger = logger
        self.grist = GristDocAPI(doc_id, server=server, api_key=api_key)


    def to_timestamp(self, dtime: datetime) -> int:
        if dtime.tzinfo is None:
            dtime = dtime.replace(tzinfo=timezone(timedelta(hours=3))) 
        return int(dtime.timestamp())

    def update_column(self, row_id, column_name, value, table=None):
        if isinstance(value, datetime):
            value = self.to_timestamp(value)
        column_name = column_name.replace(" ", "_")
        self.grist.update_records(table or self.nodes_table, [{ "id": row_id, column_name: value }])

    def update(self, row_id, updates, table=None):
        for column_name, value in updates.items():
            if isinstance(value, datetime):
                updates[column_name] = self.to_timestamp(value)
        updates = {column_name.replace(" ", "_"): value for column_name, value in updates.items()}
        self.grist.update_records(table or self.nodes_table, [{"id": row_id, **updates}])

    def fetch_table(self, table=None):
        return self.grist.fetch_table(table or self.nodes_table)

    def find_record(self, record_id=None, state=None, name=None, table=None):
        table = self.grist.fetch_table(table or self.nodes_table)
        if record_id is not None:
            record = [row for row in table if row.id == record_id]
            return record
        if state is not None and name is not None:
            record = [row for row in table if row.State == state and row.name == name]
            return record
        if state is not None:
            record = [row for row in table if row.State == state]
            return record
        if name is not None:
            record = [row for row in table if row.Name == name]
            return record

    def find_settings(self, setting, table=None):
        data = self.grist.fetch_table(table or self.settings_table)
        return [row for row in data if row.Setting == setting][0].Value

    def nodes_table_preprocessing(self):
        current_time = self.to_timestamp(datetime.now())
        max_wip = 60*60*2

        self.logger.info(f"Run grist processing NoneState -> Dirty and NoneVersion -> av1")
        for row in self.fetch_table():
            if row.State == "": self.update_column(row.id, "State", "Dirty")
            if row.Version == "": self.update_column(row.id, "Version", "av1")

        self.logger.info(f"Run grist processing av1 and !WiP -> Dirty")
        for row in self.fetch_table():
            if row.Version == "av1" and row.State != "WiP" and row.State != "Dirty" and row.State != "Error": 
                self.update_column(row.id, "State", "Dirty")
                self.update_column(row.id, "Status", "Set Dirty by old Version")
                
        self.logger.info(f"Run grist processing WiP and >1d old -> Dirty")
        for row in self.fetch_table():
            if row.Deploy_date is not None:
                vm_old_age = current_time - row.Deploy_date
                if row.State == "WiP" and vm_old_age > max_wip and row.State != "Dirty":
                    self.update_column(row.id, "State", "Dirty")
                    self.update_column(row.id, "Status", "Set Dirty by WiP Timeout")

        self.logger.info(f"Run grist processing NoneRetries -> 0/4")
        for row in self.fetch_table():
            if row.Retries is None or row.Retries == "":
                self.update_column(row.id, "Retries", "0/4")


def check_token_balance(address, api_endpoint, token_symbol, logger):
    url = f"{api_endpoint}&module=account&action=tokentx&address={address}"
    try:
        response = requests.get(url)
        data = response.json()
        if data['status'] == '1':
            transactions = data['result']
            for tx in transactions:
                if tx['tokenSymbol'] == token_symbol:
                    token_value = int(tx['value']) / (10 ** int(tx['tokenDecimal']))
                    logger.info(f"Address {address} holds {token_value} {token_symbol}")
                    return token_value, ""
            logger.error(f"No transactions found for token {token_symbol} at address {address}")
            return 0, "Token not found"
        else:
            if 'message' in data:
                if data['message'] == 'No transactions found':
                    logger.error(f"No transactions found for address {address}")
                    return 0, "No transactions found"
            logger.error(f"Error while checking token transactions for address {address}")
            raise Exception(f"Error while checking token transactions for address {address}")
    except Exception as e:
        logger.error(f"Fail: {e}\n{traceback.format_exc()}")
        logger.error(f"Error while checking token transactions for address {address}: {e}")
        raise Exception(f"Error while checking token transactions for address {address}: {e}")



def main():
    colorama.init(autoreset=True)
    logger = logging.getLogger("Token checker")
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    server = os.getenv("GRIST_SERVER")
    doc_id = os.getenv("GRIST_DOC_ID")
    api_key = os.getenv("GRIST_API_KEY")
    nodes_table = os.getenv("GRIST_NODES_TABLE")
    settings_table = os.getenv("GRIST_SETTINGS_TABLE")
    grist = GRIST(server, doc_id, api_key, nodes_table, settings_table, logger)
    while True:
        try:
            endpoint = grist.find_settings("Api endpoint")
            logger.info(f"Endpoint: {endpoint}")
            token_symbol = grist.find_settings("Token Symbol")
            logger.info(f"Token Symbol: {token_symbol}")
            wallets = grist.fetch_table()
            for wallet in wallets:
                try:
                    if wallet.Value == "" or wallet.Value is None:
                        logger.info(f"Check wallet {wallet.Address}...")
                        value, msg = check_token_balance(wallet.Address, endpoint, token_symbol, logger)
                        grist.update(wallet.id, {"Value": value})  
                        grist.update(wallet.id, {"Comment": msg})
                except Exception as e:
                    grist.update(wallet.id, {"Value": "--"})  
                    logger.error(f"Error occurred: {e}")
                    grist.update(wallet.id, {"Comment": f"Error: {e}"})
            time.sleep(10)
        except Exception as e:
            logger.error(f"Error occurred: {e}")
            time.sleep(10)


if __name__ == "__main__":
    main()
