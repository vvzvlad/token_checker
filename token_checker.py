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
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Optional

from grist_api import GristDocAPI
import colorama
import requests


class HealthCheckHandler(BaseHTTPRequestHandler):
    is_healthy = True

    @classmethod
    def set_health(cls, status: bool):
        cls.is_healthy = status

    def do_GET(self):
        if self.path == '/health':
            if self.is_healthy:
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({"status": "healthy"}).encode())
            else:
                self.send_response(503)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({"status": "unhealthy", "reason": "Request in progress taking too long"}).encode())
        else:
            self.send_response(404)
            self.end_headers()


def run_health_server(port):
    server = HTTPServer(('0.0.0.0', port), HealthCheckHandler)
    server.serve_forever()


class GristWatchdog:
    def __init__(self, logger):
        self._timeout = 300  # 5 minutes in seconds
        self._lock = threading.Lock()
        self._running = True
        self._thread: Optional[threading.Thread] = None
        self.logger = logger
        self.reset_timeout()
        self.logger.info(f"Watchdog initialized with timeout: {self._timeout} seconds")
        self._thread = threading.Thread(target=self.decrease_timeout_thread, daemon=True)
        self._thread.start()
        self.logger.info("Watchdog thread started")

    def send_telegram_notification(self):
        try:
            telegram_bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
            telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID")
            if telegram_bot_token and telegram_chat_id:
                message = f"Watchdog timeout reached. No activity detected for {self._timeout} seconds. Application will be restarted."
                telegram_url = f"https://api.telegram.org/bot{telegram_bot_token}/sendMessage"
                requests.post(telegram_url, json={
                    "chat_id": telegram_chat_id,
                    "text": message
                }, timeout=10)
                self.logger.info("Telegram notification sent about watchdog timeout")
            else:
                self.logger.warning("Telegram notification skipped - missing bot token or chat ID")
        except Exception as e:
            self.logger.error(f"Failed to send Telegram notification: {e}")


    def reset_timeout(self):
        with self._lock:
            previous = self._timeout
            self._timeout = 300  # Reset to 5 minutes
            #if previous < 60:  # Логируем только если было меньше минуты
            self.logger.info(f"Watchdog timeout reset from {previous} to {self._timeout} seconds")
    
    def decrease_timeout_thread(self):
        while self._running:
            with self._lock:
                self._timeout = max(0, self._timeout - 10)
                #if self._timeout < 60:  # Логируем когда осталось меньше минуты
                self.logger.warning(f"Watchdog timeout decreased: {self._timeout} seconds")
                if self._timeout < 60:
                    HealthCheckHandler.set_health(False)
                if self._timeout < 1:
                    self.logger.error(f"Watchdog timeout reached. No activity detected for {self._timeout} seconds. Restarting application...")
                    self.send_telegram_notification()
                    sys.exit(1)
            time.sleep(10)
    
    def get_timeout(self):
        with self._lock:
            return self._timeout

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

    def find_settings(self, setting):
        data = getattr(self.fetch_table(self.settings_table)[0], setting)
        return data

    def find_chain(self, target_id, table):
        if target_id is None or target_id == "" or int(target_id) == 0:
            raise Exception("Chain is None!")
        data = self.grist.fetch_table(table)
        if len(data) == 0:
            raise Exception("Chains table is empty!")
        search_result = [row for row in data if row.id == target_id]
        if len(search_result) == 0:
            raise Exception(f"Chain not found!")
        chain_id = search_result[0].Chain_id
        if chain_id is None or chain_id == "":
            raise Exception(f"ID is None!")
        return chain_id

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


def check_balance(address, chain_id, api_key, token, logger):
    token_url = f"https://api.etherscan.io/v2/api?apikey={api_key}&chainid={chain_id}&module=account&action=tokenbalance&address={address}&contractaddress={token}"
    eth_url = f"https://api.etherscan.io/v2/api?apikey={api_key}&chainid={chain_id}&module=account&action=balance&address={address}"
    print(token_url, eth_url)
    try:
        if token.lower() == 'eth':
            response = requests.get(eth_url, timeout=30)
            data = response.json()
            if data['status'] == '1':
                eth_value = int(data['result']) / (10 ** 18)
                formatted_eth_value = f"{eth_value:.18f}".rstrip('0').rstrip('.')
                logger.info(f"Address {address} holds {formatted_eth_value} ETH")
                return formatted_eth_value, ""
            else:
                if 'message' in data:
                    if data['message'] == 'No transactions found':
                        logger.error(f"No transactions found for address {address}")
                        return 0, "No transactions found"
                logger.error(f"Error while checking ETH transactions for address {address}")
                raise Exception(f"Error while checking ETH transactions for address {address}")
        else:
            response = requests.get(token_url, timeout=30)
            data = response.json()
            if data['status'] == '1': 
                tokens = int(data['result'])
                logger.info(f"Address {address} holds {tokens} tokens")
                token_value = tokens / (10 ** 18)
                return token_value, ""
            else:
                if 'message' in data:
                    if data['message'] == 'No transactions found':
                        logger.error(f"No transactions found for address {address}")
                        return 0, "No transactions found"
                logger.error(f"Error while checking token transactions for address {address}")
                raise Exception(f"Error while checking token transactions for address {address}")
    except Exception as e:
        #logger.error(f"Fail: {e}\n{traceback.format_exc()}")
        logger.error(f"Error while checking token transactions for address {address}: {e}")
        raise Exception(f"Error while checking token transactions for address {address}: {e}")


def find_none_value(grist):
    wallets = grist.fetch_table()
    for wallet in wallets:
        if (wallet.Value is None or wallet.Value == "" ):
            if (wallet.Address is not None and wallet.Address != ""):
                return wallet
    return None
    

def main():
    colorama.init(autoreset=True)
    logger = logging.getLogger("Token checker")
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    grist_server = os.getenv("GRIST_SERVER")
    grist_doc_id = os.getenv("GRIST_DOC_ID")
    grist_api_key = os.getenv("GRIST_API_KEY")
    etherscan_api_key = os.getenv("ETHERSCAN_API_KEY")
    if grist_server is None or grist_doc_id is None or grist_api_key is None or etherscan_api_key is None:
        logger.error("Please set GRIST_SERVER, GRIST_DOC_ID, GRIST_API_KEY, ETHERSCAN_API_KEY env variables")
        sys.exit(1)
    nodes_table = "Wallets"
    settings_table = "Settings"
    chains_table = "Chains" 
    watchdog = GristWatchdog(logger)
    grist = GRIST(grist_server, grist_doc_id, grist_api_key, nodes_table, settings_table, logger)

    health_port = int(os.getenv('HEALTH_PORT', '8080'))
    health_thread = threading.Thread(target=run_health_server, args=(health_port,), daemon=True)
    health_thread.start()
    logger.info(f"Health check server started on port {health_port}")

    while True:
        try:
            watchdog.reset_timeout()
            chain = grist.find_settings("Chain")
            chain_id = grist.find_chain(chain, chains_table)
            logger.info(f"Chain: {chain}/{chain_id}")
            token = grist.find_settings("Token")
            try:
                none_value_wallet = find_none_value(grist)
                if none_value_wallet is None:
                    logger.info("All wallets have values, sleep 10s")
                    time.sleep(10)
                    continue
                
                logger.info(f"Check wallet {none_value_wallet.Address}/{chain_id}...")
                HealthCheckHandler.set_health(False)
                value, msg = check_balance(none_value_wallet.Address, chain_id, etherscan_api_key, token, logger)
                HealthCheckHandler.set_health(True)
                grist.update(none_value_wallet.id, {"Value": value, "Comment": msg})  
            except Exception as e:
                HealthCheckHandler.set_health(True)
                grist.update(none_value_wallet.id, {"Value": "--", "Comment": f"Error: {e}"})  
                logger.error(f"Error occurred: {e}")
        except Exception as e:
            HealthCheckHandler.set_health(True)
            logger.error(f"Error occurred, sleep 10s: {e}")
            time.sleep(10)


if __name__ == "__main__":
    main()
