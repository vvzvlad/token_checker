"""Thin wrapper around grist_api: column-name sanitising and datetime coercion.

Moved out of the old top-level `token_checker.py` unchanged in behaviour. Two
methods that came with that file are deliberately NOT here — `find_record` and
`nodes_table_preprocessing`. They were copy-paste from a sibling project, they
read columns this document does not have (`State`, `Version`, `Deploy_date`,
`Retries`), and nothing ever called them; carrying them along would keep
suggesting that the Wallets table has a lifecycle it does not have.

The table names (`Wallets`, `Settings`, `Chains`) and the setting names (`Chain`,
`Token`, `Divider`) are the Grist document's, not ours: renaming any of them here
renames nothing there and simply stops the loop.
"""

from datetime import datetime, timedelta, timezone

from grist_api import GristDocAPI  # type: ignore


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
        # Naive datetimes are read as Moscow time (UTC+3), which is what the Grist
        # document stores. Guessing the runner's local zone instead would move
        # every written timestamp by however many hours the container's TZ is off.
        if dtime.tzinfo is None:
            dtime = dtime.replace(tzinfo=timezone(timedelta(hours=3)))
        return int(dtime.timestamp())

    def update_column(self, row_id, column_name, value, table=None):
        if isinstance(value, datetime):
            value = self.to_timestamp(value)
        # Grist accepts a column by its IDENTIFIER, which is the label with spaces
        # replaced — a write to "Chain id" is rejected outright.
        column_name = column_name.replace(" ", "_")
        self.grist.update_records(table or self.nodes_table, [{"id": row_id, column_name: value}])

    def update(self, row_id, updates, table=None):
        for column_name, value in updates.items():
            if isinstance(value, datetime):
                updates[column_name] = self.to_timestamp(value)
        updates = {column_name.replace(" ", "_"): value for column_name, value in updates.items()}
        self.grist.update_records(table or self.nodes_table, [{"id": row_id, **updates}])

    def fetch_table(self, table=None):
        return self.grist.fetch_table(table or self.nodes_table)

    def find_settings(self, setting):
        # The Settings table of this document holds ONE row whose columns are the
        # settings (`Chain`, `Token`, `Divider`) — not a name/value list, which is
        # how the sibling projects' Settings tables are shaped. Reading row 0 by
        # attribute is therefore correct here and would be wrong there.
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
            raise Exception("Chain not found!")
        chain_id = search_result[0].Chain_id
        if chain_id is None or chain_id == "":
            raise Exception("ID is None!")
        return chain_id
