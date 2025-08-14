import logging
from dataclasses import dataclass
from typing import Any

from keboola.component.base import ComponentBase, sync_action
from keboola.component.dao import TableDefinition
from keboola.component.exceptions import UserException
from keboola.csvwriter import ElasticDictWriter

from client import FacebookClient
from configuration import Configuration

PREFERRED_COLUMNS_ORDER = [
    "id",
    "ex_account_id",
    "fb_graph_node",
    "parent_id",
    "name",
    "key1",
    "key2",
    "ads_action_name",
    "action_type",
    "action_reaction",
    "value",
    "period",
    "end_time",
    "title",
    "publisher_platform",
]

PRIMARY_KEY_CANDIDATES = [
    "id",
    "parent_id",
    "key1",
    "key2",
    "end_time",
    "account_id",
    "campaign_id",
    "date_start",
    "date_stop",
    "ads_action_name",
    "action_type",
    "action_reaction",
    "ad_id",
    "publisher_platform",
    "ads_action_name",
    "adset_id"
]


@dataclass
class WriterCacheRecord:
    writer: ElasticDictWriter
    table_definition: TableDefinition


class Component(ComponentBase):
    def __init__(self):
        super().__init__()
        self._writer_cache: dict[str, WriterCacheRecord] = {}
        self.config = Configuration(**self.configuration.parameters)
        self.client: FacebookClient = FacebookClient(self.configuration.oauth_credentials, self.config.api_version)

    def run(self) -> None:
        self._write_accounts_from_config(self.config)
        self._process_queries(self.config)
        self._finalize_tables()

    def _write_accounts_from_config(self, config: Configuration) -> None:
        logging.info("Writing accounts table from configuration")
        accounts_data = [
            {
                key: value for key, value in {
                    "account_id": acc.account_id,
                    "id": acc.id,
                    "name": acc.name,
                    "business_name": acc.business_name,
                    "currency": acc.currency,
                    "category": acc.category,
                    "category_list": acc.category_list,
                    "tasks": acc.tasks,
                    "fb_page_id": acc.fb_page_id,
                }.items() if value is not None
            }
            for acc in config.accounts.values()
        ]
        if accounts_data:
            self._write_rows("accounts", accounts_data, ["id"], False)

    def _process_queries(self, config: Configuration) -> None:
        queries_to_process = [q for q in config.queries if not q.disabled]

        if not queries_to_process:
            return

        logging.info(f"Processing {len(queries_to_process)} queries.")

        for parsed_data in self.client.process_queries(list(config.accounts.values()), queries_to_process):
            for table_name, rows_list in parsed_data.items():
                if not rows_list:
                    continue
                primary_key = self._get_primary_key(rows_list)
                self._write_rows(table_name, rows_list, primary_key, True)
                logging.debug(f"Wrote batch of {len(rows_list)} rows to table {table_name}")

    def _finalize_tables(self) -> None:
        for table_name, cache_record in self._writer_cache.items():
            cache_record.writer.writeheader()
            cache_record.writer.close()
            self.write_manifest(cache_record.table_definition)

    def _write_rows(self, table_name: str, rows: list[dict], primary_key: list[str], incremental: bool) -> None:
        if not rows:
            return

        if table_name not in self._writer_cache:
            self._create_cached_writer(table_name, rows, primary_key, incremental)

        writer = self._writer_cache[table_name].writer
        for row in rows:
            writer.writerow(row)

    def _create_cached_writer(
        self, table_name: str, rows: list[dict], primary_key: list[str], incremental: bool
    ) -> None:
        # Build union of all columns across the batch
        all_columns_set: set[str] = set()
        for row in rows:
            all_columns_set.update(row.keys())

        # Reorder columns based on preferred order, then add remaining sorted
        ordered_columns = [col for col in PREFERRED_COLUMNS_ORDER if col in all_columns_set]
        remaining_columns = sorted(all_columns_set - set(PREFERRED_COLUMNS_ORDER))
        all_columns = ordered_columns + remaining_columns

        table_def = self.create_out_table_definition(
            f"{table_name}.csv",
            primary_key=primary_key,
            incremental=incremental,
        )

        writer = ElasticDictWriter(table_def.full_path, all_columns)
        self._writer_cache[table_name] = WriterCacheRecord(writer=writer, table_definition=table_def)

    def _get_primary_key(self, rows: list[dict[str, Any]]) -> list[str]:
        if not rows:
            return []
        # Union of keys across all rows in the batch
        available_columns: set[str] = set()
        for row in rows:
            available_columns.update(row.keys())
        primary_key = [col for col in PRIMARY_KEY_CANDIDATES if col in available_columns]
        return primary_key or (["id"] if "id" in available_columns else [])

    @sync_action("accounts")
    def run_accounts_action(self) -> list[dict[str, Any]]:
        return self.client.get_accounts("me/accounts", "id,business_name,name,category")

    @sync_action("adaccounts")
    def run_ad_accounts_action(self) -> list[dict[str, Any]]:
        return self.client.get_accounts("me/adaccounts", "account_id,id,business_name,name,currency")

    @sync_action("igaccounts")
    def run_ig_accounts_action(self) -> list[dict[str, Any]]:
        return self.client.get_accounts("me/accounts", "instagram_business_account,name,category")


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
