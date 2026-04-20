import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from keboola.component.base import ComponentBase, sync_action
from keboola.component.dao import TableDefinition
from keboola.component.exceptions import UserException
from keboola.csvwriter import ElasticDictWriter
from keboola.vcr import DefaultSanitizer, ResponseUrlSanitizer

from client import FacebookClient
from configuration import Configuration

VCR_SANITIZERS = [
    DefaultSanitizer(additional_sensitive_fields=["page_token"]),
    ResponseUrlSanitizer(
        dynamic_params=["_nc_gid", "_nc_tpa", "_nc_oc", "_nc_ohc", "oh", "oe"],
        url_domains=["fbcdn.net", "facebook.com", "cdninstagram.com"],
    ),
]

logger = logging.getLogger(__name__)

PREFERRED_COLUMNS_ORDER = [
    "id",
    "ex_account_id",
    "fb_graph_node",
    "parent_id",
    "date_start",
    "date_stop",
    "name",
    "key1",
    "key2",
    "ads_action_name",
    "action_canvas_component_name",
    "action_carousel_card_id",
    "action_carousel_card_name",
    "action_destination",
    "action_device",
    "action_reaction",
    "action_target_id",
    "action_type",
    "action_video_sound",
    "action_video_type",
    "ad_format_asset",
    "value",
    "period",
    "end_time",
    "title",
    "age",
    "app_id",
    "body_asset",
    "call_to_action_asset",
    "country",
    "description_asset",
    "device_platform",
    "dma",
    "frequency_value",
    "gender",
    "hourly_stats_aggregated_by_advertiser_time_zone",
    "hourly_stats_aggregated_by_audience_time_zone",
    "image_asset",
    "impression_device",
    "is_conversion_id_modeled",
    "link_url_asset",
    "place_page_id",
    "placement",
    "platform_position",
    "product_id",
    "publisher_platform",
    "region",
    "skan_campaign_id",
    "skan_conversion_id",
    "title_asset",
    "user_segment_key",
    "video_asset",
    "reviewer_id",
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
    "action_canvas_component_name",
    "action_carousel_card_id",
    "action_carousel_card_name",
    "action_destination",
    "action_device",
    "action_reaction",
    "action_target_id",
    "action_type",
    "action_video_sound",
    "action_video_type",
    "ad_format_asset",
    "ad_id",
    "publisher_platform",
    "adset_id",
    "age",
    "app_id",
    "body_asset",
    "call_to_action_asset",
    "country",
    "description_asset",
    "device_platform",
    "dma",
    "frequency_value",
    "gender",
    "hourly_stats_aggregated_by_advertiser_time_zone",
    "hourly_stats_aggregated_by_audience_time_zone",
    "image_asset",
    "impression_device",
    "is_conversion_id_modeled",
    "link_url_asset",
    "place_page_id",
    "placement",
    "platform_position",
    "product_id",
    "region",
    "skan_campaign_id",
    "skan_conversion_id",
    "title_asset",
    "user_segment_key",
    "video_asset",
    "reviewer_id",
]


@dataclass
class WriterCacheRecord:
    writer: ElasticDictWriter
    table_definition: TableDefinition


class Component(ComponentBase):
    def __init__(self):
        super().__init__()
        self._writer_cache: dict[str, WriterCacheRecord] = {}
        params = self.configuration.parameters
        params["accounts"] = params.get("accounts") or {}
        self.config = Configuration(**params)
        self.client: FacebookClient = FacebookClient(self.configuration.oauth_credentials, self.config.api_version)
        self.bucket_id = self._retrieve_bucket_id()

    def run(self) -> None:
        self._write_accounts_from_config(self.config)
        self._process_queries(self.config)
        self._finalize_tables()

    def _write_accounts_from_config(self, config: Configuration) -> None:
        logger.info("Writing accounts table from configuration")
        accounts_data = []
        for acc in config.accounts.values():
            account_dict = {
                "account_id": acc.account_id,
                "id": acc.id,
                "name": acc.name,
                "business_name": acc.business_name,
                "currency": acc.currency,
                "category": acc.category,
                "category_list": acc.category_list,
                "tasks": acc.tasks,
                "fb_page_id": acc.fb_page_id,
            }
            # filter None values
            accounts_data.append(dict(filter(lambda x: x[1] is not None, account_dict.items())))

        if accounts_data:
            self._write_rows("accounts", accounts_data, ["id"], False)

    def _process_queries(self, config: Configuration) -> None:
        queries_to_process = [q for q in config.queries if not q.disabled]

        if not queries_to_process:
            return

        logger.info(f"Processing {len(queries_to_process)} queries.")

        for parsed_data in self.client.process_queries(list(config.accounts.values()), queries_to_process):
            for table_name, rows_list in parsed_data.items():
                if not rows_list:
                    continue
                primary_key = self._get_primary_key(rows_list)
                self._write_rows(table_name, rows_list, primary_key, True)
                logger.debug(f"Wrote batch of {len(rows_list)} rows to table {table_name}")

    def _finalize_tables(self) -> None:
        for cache_record in self._writer_cache.values():
            cache_record.writer.writeheader()
            cache_record.writer.close()
            self.write_manifest(cache_record.table_definition)

    def _write_rows(
        self,
        table_name: str,
        rows: list[dict],
        primary_key: list[str],
        incremental: bool,
    ) -> None:
        if not rows:
            return

        if table_name not in self._writer_cache:
            self._create_cached_writer(table_name, rows, primary_key, incremental)

        writer = self._writer_cache[table_name].writer
        for row in rows:
            writer.writerow(row)

    def _create_cached_writer(
        self,
        table_name: str,
        rows: list[dict],
        primary_key: list[str],
        incremental: bool,
    ) -> None:
        # Build union of all columns across the batch
        all_columns_set: set[str] = set()
        for row in rows:
            all_columns_set.update(row.keys())

        # Reorder columns based on preferred order, then add remaining sorted
        ordered_columns = [col for col in PREFERRED_COLUMNS_ORDER if col in all_columns_set]
        remaining_columns = sorted(all_columns_set - set(PREFERRED_COLUMNS_ORDER))
        all_columns = ordered_columns + remaining_columns

        logger.debug(f"Creating table definition for {table_name} with destination {self.bucket_id}.{table_name}")
        table_def = self.create_out_table_definition(
            f"{table_name}.csv",
            primary_key=primary_key,
            incremental=incremental,
            destination=f"{self.bucket_id}.{table_name}",
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

    def _retrieve_bucket_id(self) -> str:
        # This function replace default bucket option in Developer portal with custom implementation.
        # It allows set own bucket.
        if self.config.bucket_id:
            logger.info(f"Using bucket ID from configuration: {self.config.bucket_id}")
            return f"{self.config.bucket_id}"
        config_id = self.environment_variables.config_id
        component_id = self.environment_variables.component_id
        if not config_id:
            config_id = datetime.now().strftime("%Y%m%d%H%M%S")
        if not component_id:
            component_id = "keboola-ex-meta"
        logger.info(f"Using default bucket: in.c-{component_id.replace('.', '-')}-{config_id}")
        return f"in.c-{component_id.replace('.', '-')}-{config_id}"

    @sync_action("accounts")
    def run_accounts_action(self) -> list[dict[str, Any]]:
        return self.client.get_accounts("me/accounts", "id,business_name,name,category")

    @sync_action("adaccounts")
    def run_ad_accounts_action(self) -> list[dict[str, Any]]:
        return self.client.get_accounts("me/adaccounts", "account_id,id,business_name,name,currency")

    @sync_action("igaccounts")
    def run_ig_accounts_action(self) -> list[dict[str, Any]]:
        """
        Get Instagram Business Accounts linked to Facebook Pages.

        Returns only accounts with an instagram_business_account field,
        transforming the response to use the Instagram Business Account ID
        as the primary ID (matching V1 behavior).
        """
        raw_accounts = self.client.get_accounts("me/accounts", "instagram_business_account,name,category")

        # Transform to V1 format: filter and restructure
        result = []
        for account in raw_accounts:
            ig_account_data = account.get("instagram_business_account")
            if not ig_account_data:
                continue

            transformed = {
                "id": ig_account_data["id"],
                "fb_page_id": account["id"],
                "name": account.get("name"),
                "category": account.get("category"),
            }
            # Remove None values
            result.append({k: v for k, v in transformed.items() if v is not None})

        return result


"""
        Main entrypoint
"""


if __name__ == "__main__":
    try:
        comp = Component()
        comp.execute_action()
    except UserException as exc:
        logger.exception(exc)
        exit(1)
    except Exception as exc:
        logger.exception(exc)
        exit(2)
