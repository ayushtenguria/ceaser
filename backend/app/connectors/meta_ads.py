"""Meta (Facebook/Instagram) Ads connector via Marketing API v21.0.

Translates SQL-like queries into Meta Marketing API calls and returns
results in the standard (columns, rows) format.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx

from app.connectors.ads_query_parser import ParsedQuery, parse_ads_query
from app.connectors.base import BaseConnector
from app.db.models import DatabaseConnection
from app.services.oauth import (
    get_access_token,
    is_token_expired,
    meta_refresh_token,
    store_tokens,
)

logger = logging.getLogger(__name__)

META_GRAPH_URL = "https://graph.facebook.com/v21.0"

# Virtual table definitions — exposed to the LLM as the schema
SCHEMA_TABLES = {
    "campaigns": {
        "columns": [
            {"name": "campaign_id", "data_type": "VARCHAR", "nullable": False},
            {"name": "campaign_name", "data_type": "VARCHAR", "nullable": False},
            {"name": "status", "data_type": "VARCHAR", "nullable": False},
            {"name": "objective", "data_type": "VARCHAR", "nullable": True},
            {"name": "daily_budget", "data_type": "DECIMAL", "nullable": True},
            {"name": "lifetime_budget", "data_type": "DECIMAL", "nullable": True},
            {"name": "buying_type", "data_type": "VARCHAR", "nullable": True},
            {"name": "created_time", "data_type": "TIMESTAMP", "nullable": False},
            {"name": "start_time", "data_type": "TIMESTAMP", "nullable": True},
            {"name": "stop_time", "data_type": "TIMESTAMP", "nullable": True},
        ],
    },
    "ad_sets": {
        "columns": [
            {"name": "ad_set_id", "data_type": "VARCHAR", "nullable": False},
            {"name": "ad_set_name", "data_type": "VARCHAR", "nullable": False},
            {"name": "campaign_id", "data_type": "VARCHAR", "nullable": False},
            {"name": "status", "data_type": "VARCHAR", "nullable": False},
            {"name": "daily_budget", "data_type": "DECIMAL", "nullable": True},
            {"name": "bid_strategy", "data_type": "VARCHAR", "nullable": True},
            {"name": "targeting", "data_type": "JSON", "nullable": True},
            {"name": "optimization_goal", "data_type": "VARCHAR", "nullable": True},
        ],
    },
    "ads": {
        "columns": [
            {"name": "ad_id", "data_type": "VARCHAR", "nullable": False},
            {"name": "ad_name", "data_type": "VARCHAR", "nullable": False},
            {"name": "ad_set_id", "data_type": "VARCHAR", "nullable": False},
            {"name": "campaign_id", "data_type": "VARCHAR", "nullable": False},
            {"name": "status", "data_type": "VARCHAR", "nullable": False},
            {"name": "creative_id", "data_type": "VARCHAR", "nullable": True},
        ],
    },
    "ad_insights": {
        "columns": [
            {"name": "date", "data_type": "DATE", "nullable": False},
            {"name": "campaign_id", "data_type": "VARCHAR", "nullable": False},
            {"name": "campaign_name", "data_type": "VARCHAR", "nullable": False},
            {"name": "ad_set_id", "data_type": "VARCHAR", "nullable": True},
            {"name": "ad_set_name", "data_type": "VARCHAR", "nullable": True},
            {"name": "ad_id", "data_type": "VARCHAR", "nullable": True},
            {"name": "ad_name", "data_type": "VARCHAR", "nullable": True},
            {"name": "impressions", "data_type": "INTEGER", "nullable": False},
            {"name": "clicks", "data_type": "INTEGER", "nullable": False},
            {"name": "spend", "data_type": "DECIMAL", "nullable": False},
            {"name": "cpc", "data_type": "DECIMAL", "nullable": True},
            {"name": "cpm", "data_type": "DECIMAL", "nullable": True},
            {"name": "ctr", "data_type": "DECIMAL", "nullable": True},
            {"name": "reach", "data_type": "INTEGER", "nullable": True},
            {"name": "frequency", "data_type": "DECIMAL", "nullable": True},
            {"name": "conversions", "data_type": "INTEGER", "nullable": True},
            {"name": "cost_per_conversion", "data_type": "DECIMAL", "nullable": True},
            {"name": "conversion_value", "data_type": "DECIMAL", "nullable": True},
            {"name": "roas", "data_type": "DECIMAL", "nullable": True},
        ],
    },
    "demographics": {
        "columns": [
            {"name": "date", "data_type": "DATE", "nullable": False},
            {"name": "campaign_name", "data_type": "VARCHAR", "nullable": False},
            {"name": "age", "data_type": "VARCHAR", "nullable": False},
            {"name": "gender", "data_type": "VARCHAR", "nullable": False},
            {"name": "impressions", "data_type": "INTEGER", "nullable": False},
            {"name": "clicks", "data_type": "INTEGER", "nullable": False},
            {"name": "spend", "data_type": "DECIMAL", "nullable": False},
            {"name": "conversions", "data_type": "INTEGER", "nullable": True},
        ],
    },
    "placements": {
        "columns": [
            {"name": "date", "data_type": "DATE", "nullable": False},
            {"name": "campaign_name", "data_type": "VARCHAR", "nullable": False},
            {"name": "publisher_platform", "data_type": "VARCHAR", "nullable": False},
            {"name": "platform_position", "data_type": "VARCHAR", "nullable": False},
            {"name": "impressions", "data_type": "INTEGER", "nullable": False},
            {"name": "clicks", "data_type": "INTEGER", "nullable": False},
            {"name": "spend", "data_type": "DECIMAL", "nullable": False},
        ],
    },
    "geographic": {
        "columns": [
            {"name": "date", "data_type": "DATE", "nullable": False},
            {"name": "campaign_name", "data_type": "VARCHAR", "nullable": False},
            {"name": "country", "data_type": "VARCHAR", "nullable": False},
            {"name": "region", "data_type": "VARCHAR", "nullable": True},
            {"name": "impressions", "data_type": "INTEGER", "nullable": False},
            {"name": "clicks", "data_type": "INTEGER", "nullable": False},
            {"name": "spend", "data_type": "DECIMAL", "nullable": False},
        ],
    },
}

# Map virtual tables to Meta API levels and breakdowns
TABLE_API_MAP = {
    "campaigns": {"level": "campaign", "endpoint": "campaigns"},
    "ad_sets": {"level": "adset", "endpoint": "adsets"},
    "ads": {"level": "ad", "endpoint": "ads"},
    "ad_insights": {"level": "campaign", "endpoint": "insights"},
    "demographics": {"level": "campaign", "endpoint": "insights", "breakdowns": "age,gender"},
    "placements": {
        "level": "campaign",
        "endpoint": "insights",
        "breakdowns": "publisher_platform,platform_position",
    },
    "geographic": {"level": "campaign", "endpoint": "insights", "breakdowns": "country,region"},
}


class MetaAdsConnector(BaseConnector):
    """Connector for Meta Marketing API."""

    def __init__(self, connection: DatabaseConnection) -> None:
        super().__init__(connection)
        self._access_token: str = ""
        self._account_id: str = ""

    async def connect(self) -> bool:
        self._access_token = get_access_token(self._connection)
        self._account_id = self._connection.database or ""

        if is_token_expired(self._connection):
            try:
                new_data = await meta_refresh_token(self._access_token)
                self._access_token = new_data["access_token"]
                store_tokens(
                    self._connection,
                    access_token=self._access_token,
                    expires_in=new_data.get("expires_in", 5184000),
                )
            except Exception as exc:
                logger.warning("Meta token refresh failed: %s", exc)
                raise ConnectionError(f"Meta token expired and refresh failed: {exc}") from exc

        # Verify token works
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                f"{META_GRAPH_URL}/me",
                params={"access_token": self._access_token, "fields": "id,name"},
            )
            resp.raise_for_status()

        return True

    async def get_schema(self) -> dict[str, Any]:
        tables = {}
        for table_name, table_def in SCHEMA_TABLES.items():
            tables[table_name] = {
                "columns": table_def["columns"],
                "row_count": None,
            }
        return {"tables": tables}

    async def _execute_query_impl(self, query: str) -> tuple[list[str], list[dict[str, Any]]]:
        parsed = parse_ads_query(query)
        table = parsed.table

        if table not in TABLE_API_MAP:
            raise ValueError(f"Unknown table: {table}. Available: {', '.join(TABLE_API_MAP)}")

        api_config = TABLE_API_MAP[table]

        if api_config["endpoint"] == "insights":
            return await self._fetch_insights(parsed, api_config)
        else:
            return await self._fetch_objects(parsed, api_config)

    async def _fetch_insights(
        self, parsed: ParsedQuery, api_config: dict
    ) -> tuple[list[str], list[dict[str, Any]]]:
        params: dict[str, Any] = {
            "access_token": self._access_token,
            "level": api_config.get("level", "campaign"),
        }

        # Map requested columns to API fields
        metric_fields = []
        dimension_fields = []
        for col in parsed.columns:
            col_clean = col.lower().replace("sum_", "").replace("avg_", "").replace("count_", "")
            if col_clean in (
                "campaign_name",
                "campaign_id",
                "ad_set_name",
                "ad_set_id",
                "ad_name",
                "ad_id",
            ):
                dimension_fields.append(col_clean)
            elif col_clean == "date":
                continue
            else:
                metric_fields.append(col_clean)

        all_fields = list(set(dimension_fields + metric_fields))
        if not all_fields:
            all_fields = ["impressions", "clicks", "spend", "cpc", "ctr", "conversions"]
        params["fields"] = ",".join(all_fields)

        # Date range
        if parsed.date_start or parsed.date_end:
            time_range = {}
            if parsed.date_start:
                time_range["since"] = parsed.date_start
            if parsed.date_end:
                time_range["until"] = parsed.date_end
            else:
                time_range["until"] = datetime.now(UTC).strftime("%Y-%m-%d")
            params["time_range"] = str(time_range).replace("'", '"')
        else:
            # Default: last 30 days
            end = datetime.now(UTC)
            start = end - timedelta(days=30)
            params["time_range"] = (
                f'{{"since":"{start.strftime("%Y-%m-%d")}","until":"{end.strftime("%Y-%m-%d")}"}}'
            )

        # Breakdowns
        if "breakdowns" in api_config:
            params["breakdowns"] = api_config["breakdowns"]

        # Time increment for daily data
        if "date" in parsed.columns or parsed.group_by:
            params["time_increment"] = 1

        if parsed.limit:
            params["limit"] = parsed.limit

        url = f"{META_GRAPH_URL}/{self._account_id}/insights"
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()

        rows = []
        for item in data.get("data", []):
            row: dict[str, Any] = {}
            if "date_start" in item:
                row["date"] = item["date_start"]
            for field_name in all_fields:
                row[field_name] = item.get(field_name)
            # Add breakdown values
            for key in (
                "age",
                "gender",
                "country",
                "region",
                "publisher_platform",
                "platform_position",
            ):
                if key in item:
                    row[key] = item[key]
            rows.append(row)

        columns = list(rows[0].keys()) if rows else parsed.columns
        return columns, rows

    async def _fetch_objects(
        self, parsed: ParsedQuery, api_config: dict
    ) -> tuple[list[str], list[dict[str, Any]]]:
        endpoint = api_config["endpoint"]
        fields = ",".join(parsed.columns) if parsed.columns else "id,name,status"

        params: dict[str, Any] = {
            "access_token": self._access_token,
            "fields": fields,
            "limit": parsed.limit or 100,
        }

        # Apply status filter
        if "status" in parsed.filters:
            status_val = parsed.filters["status"]
            if isinstance(status_val, list):
                params["filtering"] = str(
                    [{"field": "effective_status", "operator": "IN", "value": status_val}]
                )
            else:
                params["filtering"] = str(
                    [{"field": "effective_status", "operator": "EQUAL", "value": status_val}]
                )

        url = f"{META_GRAPH_URL}/{self._account_id}/{endpoint}"
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()

        rows = data.get("data", [])
        columns = list(rows[0].keys()) if rows else parsed.columns
        return columns, rows

    async def disconnect(self) -> None:
        pass

    def get_connection_string(self) -> str:
        return f"meta_ads://{self._account_id}"
