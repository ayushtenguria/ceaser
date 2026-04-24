"""Google Ads connector via Google Ads API v18.

Translates SQL-like queries into GAQL (Google Ads Query Language) and
returns results in the standard (columns, rows) format.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx

from app.connectors.ads_query_parser import ParsedQuery, parse_ads_query
from app.connectors.base import BaseConnector
from app.core.config import get_settings
from app.db.models import DatabaseConnection
from app.services.oauth import (
    get_access_token,
    get_refresh_token,
    google_refresh_access_token,
    is_token_expired,
    store_tokens,
)

logger = logging.getLogger(__name__)

GOOGLE_ADS_API = "https://googleads.googleapis.com/v18"

SCHEMA_TABLES = {
    "campaigns": {
        "columns": [
            {"name": "campaign_id", "data_type": "INTEGER", "nullable": False},
            {"name": "campaign_name", "data_type": "VARCHAR", "nullable": False},
            {"name": "status", "data_type": "VARCHAR", "nullable": False},
            {"name": "campaign_type", "data_type": "VARCHAR", "nullable": True},
            {"name": "bidding_strategy", "data_type": "VARCHAR", "nullable": True},
            {"name": "budget", "data_type": "DECIMAL", "nullable": True},
            {"name": "start_date", "data_type": "DATE", "nullable": True},
            {"name": "end_date", "data_type": "DATE", "nullable": True},
        ],
    },
    "ad_groups": {
        "columns": [
            {"name": "ad_group_id", "data_type": "INTEGER", "nullable": False},
            {"name": "ad_group_name", "data_type": "VARCHAR", "nullable": False},
            {"name": "campaign_id", "data_type": "INTEGER", "nullable": False},
            {"name": "status", "data_type": "VARCHAR", "nullable": False},
            {"name": "cpc_bid", "data_type": "DECIMAL", "nullable": True},
        ],
    },
    "ads": {
        "columns": [
            {"name": "ad_id", "data_type": "INTEGER", "nullable": False},
            {"name": "ad_name", "data_type": "VARCHAR", "nullable": True},
            {"name": "ad_group_id", "data_type": "INTEGER", "nullable": False},
            {"name": "ad_type", "data_type": "VARCHAR", "nullable": False},
            {"name": "status", "data_type": "VARCHAR", "nullable": False},
            {"name": "headlines", "data_type": "JSON", "nullable": True},
            {"name": "descriptions", "data_type": "JSON", "nullable": True},
            {"name": "final_urls", "data_type": "JSON", "nullable": True},
        ],
    },
    "ad_performance": {
        "columns": [
            {"name": "date", "data_type": "DATE", "nullable": False},
            {"name": "campaign_id", "data_type": "INTEGER", "nullable": False},
            {"name": "campaign_name", "data_type": "VARCHAR", "nullable": False},
            {"name": "ad_group_id", "data_type": "INTEGER", "nullable": True},
            {"name": "ad_group_name", "data_type": "VARCHAR", "nullable": True},
            {"name": "ad_id", "data_type": "INTEGER", "nullable": True},
            {"name": "impressions", "data_type": "INTEGER", "nullable": False},
            {"name": "clicks", "data_type": "INTEGER", "nullable": False},
            {"name": "cost", "data_type": "DECIMAL", "nullable": False},
            {"name": "conversions", "data_type": "DECIMAL", "nullable": True},
            {"name": "conversion_value", "data_type": "DECIMAL", "nullable": True},
            {"name": "ctr", "data_type": "DECIMAL", "nullable": True},
            {"name": "avg_cpc", "data_type": "DECIMAL", "nullable": True},
            {"name": "avg_cpm", "data_type": "DECIMAL", "nullable": True},
            {"name": "roas", "data_type": "DECIMAL", "nullable": True},
        ],
    },
    "keywords": {
        "columns": [
            {"name": "keyword_id", "data_type": "INTEGER", "nullable": False},
            {"name": "keyword_text", "data_type": "VARCHAR", "nullable": False},
            {"name": "match_type", "data_type": "VARCHAR", "nullable": False},
            {"name": "ad_group_id", "data_type": "INTEGER", "nullable": False},
            {"name": "status", "data_type": "VARCHAR", "nullable": False},
            {"name": "quality_score", "data_type": "INTEGER", "nullable": True},
            {"name": "cpc_bid", "data_type": "DECIMAL", "nullable": True},
        ],
    },
    "demographics": {
        "columns": [
            {"name": "date", "data_type": "DATE", "nullable": False},
            {"name": "campaign_name", "data_type": "VARCHAR", "nullable": False},
            {"name": "age_range", "data_type": "VARCHAR", "nullable": False},
            {"name": "gender", "data_type": "VARCHAR", "nullable": False},
            {"name": "impressions", "data_type": "INTEGER", "nullable": False},
            {"name": "clicks", "data_type": "INTEGER", "nullable": False},
            {"name": "cost", "data_type": "DECIMAL", "nullable": False},
            {"name": "conversions", "data_type": "DECIMAL", "nullable": True},
        ],
    },
    "geographic": {
        "columns": [
            {"name": "date", "data_type": "DATE", "nullable": False},
            {"name": "campaign_name", "data_type": "VARCHAR", "nullable": False},
            {"name": "country", "data_type": "VARCHAR", "nullable": False},
            {"name": "region", "data_type": "VARCHAR", "nullable": True},
            {"name": "city", "data_type": "VARCHAR", "nullable": True},
            {"name": "impressions", "data_type": "INTEGER", "nullable": False},
            {"name": "clicks", "data_type": "INTEGER", "nullable": False},
            {"name": "cost", "data_type": "DECIMAL", "nullable": False},
        ],
    },
    "devices": {
        "columns": [
            {"name": "date", "data_type": "DATE", "nullable": False},
            {"name": "campaign_name", "data_type": "VARCHAR", "nullable": False},
            {"name": "device", "data_type": "VARCHAR", "nullable": False},
            {"name": "impressions", "data_type": "INTEGER", "nullable": False},
            {"name": "clicks", "data_type": "INTEGER", "nullable": False},
            {"name": "cost", "data_type": "DECIMAL", "nullable": False},
            {"name": "conversions", "data_type": "DECIMAL", "nullable": True},
        ],
    },
}

# Map virtual tables to GAQL resources
TABLE_GAQL_MAP = {
    "campaigns": "campaign",
    "ad_groups": "ad_group",
    "ads": "ad_group_ad",
    "ad_performance": "campaign",
    "keywords": "keyword_view",
    "demographics": "gender_view",
    "geographic": "geographic_view",
    "devices": "campaign",
}

# Map column names to GAQL field paths
COLUMN_GAQL_MAP = {
    "campaign_id": "campaign.id",
    "campaign_name": "campaign.name",
    "campaign_type": "campaign.advertising_channel_type",
    "status": "campaign.status",
    "budget": "campaign_budget.amount_micros",
    "bidding_strategy": "campaign.bidding_strategy_type",
    "ad_group_id": "ad_group.id",
    "ad_group_name": "ad_group.name",
    "ad_id": "ad_group_ad.ad.id",
    "ad_name": "ad_group_ad.ad.name",
    "ad_type": "ad_group_ad.ad.type",
    "date": "segments.date",
    "impressions": "metrics.impressions",
    "clicks": "metrics.clicks",
    "cost": "metrics.cost_micros",
    "conversions": "metrics.conversions",
    "conversion_value": "metrics.conversions_value",
    "ctr": "metrics.ctr",
    "avg_cpc": "metrics.average_cpc",
    "avg_cpm": "metrics.average_cpm",
    "roas": "metrics.conversions_value / metrics.cost_micros",
    "keyword_text": "ad_group_criterion.keyword.text",
    "match_type": "ad_group_criterion.keyword.match_type",
    "quality_score": "ad_group_criterion.quality_info.quality_score",
    "age_range": "ad_group_criterion.age_range.type",
    "gender": "ad_group_criterion.gender.type",
    "device": "segments.device",
    "country": "geographic_view.country_criterion_id",
    "region": "geographic_view.state",
    "city": "geographic_view.city",
}


class GoogleAdsConnector(BaseConnector):
    """Connector for Google Ads API."""

    def __init__(self, connection: DatabaseConnection) -> None:
        super().__init__(connection)
        self._access_token: str = ""
        self._customer_id: str = ""
        self._developer_token: str = ""

    async def connect(self) -> bool:
        self._access_token = get_access_token(self._connection)
        self._customer_id = (self._connection.database or "").replace("-", "")
        self._developer_token = get_settings().google_ads_developer_token

        if is_token_expired(self._connection):
            refresh = get_refresh_token(self._connection)
            if not refresh:
                raise ConnectionError("Google token expired and no refresh token available")
            try:
                new_data = await google_refresh_access_token(refresh)
                self._access_token = new_data["access_token"]
                store_tokens(
                    self._connection,
                    access_token=self._access_token,
                    refresh_token=refresh,
                    expires_in=new_data.get("expires_in", 3600),
                )
            except Exception as exc:
                raise ConnectionError(f"Google token refresh failed: {exc}") from exc

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
        gaql = self._build_gaql(parsed)
        return await self._execute_gaql(gaql, parsed.columns)

    def _build_gaql(self, parsed: ParsedQuery) -> str:
        """Convert parsed SQL into GAQL (Google Ads Query Language)."""
        resource = TABLE_GAQL_MAP.get(parsed.table)
        if not resource:
            raise ValueError(
                f"Unknown table: {parsed.table}. Available: {', '.join(TABLE_GAQL_MAP)}"
            )

        # Map columns to GAQL field paths
        gaql_fields = []
        for col in parsed.columns:
            col_clean = col.lower().replace("sum_", "").replace("avg_", "").replace("count_", "")
            gaql_field = COLUMN_GAQL_MAP.get(col_clean, col_clean)
            if gaql_field not in gaql_fields:
                gaql_fields.append(gaql_field)

        if not gaql_fields:
            gaql_fields = [
                "campaign.name",
                "metrics.impressions",
                "metrics.clicks",
                "metrics.cost_micros",
            ]

        select_clause = ", ".join(gaql_fields)
        gaql = f"SELECT {select_clause} FROM {resource}"

        # WHERE clause
        conditions = []
        if parsed.date_start:
            conditions.append(f"segments.date >= '{parsed.date_start}'")
        if parsed.date_end:
            conditions.append(f"segments.date <= '{parsed.date_end}'")
        elif not parsed.date_start:
            # Default: last 30 days
            end = datetime.now(UTC)
            start = end - timedelta(days=30)
            conditions.append(f"segments.date >= '{start.strftime('%Y-%m-%d')}'")
            conditions.append(f"segments.date <= '{end.strftime('%Y-%m-%d')}'")

        for key, value in parsed.filters.items():
            gaql_key = COLUMN_GAQL_MAP.get(key, key)
            if isinstance(value, list):
                quoted = ", ".join(f"'{v}'" for v in value)
                conditions.append(f"{gaql_key} IN ({quoted})")
            elif isinstance(value, str):
                conditions.append(f"{gaql_key} = '{value}'")
            else:
                conditions.append(f"{gaql_key} = {value}")

        if conditions:
            gaql += " WHERE " + " AND ".join(conditions)

        if parsed.order_by:
            order_parts = []
            for col, direction in parsed.order_by:
                gaql_col = COLUMN_GAQL_MAP.get(col, col)
                order_parts.append(f"{gaql_col} {direction}")
            gaql += " ORDER BY " + ", ".join(order_parts)

        if parsed.limit:
            gaql += f" LIMIT {parsed.limit}"

        return gaql

    async def _execute_gaql(
        self, gaql: str, requested_columns: list[str]
    ) -> tuple[list[str], list[dict[str, Any]]]:
        """Execute a GAQL query and return (columns, rows)."""
        url = f"{GOOGLE_ADS_API}/customers/{self._customer_id}/googleAds:searchStream"

        headers = {
            "Authorization": f"Bearer {self._access_token}",
            "developer-token": self._developer_token,
            "Content-Type": "application/json",
        }

        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                url,
                headers=headers,
                json={"query": gaql},
            )
            resp.raise_for_status()
            data = resp.json()

        rows: list[dict[str, Any]] = []
        for batch in data:
            for result in batch.get("results", []):
                row = self._flatten_result(result, requested_columns)
                rows.append(row)

        columns = list(rows[0].keys()) if rows else requested_columns
        return columns, rows

    def _flatten_result(self, result: dict, requested_columns: list[str]) -> dict[str, Any]:
        """Flatten a nested Google Ads API result into a flat dict."""
        flat: dict[str, Any] = {}

        campaign = result.get("campaign", {})
        ad_group = result.get("adGroup", {})
        ad = result.get("adGroupAd", {}).get("ad", {})
        metrics = result.get("metrics", {})
        segments = result.get("segments", {})
        keyword = result.get("adGroupCriterion", {}).get("keyword", {})

        field_map = {
            "campaign_id": campaign.get("id"),
            "campaign_name": campaign.get("name"),
            "status": campaign.get("status"),
            "campaign_type": campaign.get("advertisingChannelType"),
            "ad_group_id": ad_group.get("id"),
            "ad_group_name": ad_group.get("name"),
            "ad_id": ad.get("id"),
            "ad_name": ad.get("name"),
            "date": segments.get("date"),
            "device": segments.get("device"),
            "impressions": int(metrics.get("impressions", 0)),
            "clicks": int(metrics.get("clicks", 0)),
            "cost": int(metrics.get("costMicros", 0)) / 1_000_000,
            "conversions": float(metrics.get("conversions", 0)),
            "conversion_value": float(metrics.get("conversionsValue", 0)),
            "ctr": float(metrics.get("ctr", 0)),
            "avg_cpc": int(metrics.get("averageCpc", 0)) / 1_000_000,
            "avg_cpm": int(metrics.get("averageCpm", 0)) / 1_000_000,
            "keyword_text": keyword.get("text"),
            "match_type": keyword.get("matchType"),
        }

        # Compute ROAS
        cost = field_map.get("cost", 0)
        conv_value = field_map.get("conversion_value", 0)
        field_map["roas"] = round(conv_value / cost, 2) if cost > 0 else 0

        for col in requested_columns:
            col_clean = col.lower().replace("sum_", "").replace("avg_", "").replace("count_", "")
            if col_clean in field_map:
                flat[col] = field_map[col_clean]
            else:
                flat[col] = None

        return flat

    async def disconnect(self) -> None:
        pass

    def get_connection_string(self) -> str:
        return f"google_ads://{self._customer_id}"
