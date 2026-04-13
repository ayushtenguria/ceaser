"""Feature flags — per-org feature gating for B2B SaaS.

Features are resolved in this order:
1. Per-org override in OrganizationPlan.features JSON  (highest priority)
2. Plan-level defaults from PLAN_FEATURES mapping
3. Global default (False)

Usage in endpoints:
    await check_feature("advanced_analytics", db, org_id)
    # raises 402 if feature is not enabled

Or check without raising:
    enabled = await has_feature("notebooks", db, org_id)
"""

from __future__ import annotations

import logging
from enum import Enum

from fastapi import HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import OrganizationPlan, User

logger = logging.getLogger(__name__)


class Feature(str, Enum):
    """All gatable features in the platform."""

    SQL_QUERIES = "sql_queries"
    FILE_UPLOAD = "file_upload"
    MULTI_DB = "multi_db"
    SNOWFLAKE = "snowflake"
    BIGQUERY = "bigquery"

    ADVANCED_ANALYTICS = "advanced_analytics"
    PYTHON_EXECUTION = "python_execution"
    VISUALIZATIONS = "visualizations"

    NOTEBOOKS = "notebooks"
    REPORTS = "reports"
    SCHEDULED_REPORTS = "scheduled_reports"
    SHARED_CONVERSATIONS = "shared_conversations"

    METRICS = "metrics"
    CUSTOM_PROMPTS = "custom_prompts"

    AUDIT_LOGS = "audit_logs"
    SSO = "sso"
    IP_ALLOWLIST = "ip_allowlist"
    DATA_MASKING = "data_masking"

    CLAUDE_MODEL = "claude_model"
    PRIORITY_QUEUE = "priority_queue"


_FREE = {
    Feature.SQL_QUERIES,
    Feature.FILE_UPLOAD,
    Feature.VISUALIZATIONS,
}

_STARTER = _FREE | {
    Feature.PYTHON_EXECUTION,
    Feature.REPORTS,
    Feature.METRICS,
    Feature.AUDIT_LOGS,
    Feature.CLAUDE_MODEL,
    Feature.SHARED_CONVERSATIONS,
}

_BUSINESS = _STARTER | {
    Feature.ADVANCED_ANALYTICS,
    Feature.NOTEBOOKS,
    Feature.SCHEDULED_REPORTS,
    Feature.MULTI_DB,
    Feature.SNOWFLAKE,
    Feature.BIGQUERY,
    Feature.CUSTOM_PROMPTS,
}

_ENTERPRISE = _BUSINESS | {
    Feature.SSO,
    Feature.IP_ALLOWLIST,
    Feature.DATA_MASKING,
    Feature.PRIORITY_QUEUE,
}

PLAN_FEATURES: dict[str, set[Feature]] = {
    "free": _FREE,
    "starter": _STARTER,
    "business": _BUSINESS,
    "enterprise": _ENTERPRISE,
}


async def _get_org_features(db: AsyncSession, org_id: str) -> tuple[str, dict | None]:
    """Return (plan_name, features_override) for an org."""
    if not org_id:
        return "free", None
    stmt = select(OrganizationPlan.plan_name, OrganizationPlan.features).where(
        OrganizationPlan.organization_id == org_id
    )
    result = await db.execute(stmt)
    row = result.first()
    if row is None:
        return "free", None
    return row[0], row[1]


async def has_feature(feature: str | Feature, db: AsyncSession, org_id: str) -> bool:
    """Check if a feature is enabled for the given org.

    Resolution order:
    1. org-level override (features JSON: {"notebooks": true/false})
    2. plan-level default
    3. super_admin → everything enabled
    """
    feat = Feature(feature) if isinstance(feature, str) else feature

    admin_stmt = select(User).where(
        User.organization_id == org_id,
        User.is_super_admin == True,  # noqa: E712
    )
    admin_result = await db.execute(admin_stmt)
    if admin_result.scalar_one_or_none() is not None:
        return True

    plan_name, overrides = await _get_org_features(db, org_id)

    if overrides and feat.value in overrides:
        return bool(overrides[feat.value])

    plan_feats = PLAN_FEATURES.get(plan_name, PLAN_FEATURES["free"])
    return feat in plan_feats


async def check_feature(feature: str | Feature, db: AsyncSession, org_id: str) -> None:
    """Raise 402 if feature is not enabled for the org."""
    if not await has_feature(feature, db, org_id):
        feat_name = feature if isinstance(feature, str) else feature.value
        raise HTTPException(
            status_code=status.HTTP_402_PAYMENT_REQUIRED,
            detail=f"Feature '{feat_name}' is not available on your plan. Please upgrade.",
        )


async def get_all_features(db: AsyncSession, org_id: str) -> dict[str, bool]:
    """Return all features with their enabled/disabled state for the org."""
    plan_name, overrides = await _get_org_features(db, org_id)

    admin_stmt = select(User).where(
        User.organization_id == org_id,
        User.is_super_admin == True,  # noqa: E712
    )
    admin_result = await db.execute(admin_stmt)
    is_admin = admin_result.scalar_one_or_none() is not None

    plan_feats = PLAN_FEATURES.get(plan_name, PLAN_FEATURES["free"])
    result = {}
    for feat in Feature:
        if is_admin:
            result[feat.value] = True
        elif overrides and feat.value in overrides:
            result[feat.value] = bool(overrides[feat.value])
        else:
            result[feat.value] = feat in plan_feats

    return result
