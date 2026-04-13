"""Payment service — factory + plan upgrade logic."""

from __future__ import annotations

import logging

from app.core.config import get_settings
from app.payments.base import IPaymentProvider, PaymentProvider

logger = logging.getLogger(__name__)

_provider_instance: IPaymentProvider | None = None


PLAN_LIMITS = {
    "free": {"max_queries_per_day": 50, "max_connections": 1, "max_reports": 5, "max_seats": 5},
    "starter": {
        "max_queries_per_day": 100,
        "max_connections": 3,
        "max_reports": 30,
        "max_seats": 3,
    },
    "business": {
        "max_queries_per_day": 500,
        "max_connections": 10,
        "max_reports": -1,
        "max_seats": 10,
    },
    "enterprise": {
        "max_queries_per_day": -1,
        "max_connections": -1,
        "max_reports": -1,
        "max_seats": -1,
    },
}


def _parse_plan_map(raw: str) -> dict[str, str]:
    """Parse 'starter:price_xxx,business:price_yyy' into dict."""
    if not raw:
        return {}
    result = {}
    for pair in raw.split(","):
        pair = pair.strip()
        if ":" in pair:
            plan, pid = pair.split(":", 1)
            result[plan.strip()] = pid.strip()
    return result


def get_payment_provider() -> IPaymentProvider | None:
    """Factory — returns the configured payment provider singleton, or None if not configured."""
    global _provider_instance
    if _provider_instance is not None:
        return _provider_instance

    settings = get_settings()
    provider_name = getattr(settings, "payment_provider", "").lower()

    if not provider_name:
        logger.info("No payment provider configured (PAYMENT_PROVIDER env var not set)")
        return None

    if provider_name == PaymentProvider.STRIPE:
        from app.payments.providers.stripe_provider import StripeProvider

        price_map = _parse_plan_map(getattr(settings, "stripe_price_map", ""))
        _provider_instance = StripeProvider(
            secret_key=settings.stripe_secret_key,
            webhook_secret=settings.stripe_webhook_secret,
            price_map=price_map,
        )

    elif provider_name == PaymentProvider.RAZORPAY:
        from app.payments.providers.razorpay_provider import RazorpayProvider

        plan_map = _parse_plan_map(getattr(settings, "razorpay_plan_map", ""))
        _provider_instance = RazorpayProvider(
            key_id=settings.razorpay_key_id,
            key_secret=settings.razorpay_key_secret,
            webhook_secret=settings.razorpay_webhook_secret,
            plan_map=plan_map,
        )

    elif provider_name == PaymentProvider.CASHFREE:
        from app.payments.providers.cashfree_provider import CashfreeProvider

        plan_map = _parse_plan_map(getattr(settings, "cashfree_plan_map", ""))
        _provider_instance = CashfreeProvider(
            app_id=settings.cashfree_app_id,
            secret_key=settings.cashfree_secret_key,
            webhook_secret=settings.cashfree_webhook_secret,
            plan_map=plan_map,
            sandbox=getattr(settings, "cashfree_sandbox", False),
        )

    else:
        raise ValueError(
            f"Unknown payment provider: {provider_name}. Supported: stripe, razorpay, cashfree"
        )

    logger.info("Payment provider initialized: %s", provider_name)
    return _provider_instance
