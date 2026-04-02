"""Razorpay payment provider implementation."""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
from datetime import datetime

import httpx

from app.payments.base import (
    CheckoutResult,
    IPaymentProvider,
    PaymentEvent,
    SubscriptionInfo,
)

logger = logging.getLogger(__name__)


class RazorpayProvider(IPaymentProvider):
    """Razorpay Subscriptions for Indian market."""

    BASE_URL = "https://api.razorpay.com/v1"

    def __init__(self, key_id: str, key_secret: str, webhook_secret: str, plan_map: dict[str, str]):
        self._key_id = key_id
        self._key_secret = key_secret
        self._webhook_secret = webhook_secret
        self._plan_map = plan_map

    def _auth(self) -> tuple[str, str]:
        return (self._key_id, self._key_secret)

    async def create_checkout(
        self,
        *,
        org_id: str,
        plan_name: str,
        customer_email: str,
        success_url: str,
        cancel_url: str,
    ) -> CheckoutResult:
        rz_plan_id = self._plan_map.get(plan_name)
        if not rz_plan_id:
            raise ValueError(f"No Razorpay plan configured for: {plan_name}")

        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{self.BASE_URL}/subscriptions",
                auth=self._auth(),
                json={
                    "plan_id": rz_plan_id,
                    "total_count": 12,
                    "notes": {"org_id": org_id, "plan_name": plan_name},
                    "customer_notify": 0,
                },
            )
            resp.raise_for_status()
            sub = resp.json()

        short_url = sub.get("short_url", "")
        return CheckoutResult(
            checkout_url=short_url or f"https://rzp.io/i/{sub['id']}",
            session_id=sub["id"],
            provider="razorpay",
        )

    async def verify_webhook(self, *, payload: bytes, headers: dict) -> PaymentEvent:
        sig = headers.get("x-razorpay-signature", "")
        expected = hmac.new(
            self._webhook_secret.encode(), payload, hashlib.sha256
        ).hexdigest()
        if not hmac.compare_digest(sig, expected):
            raise ValueError("Invalid Razorpay webhook signature")

        data = json.loads(payload)
        event_type = data.get("event", "")
        entity = data.get("payload", {}).get("subscription", {}).get("entity", {})
        payment_entity = data.get("payload", {}).get("payment", {}).get("entity", {})

        notes = entity.get("notes", {}) or payment_entity.get("notes", {})

        if event_type == "subscription.charged":
            return PaymentEvent(
                event_type="payment.success",
                provider="razorpay",
                provider_subscription_id=entity.get("id"),
                provider_payment_id=payment_entity.get("id"),
                plan_name=notes.get("plan_name"),
                amount=payment_entity.get("amount"),
                currency=payment_entity.get("currency"),
                metadata=notes,
            )
        elif event_type in ("subscription.cancelled", "subscription.halted"):
            return PaymentEvent(
                event_type="subscription.cancelled",
                provider="razorpay",
                provider_subscription_id=entity.get("id"),
                provider_payment_id=None,
                plan_name=notes.get("plan_name"),
                amount=None,
                currency=None,
                metadata=notes,
            )
        else:
            return PaymentEvent(
                event_type=event_type,
                provider="razorpay",
                provider_subscription_id=entity.get("id"),
                provider_payment_id=payment_entity.get("id"),
                plan_name=notes.get("plan_name"),
                amount=None,
                currency=None,
                metadata=notes,
            )

    async def cancel_subscription(self, subscription_id: str) -> bool:
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.post(
                    f"{self.BASE_URL}/subscriptions/{subscription_id}/cancel",
                    auth=self._auth(),
                    json={"cancel_at_cycle_end": 1},
                )
                resp.raise_for_status()
                return True
        except Exception as e:
            logger.error("Razorpay cancel failed: %s", e)
            return False

    async def get_subscription(self, subscription_id: str) -> SubscriptionInfo | None:
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(
                    f"{self.BASE_URL}/subscriptions/{subscription_id}",
                    auth=self._auth(),
                )
                resp.raise_for_status()
                sub = resp.json()

            status_map = {
                "active": "active",
                "pending": "incomplete",
                "halted": "past_due",
                "cancelled": "cancelled",
                "completed": "cancelled",
            }

            return SubscriptionInfo(
                provider_subscription_id=sub["id"],
                status=status_map.get(sub.get("status", ""), sub.get("status", "")),
                plan_name=sub.get("notes", {}).get("plan_name", "unknown"),
                current_period_end=sub.get("current_end"),
                cancel_at_period_end=sub.get("status") == "cancelled",
            )
        except Exception as e:
            logger.error("Razorpay get_subscription failed: %s", e)
            return None
