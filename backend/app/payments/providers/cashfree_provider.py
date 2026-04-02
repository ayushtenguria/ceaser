"""Cashfree payment provider implementation."""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import time

import httpx

from app.payments.base import (
    CheckoutResult,
    IPaymentProvider,
    PaymentEvent,
    SubscriptionInfo,
)

logger = logging.getLogger(__name__)


class CashfreeProvider(IPaymentProvider):
    """Cashfree Subscriptions for Indian market."""

    def __init__(self, app_id: str, secret_key: str, webhook_secret: str, plan_map: dict[str, str], *, sandbox: bool = False):
        self._app_id = app_id
        self._secret_key = secret_key
        self._webhook_secret = webhook_secret
        self._plan_map = plan_map
        self._base_url = (
            "https://sandbox.cashfree.com/pg" if sandbox
            else "https://api.cashfree.com/pg"
        )

    def _headers(self) -> dict:
        return {
            "x-client-id": self._app_id,
            "x-client-secret": self._secret_key,
            "x-api-version": "2023-08-01",
            "Content-Type": "application/json",
        }

    async def create_checkout(
        self,
        *,
        org_id: str,
        plan_name: str,
        customer_email: str,
        success_url: str,
        cancel_url: str,
    ) -> CheckoutResult:
        cf_plan_id = self._plan_map.get(plan_name)
        if not cf_plan_id:
            raise ValueError(f"No Cashfree plan configured for: {plan_name}")

        order_id = f"ceaser_{org_id}_{int(time.time())}"

        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{self._base_url}/subscriptions",
                headers=self._headers(),
                json={
                    "subscription_id": order_id,
                    "plan_id": cf_plan_id,
                    "customer_email": customer_email,
                    "return_url": success_url,
                    "subscription_note": json.dumps({"org_id": org_id, "plan_name": plan_name}),
                },
            )
            resp.raise_for_status()
            data = resp.json()

        return CheckoutResult(
            checkout_url=data.get("subscription_url", ""),
            session_id=data.get("subscription_id", order_id),
            provider="cashfree",
        )

    async def verify_webhook(self, *, payload: bytes, headers: dict) -> PaymentEvent:
        sig = headers.get("x-cashfree-signature", "")
        ts = headers.get("x-cashfree-timestamp", "")
        sign_data = ts.encode() + payload
        expected = hmac.new(
            self._webhook_secret.encode(), sign_data, hashlib.sha256
        ).hexdigest()

        if not hmac.compare_digest(sig, expected):
            raise ValueError("Invalid Cashfree webhook signature")

        data = json.loads(payload)
        event_type = data.get("type", "")
        sub_data = data.get("data", {}).get("subscription", {})
        payment_data = data.get("data", {}).get("payment", {})

        notes = {}
        note_str = sub_data.get("subscription_note", "")
        if note_str:
            try:
                notes = json.loads(note_str)
            except json.JSONDecodeError:
                pass

        if event_type == "SUBSCRIPTION_PAYMENT_SUCCESS":
            return PaymentEvent(
                event_type="payment.success",
                provider="cashfree",
                provider_subscription_id=sub_data.get("subscription_id"),
                provider_payment_id=payment_data.get("cf_payment_id"),
                plan_name=notes.get("plan_name"),
                amount=payment_data.get("payment_amount"),
                currency=payment_data.get("payment_currency"),
                metadata=notes,
            )
        elif event_type in ("SUBSCRIPTION_CANCELLED", "SUBSCRIPTION_EXPIRED"):
            return PaymentEvent(
                event_type="subscription.cancelled",
                provider="cashfree",
                provider_subscription_id=sub_data.get("subscription_id"),
                provider_payment_id=None,
                plan_name=notes.get("plan_name"),
                amount=None,
                currency=None,
                metadata=notes,
            )
        else:
            return PaymentEvent(
                event_type=event_type,
                provider="cashfree",
                provider_subscription_id=sub_data.get("subscription_id"),
                provider_payment_id=payment_data.get("cf_payment_id"),
                plan_name=notes.get("plan_name"),
                amount=None,
                currency=None,
                metadata=notes,
            )

    async def cancel_subscription(self, subscription_id: str) -> bool:
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.post(
                    f"{self._base_url}/subscriptions/{subscription_id}/cancel",
                    headers=self._headers(),
                )
                resp.raise_for_status()
                return True
        except Exception as e:
            logger.error("Cashfree cancel failed: %s", e)
            return False

    async def get_subscription(self, subscription_id: str) -> SubscriptionInfo | None:
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(
                    f"{self._base_url}/subscriptions/{subscription_id}",
                    headers=self._headers(),
                )
                resp.raise_for_status()
                data = resp.json()

            status_map = {
                "ACTIVE": "active",
                "ON_HOLD": "past_due",
                "CANCELLED": "cancelled",
                "COMPLETED": "cancelled",
                "INITIALIZED": "incomplete",
            }

            notes = {}
            note_str = data.get("subscription_note", "")
            if note_str:
                try:
                    notes = json.loads(note_str)
                except json.JSONDecodeError:
                    pass

            return SubscriptionInfo(
                provider_subscription_id=data.get("subscription_id", subscription_id),
                status=status_map.get(data.get("subscription_status", ""), "unknown"),
                plan_name=notes.get("plan_name", "unknown"),
                current_period_end=data.get("current_period_end"),
                cancel_at_period_end=data.get("subscription_status") == "CANCELLED",
            )
        except Exception as e:
            logger.error("Cashfree get_subscription failed: %s", e)
            return None
