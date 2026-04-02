"""Stripe payment provider implementation."""

from __future__ import annotations

import logging

import stripe

from app.payments.base import (
    CheckoutResult,
    IPaymentProvider,
    PaymentEvent,
    SubscriptionInfo,
)

logger = logging.getLogger(__name__)


PLAN_PRICE_MAP: dict[str, str] = {}


class StripeProvider(IPaymentProvider):
    """Stripe Checkout + Subscriptions."""

    def __init__(self, secret_key: str, webhook_secret: str, price_map: dict[str, str]):
        stripe.api_key = secret_key
        self._webhook_secret = webhook_secret
        global PLAN_PRICE_MAP
        PLAN_PRICE_MAP = price_map

    async def create_checkout(
        self,
        *,
        org_id: str,
        plan_name: str,
        customer_email: str,
        success_url: str,
        cancel_url: str,
    ) -> CheckoutResult:
        price_id = PLAN_PRICE_MAP.get(plan_name)
        if not price_id:
            raise ValueError(f"No Stripe price configured for plan: {plan_name}")

        session = stripe.checkout.Session.create(
            mode="subscription",
            payment_method_types=["card"],
            customer_email=customer_email,
            line_items=[{"price": price_id, "quantity": 1}],
            success_url=success_url,
            cancel_url=cancel_url,
            metadata={"org_id": org_id, "plan_name": plan_name},
        )

        return CheckoutResult(
            checkout_url=session.url,
            session_id=session.id,
            provider="stripe",
        )

    async def verify_webhook(self, *, payload: bytes, headers: dict) -> PaymentEvent:
        sig = headers.get("stripe-signature", "")
        try:
            event = stripe.Webhook.construct_event(payload, sig, self._webhook_secret)
        except stripe.error.SignatureVerificationError:
            raise ValueError("Invalid Stripe webhook signature")

        event_type = event["type"]
        obj = event["data"]["object"]

        if event_type == "checkout.session.completed":
            metadata = obj.get("metadata", {})
            return PaymentEvent(
                event_type="payment.success",
                provider="stripe",
                provider_subscription_id=obj.get("subscription"),
                provider_payment_id=obj.get("payment_intent"),
                plan_name=metadata.get("plan_name"),
                amount=obj.get("amount_total"),
                currency=obj.get("currency"),
                metadata=metadata,
            )
        elif event_type in ("customer.subscription.updated", "customer.subscription.deleted"):
            plan_name = None
            items = obj.get("items", {}).get("data", [])
            if items:
                price_id = items[0].get("price", {}).get("id", "")
                for pn, pid in PLAN_PRICE_MAP.items():
                    if pid == price_id:
                        plan_name = pn
                        break

            normalized_type = (
                "subscription.cancelled" if obj.get("status") == "canceled"
                else "subscription.updated"
            )
            return PaymentEvent(
                event_type=normalized_type,
                provider="stripe",
                provider_subscription_id=obj.get("id"),
                provider_payment_id=None,
                plan_name=plan_name,
                amount=None,
                currency=None,
                metadata=obj.get("metadata"),
            )
        else:
            return PaymentEvent(
                event_type=event_type,
                provider="stripe",
                provider_subscription_id=None,
                provider_payment_id=None,
                plan_name=None,
                amount=None,
                currency=None,
                metadata=None,
            )

    async def cancel_subscription(self, subscription_id: str) -> bool:
        try:
            stripe.Subscription.modify(subscription_id, cancel_at_period_end=True)
            return True
        except Exception as e:
            logger.error("Stripe cancel failed: %s", e)
            return False

    async def get_subscription(self, subscription_id: str) -> SubscriptionInfo | None:
        try:
            sub = stripe.Subscription.retrieve(subscription_id)
            plan_name = None
            items = sub.get("items", {}).get("data", [])
            if items:
                price_id = items[0].get("price", {}).get("id", "")
                for pn, pid in PLAN_PRICE_MAP.items():
                    if pid == price_id:
                        plan_name = pn
                        break

            return SubscriptionInfo(
                provider_subscription_id=sub["id"],
                status=sub["status"],
                plan_name=plan_name or "unknown",
                current_period_end=sub.get("current_period_end"),
                cancel_at_period_end=sub.get("cancel_at_period_end", False),
            )
        except Exception as e:
            logger.error("Stripe get_subscription failed: %s", e)
            return None
