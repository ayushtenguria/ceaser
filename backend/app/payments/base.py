"""Abstract base class for payment providers."""

from __future__ import annotations

import abc
from dataclasses import dataclass
from enum import Enum


class PaymentProvider(str, Enum):
    STRIPE = "stripe"
    RAZORPAY = "razorpay"
    CASHFREE = "cashfree"


class SubscriptionStatus(str, Enum):
    ACTIVE = "active"
    PAST_DUE = "past_due"
    CANCELLED = "cancelled"
    INCOMPLETE = "incomplete"
    TRIALING = "trialing"


@dataclass
class CheckoutResult:
    """Returned by create_checkout — contains the URL/session for redirect."""
    checkout_url: str
    session_id: str
    provider: str


@dataclass
class PaymentEvent:
    """Normalized webhook event from any provider."""
    event_type: str  # "payment.success", "subscription.updated", "subscription.cancelled"
    provider: str
    provider_subscription_id: str | None
    provider_payment_id: str | None
    plan_name: str | None
    amount: int | None  # cents
    currency: str | None
    metadata: dict | None


@dataclass
class SubscriptionInfo:
    """Current subscription state from a provider."""
    provider_subscription_id: str
    status: str
    plan_name: str
    current_period_end: str | None  # ISO datetime
    cancel_at_period_end: bool


class IPaymentProvider(abc.ABC):
    """Interface that every payment gateway must implement."""

    @abc.abstractmethod
    async def create_checkout(
        self,
        *,
        org_id: str,
        plan_name: str,
        customer_email: str,
        success_url: str,
        cancel_url: str,
    ) -> CheckoutResult:
        """Create a checkout session and return the redirect URL."""

    @abc.abstractmethod
    async def verify_webhook(self, *, payload: bytes, headers: dict) -> PaymentEvent:
        """Verify and parse a webhook payload. Raises ValueError if invalid."""

    @abc.abstractmethod
    async def cancel_subscription(self, subscription_id: str) -> bool:
        """Cancel a subscription. Returns True on success."""

    @abc.abstractmethod
    async def get_subscription(self, subscription_id: str) -> SubscriptionInfo | None:
        """Fetch current subscription state from the provider."""
