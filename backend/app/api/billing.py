"""Billing endpoints — checkout, webhooks, subscription management."""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Request
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from app.core.deps import CurrentUser, DbSession
from app.core.permissions import get_user_with_role
from app.db.models import OrganizationPlan, Payment, Subscription
from app.payments.base import PaymentEvent
from app.payments.service import PLAN_LIMITS, get_payment_provider

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/billing", tags=["billing"])


@router.post("/checkout")
async def create_checkout(
    request: dict,
    current_user: CurrentUser,
    db: DbSession,
) -> dict:
    """Create a checkout session for plan upgrade. Returns checkout URL."""
    provider = get_payment_provider()
    if not provider:
        raise HTTPException(status_code=501, detail="Payment not configured. Contact support.")

    plan_name = request.get("planName")
    if plan_name not in ("starter", "business"):
        raise HTTPException(status_code=400, detail="Invalid plan. Choose 'starter' or 'business'.")

    user = await get_user_with_role(db, current_user.user_id)
    org_id = user.organization_id or ""
    if not org_id:
        raise HTTPException(
            status_code=400, detail="No organization found. Please set up your org first."
        )

    success_url = request.get("successUrl", "")
    cancel_url = request.get("cancelUrl", "")

    result = await provider.create_checkout(
        org_id=org_id,
        plan_name=plan_name,
        customer_email=user.email,
        success_url=success_url,
        cancel_url=cancel_url,
    )

    sub = Subscription(
        organization_id=org_id,
        provider=result.provider,
        provider_subscription_id=result.session_id,
        plan_name=plan_name,
        status="incomplete",
    )
    db.add(sub)
    await db.flush()

    return {"checkoutUrl": result.checkout_url, "sessionId": result.session_id}


@router.post("/webhooks/{provider_name}")
async def handle_webhook(provider_name: str, request: Request, db: DbSession) -> dict:
    """Handle payment provider webhooks. Verifies signature and updates plan."""
    provider = get_payment_provider()
    if not provider:
        raise HTTPException(status_code=501, detail="Payment not configured")

    from app.core.config import get_settings

    settings = get_settings()
    configured_provider = settings.payment_provider.lower()
    if provider_name.lower() != configured_provider:
        logger.warning(
            "Webhook received for %s but configured provider is %s",
            provider_name,
            configured_provider,
        )
        raise HTTPException(status_code=404, detail="Unknown provider")

    payload = await request.body()
    headers = dict(request.headers)

    try:
        event: PaymentEvent = await provider.verify_webhook(payload=payload, headers=headers)
    except ValueError as e:
        logger.warning("Webhook verification failed for %s: %s", provider_name, e)
        raise HTTPException(status_code=400, detail="Invalid webhook signature")

    logger.info(
        "Webhook event: type=%s provider=%s sub_id=%s",
        event.event_type,
        event.provider,
        event.provider_subscription_id,
    )

    if event.event_type == "payment.success":
        await _handle_payment_success(db, event)
    elif event.event_type == "subscription.cancelled":
        await _handle_subscription_cancelled(db, event)
    elif event.event_type == "subscription.updated":
        await _handle_subscription_updated(db, event)

    return {"status": "ok"}


async def _handle_payment_success(db: DbSession, event: PaymentEvent) -> None:
    """Upgrade org plan on successful payment."""
    org_id = (event.metadata or {}).get("org_id", "")
    plan_name = event.plan_name

    if not org_id or not plan_name:
        if event.provider_subscription_id:
            stmt = select(Subscription).where(
                Subscription.provider_subscription_id == event.provider_subscription_id
            )
            result = await db.execute(stmt)
            sub = result.scalar_one_or_none()
            if sub:
                org_id = org_id or sub.organization_id
                plan_name = plan_name or sub.plan_name

    if not org_id or not plan_name:
        logger.error("Cannot process payment: missing org_id or plan_name")
        return

    if plan_name not in PLAN_LIMITS:
        logger.error("Unknown plan name in webhook: %s", plan_name)
        return

    if event.provider_subscription_id:
        stmt = select(Subscription).where(
            Subscription.provider_subscription_id == event.provider_subscription_id
        )
        result = await db.execute(stmt)
        sub = result.scalar_one_or_none()
        if sub:
            sub.status = "active"
            sub.plan_name = plan_name
        else:
            sub = Subscription(
                organization_id=org_id,
                provider=event.provider,
                provider_subscription_id=event.provider_subscription_id,
                plan_name=plan_name,
                status="active",
            )
            db.add(sub)

    if event.provider_payment_id:
        try:
            existing = await db.execute(
                select(Payment).where(Payment.provider_payment_id == event.provider_payment_id)
            )
            if not existing.scalar_one_or_none():
                payment = Payment(
                    organization_id=org_id,
                    provider=event.provider,
                    provider_payment_id=event.provider_payment_id,
                    amount=event.amount or 0,
                    currency=event.currency or "usd",
                    status="success",
                    plan_name=plan_name,
                )
                db.add(payment)
                await db.flush()
        except IntegrityError:
            await db.rollback()
            logger.info("Duplicate payment ignored: %s", event.provider_payment_id)
            return

    await _upgrade_org_plan(db, org_id, plan_name)
    logger.info("Plan upgraded: org=%s plan=%s", org_id, plan_name)


async def _handle_subscription_cancelled(db: DbSession, event: PaymentEvent) -> None:
    """Downgrade org to free plan on cancellation."""
    if not event.provider_subscription_id:
        return

    stmt = select(Subscription).where(
        Subscription.provider_subscription_id == event.provider_subscription_id
    )
    result = await db.execute(stmt)
    sub = result.scalar_one_or_none()

    if sub:
        sub.status = "cancelled"
        await _upgrade_org_plan(db, sub.organization_id, "free")
        logger.info("Subscription cancelled, downgraded to free: org=%s", sub.organization_id)


async def _handle_subscription_updated(db: DbSession, event: PaymentEvent) -> None:
    """Update subscription status."""
    if not event.provider_subscription_id:
        return

    stmt = select(Subscription).where(
        Subscription.provider_subscription_id == event.provider_subscription_id
    )
    result = await db.execute(stmt)
    sub = result.scalar_one_or_none()

    if sub and event.plan_name and event.plan_name in PLAN_LIMITS:
        sub.plan_name = event.plan_name
        await _upgrade_org_plan(db, sub.organization_id, event.plan_name)


async def _upgrade_org_plan(db: DbSession, org_id: str, plan_name: str) -> None:
    """Update OrganizationPlan with new limits."""
    limits = PLAN_LIMITS.get(plan_name, PLAN_LIMITS["free"])

    stmt = select(OrganizationPlan).where(OrganizationPlan.organization_id == org_id)
    result = await db.execute(stmt)
    plan = result.scalar_one_or_none()

    if plan:
        plan.plan_name = plan_name
        plan.max_queries_per_day = limits["max_queries_per_day"]
        plan.max_connections = limits["max_connections"]
        plan.max_reports = limits["max_reports"]
        plan.max_seats = limits["max_seats"]
    else:
        plan = OrganizationPlan(
            organization_id=org_id,
            plan_name=plan_name,
            max_queries_per_day=limits["max_queries_per_day"],
            max_connections=limits["max_connections"],
            max_reports=limits["max_reports"],
            max_seats=limits["max_seats"],
        )
        db.add(plan)

    await db.flush()


@router.get("/subscription")
async def get_subscription(current_user: CurrentUser, db: DbSession) -> dict:
    """Get current subscription status."""
    user = await get_user_with_role(db, current_user.user_id)
    org_id = user.organization_id or ""

    stmt = select(Subscription).where(
        Subscription.organization_id == org_id,
        Subscription.status.in_(["active", "past_due", "trialing"]),
    )
    result = await db.execute(stmt)
    sub = result.scalar_one_or_none()

    if not sub:
        return {
            "hasSubscription": False,
            "planName": "free",
            "status": None,
            "provider": None,
            "cancelAtPeriodEnd": False,
        }

    return {
        "hasSubscription": True,
        "planName": sub.plan_name,
        "status": sub.status,
        "provider": sub.provider,
        "cancelAtPeriodEnd": sub.cancel_at_period_end,
        "currentPeriodEnd": sub.current_period_end.isoformat() if sub.current_period_end else None,
    }


@router.post("/cancel")
async def cancel_subscription(current_user: CurrentUser, db: DbSession) -> dict:
    """Cancel the current subscription (at period end)."""
    provider = get_payment_provider()
    if not provider:
        raise HTTPException(status_code=501, detail="Payment not configured")

    user = await get_user_with_role(db, current_user.user_id)
    org_id = user.organization_id or ""

    stmt = select(Subscription).where(
        Subscription.organization_id == org_id,
        Subscription.status == "active",
    )
    result = await db.execute(stmt)
    sub = result.scalar_one_or_none()

    if not sub:
        raise HTTPException(status_code=404, detail="No active subscription found")

    success = await provider.cancel_subscription(sub.provider_subscription_id)
    if not success:
        raise HTTPException(status_code=500, detail="Failed to cancel subscription with provider")

    sub.cancel_at_period_end = True
    await db.flush()

    return {"status": "cancelling", "cancelAtPeriodEnd": True}


@router.get("/invoices")
async def get_invoices(current_user: CurrentUser, db: DbSession) -> list:
    """Get payment history for the organization."""
    user = await get_user_with_role(db, current_user.user_id)
    org_id = user.organization_id or ""

    stmt = (
        select(Payment)
        .where(Payment.organization_id == org_id)
        .order_by(Payment.created_at.desc())
        .limit(50)
    )
    result = await db.execute(stmt)
    payments = result.scalars().all()

    return [
        {
            "id": str(p.id),
            "amount": p.amount,
            "currency": p.currency,
            "status": p.status,
            "planName": p.plan_name,
            "provider": p.provider,
            "createdAt": p.created_at.isoformat(),
        }
        for p in payments
    ]
