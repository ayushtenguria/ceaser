"""Database-connection management endpoints."""

from __future__ import annotations

import logging
import uuid

from fastapi import APIRouter, HTTPException, status
from sqlalchemy import select

from app.api.schemas import ConnectionCreate, ConnectionResponse, ConnectionTestResult
from app.connectors.factory import get_connector
from app.core.deps import CurrentUser, DbSession
from app.core.permissions import Permission, require_permission
from app.db.models import DatabaseConnection, User
from app.services.encryption import encrypt_value
from app.services.schema import introspect_schema, format_schema_for_llm

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/connections", tags=["connections"])


async def _get_user(db: DbSession, clerk_id: str) -> User:
    """Fetch user by clerk_id, auto-creating in dev mode if needed."""
    stmt = select(User).where(User.clerk_id == clerk_id)
    result = await db.execute(stmt)
    user = result.scalar_one_or_none()
    if user is None:
        from app.core.config import get_settings
        if get_settings().dev_mode and clerk_id == "dev_user":
            user = User(
                clerk_id="dev_user",
                email=get_settings().dev_fallback_email,
                first_name="Dev",
                last_name="User",
                organization_id="dev_org",
            )
            db.add(user)
            await db.flush()
            await db.refresh(user)
            return user
        raise HTTPException(status_code=404, detail="User not found.")
    return user


@router.post("/", response_model=ConnectionResponse, status_code=status.HTTP_201_CREATED)
async def create_connection(
    body: ConnectionCreate,
    current_user: CurrentUser,
    db: DbSession,
) -> DatabaseConnection:
    """Register a new external database connection (password is encrypted at rest)."""
    user = await require_permission(Permission.MANAGE_CONNECTIONS, current_user, db)

    from app.core.plan_enforcement import check_connection_limit
    await check_connection_limit(db, user.organization_id or current_user.org_id or "")

    connection = DatabaseConnection(
        name=body.name,
        db_type=body.db_type,
        host=body.host,
        port=body.port,
        database=body.database,
        username=body.username,
        encrypted_password=encrypt_value(body.password),
        organization_id=user.organization_id or current_user.org_id or "",
        user_id=user.id,
    )
    db.add(connection)
    await db.flush()
    await db.refresh(connection)
    logger.info("Created connection '%s' (%s) for user %s", body.name, body.db_type, user.id)
    return connection


@router.get("/", response_model=list[ConnectionResponse])
async def list_connections(current_user: CurrentUser, db: DbSession) -> list[DatabaseConnection]:
    """List all database connections belonging to the current user's organisation."""
    user = await require_permission(Permission.VIEW_DATA, current_user, db)
    org_id = user.organization_id or current_user.org_id or ""
    stmt = (
        select(DatabaseConnection)
        .where(DatabaseConnection.organization_id == org_id)
        .order_by(DatabaseConnection.created_at.desc())
    )
    result = await db.execute(stmt)
    return list(result.scalars().all())


@router.post("/{connection_id}/test", response_model=ConnectionTestResult)
async def test_connection(
    connection_id: uuid.UUID,
    current_user: CurrentUser,
    db: DbSession,
) -> ConnectionTestResult:
    """Test an existing connection by connecting and introspecting its schema."""
    user = await require_permission(Permission.MANAGE_CONNECTIONS, current_user, db)
    connection = await _load_connection(db, connection_id, user.id)

    try:
        # Use the enriched introspection (includes sample values, FKs, row counts).
        schema = await introspect_schema(connection)
        schema_dict: dict = {
            "tables": [
                {
                    "name": t.name,
                    "row_count": t.row_count,
                    "columns": [
                        {
                            "name": c.name,
                            "data_type": c.data_type,
                            "nullable": c.nullable,
                            "primary_key": c.primary_key,
                            "foreign_key": c.foreign_key,
                            "sample_values": c.sample_values,
                        }
                        for c in t.columns
                    ],
                }
                for t in schema.tables
            ]
        }

        connection.is_connected = True
        connection.schema_cache = schema_dict
        await db.flush()

        return ConnectionTestResult(
            success=True,
            message="Connection successful.",
            schema_info=schema_dict,
        )
    except Exception as exc:
        connection.is_connected = False
        await db.flush()
        logger.warning("Connection test failed for %s: %s", connection_id, exc)
        return ConnectionTestResult(success=False, message=str(exc))


@router.post("/{connection_id}/schema", response_model=ConnectionTestResult)
async def refresh_schema(
    connection_id: uuid.UUID,
    current_user: CurrentUser,
    db: DbSession,
) -> ConnectionTestResult:
    """Re-introspect the schema for an existing connection and update the cache."""
    user = await require_permission(Permission.MANAGE_CONNECTIONS, current_user, db)
    connection = await _load_connection(db, connection_id, user.id)

    try:
        schema = await introspect_schema(connection)
        # Store the raw dict representation.
        schema_dict: dict = {
            "tables": [
                {
                    "name": t.name,
                    "row_count": t.row_count,
                    "columns": [
                        {
                            "name": c.name,
                            "data_type": c.data_type,
                            "nullable": c.nullable,
                            "primary_key": c.primary_key,
                            "foreign_key": c.foreign_key,
                            "sample_values": c.sample_values,
                        }
                        for c in t.columns
                    ],
                }
                for t in schema.tables
            ]
        }
        connection.schema_cache = schema_dict
        connection.is_connected = True
        await db.flush()

        return ConnectionTestResult(
            success=True,
            message="Schema refreshed.",
            schema_info=schema_dict,
        )
    except Exception as exc:
        logger.warning("Schema refresh failed: %s", exc)
        return ConnectionTestResult(success=False, message=str(exc))


@router.delete("/{connection_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_connection(
    connection_id: uuid.UUID,
    current_user: CurrentUser,
    db: DbSession,
) -> None:
    """Delete a database connection."""
    user = await require_permission(Permission.MANAGE_CONNECTIONS, current_user, db)
    connection = await _load_connection(db, connection_id, user.id)
    await db.delete(connection)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

async def _load_connection(
    db: DbSession,
    connection_id: uuid.UUID,
    user_id: uuid.UUID,
) -> DatabaseConnection:
    """Load a connection and verify ownership."""
    stmt = select(DatabaseConnection).where(
        DatabaseConnection.id == connection_id,
        DatabaseConnection.user_id == user_id,
    )
    result = await db.execute(stmt)
    connection = result.scalar_one_or_none()
    if connection is None:
        raise HTTPException(status_code=404, detail="Connection not found.")
    return connection
