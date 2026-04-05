"""Database-connection management endpoints."""

from __future__ import annotations

import logging
import uuid

from fastapi import APIRouter, HTTPException, status
from sqlalchemy import select

from app.api.schemas import ConnectionCreate, ConnectionResponse, ConnectionTestResult, ConnectionUpdate
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

    from app.services.audit import log_action
    await log_action(
        db,
        user_id=current_user.user_id,
        action="create_connection",
        resource_type="connection",
        resource_id=str(connection.id),
        details={"name": body.name, "db_type": body.db_type, "host": body.host},
    )

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
    connection = await _load_connection(db, connection_id, user.id, user.organization_id or "")

    try:
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

        try:
            from app.services.schema_graph import build_schema_graph
            org_id = connection.organization_id or ""
            graph_tables = await build_schema_graph(
                str(connection_id), org_id, schema_dict, connection.db_type
            )
            logger.info("Schema graph built: %d tables", graph_tables)
        except Exception as exc:
            logger.warning("Schema graph build failed (non-blocking): %s", exc)

        return ConnectionTestResult(
            success=True,
            message="Connection successful.",
            schema_info=schema_dict,
        )
    except Exception as exc:
        connection.is_connected = False
        await db.flush()
        logger.warning("Connection test failed for %s: %s", connection_id, exc)
        # Sanitize error: don't expose internal DB details, versions, or paths
        error_str = str(exc).lower()
        if "password" in error_str or "authentication" in error_str:
            safe_msg = "Authentication failed. Check your username and password."
        elif "refused" in error_str or "could not connect" in error_str:
            safe_msg = "Connection refused. Check the host, port, and ensure the database is reachable."
        elif "timeout" in error_str:
            safe_msg = "Connection timed out. The database may be unreachable or behind a firewall."
        elif "does not exist" in error_str or "unknown database" in error_str:
            safe_msg = "Database not found. Check the database name."
        elif "ssl" in error_str or "certificate" in error_str:
            safe_msg = "SSL/TLS error. Contact your database administrator."
        elif "too many connections" in error_str:
            safe_msg = "Too many connections to the database. Try again later."
        else:
            safe_msg = "Connection failed. Please verify your connection settings."
        return ConnectionTestResult(success=False, message=safe_msg)


@router.post("/{connection_id}/schema", response_model=ConnectionTestResult)
async def refresh_schema(
    connection_id: uuid.UUID,
    current_user: CurrentUser,
    db: DbSession,
) -> ConnectionTestResult:
    """Re-introspect the schema for an existing connection and update the cache."""
    user = await require_permission(Permission.MANAGE_CONNECTIONS, current_user, db)
    connection = await _load_connection(db, connection_id, user.id, user.organization_id or "")

    try:
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


@router.patch("/{connection_id}", response_model=ConnectionResponse)
async def update_connection(
    connection_id: uuid.UUID,
    body: ConnectionUpdate,
    current_user: CurrentUser,
    db: DbSession,
) -> DatabaseConnection:
    """Update an existing connection's settings (host, port, credentials, etc.)."""
    from app.api.schemas import ConnectionUpdate
    user = await require_permission(Permission.MANAGE_CONNECTIONS, current_user, db)
    connection = await _load_connection(db, connection_id, user.id, user.organization_id or "")

    if body.name is not None:
        connection.name = body.name
    if body.host is not None:
        # Re-validate host for SSRF
        from app.api.schemas import ConnectionCreate
        ConnectionCreate.validate_host(body.host)
        connection.host = body.host
    if body.port is not None:
        connection.port = body.port
    if body.database is not None:
        connection.database = body.database
    if body.username is not None:
        connection.username = body.username
    if body.password is not None:
        connection.encrypted_password = encrypt_value(body.password)

    # Mark as needing re-test after credential change
    connection.is_connected = False

    await db.flush()
    await db.refresh(connection)

    from app.services.audit import log_action
    await log_action(
        db,
        user_id=current_user.user_id,
        action="update_connection",
        resource_type="connection",
        resource_id=str(connection_id),
        details={"fields_updated": [k for k, v in body.model_dump(exclude_none=True).items()]},
    )

    await db.commit()
    await db.refresh(connection)
    logger.info("Updated connection '%s' for user %s", connection.name, user.id)
    return connection


@router.delete("/{connection_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_connection(
    connection_id: uuid.UUID,
    current_user: CurrentUser,
    db: DbSession,
) -> None:
    """Delete a database connection."""
    user = await require_permission(Permission.MANAGE_CONNECTIONS, current_user, db)
    connection = await _load_connection(db, connection_id, user.id, user.organization_id or "")

    from app.services.audit import log_action
    await log_action(
        db,
        user_id=current_user.user_id,
        action="delete_connection",
        resource_type="connection",
        resource_id=str(connection_id),
        details={"name": connection.name, "db_type": connection.db_type},
    )

    await db.delete(connection)


@router.post("/{connection_id}/scan-metrics")
async def scan_metrics(
    connection_id: uuid.UUID,
    current_user: CurrentUser,
    db: DbSession,
) -> list[dict]:
    """Scan schema for potential metric definitions.

    Returns candidates with name, SQL expression, source table/column,
    confidence, and ambiguity notes. Does NOT auto-create anything.
    """
    user = await require_permission(Permission.QUERY_DATA, current_user, db)
    connection = await _load_connection(db, connection_id, user.id, user.organization_id or "")

    if not connection.schema_cache:
        raise HTTPException(status_code=400, detail="Schema not cached. Refresh the connection first.")

    from app.services.metric_scanner import scan_schema_for_metrics
    candidates = scan_schema_for_metrics(connection.schema_cache)

    connection.metrics_scanned = True
    await db.commit()

    return [
        {
            "name": c.name,
            "description": c.description,
            "sqlExpression": c.sql_expression,
            "category": c.category,
            "sourceTable": c.source_table,
            "sourceColumn": c.source_column,
            "confidence": c.confidence,
            "ambiguityNote": c.ambiguity_note,
        }
        for c in candidates
    ]


@router.post("/{connection_id}/accept-metrics")
async def accept_metrics(
    connection_id: uuid.UUID,
    body: dict,
    current_user: CurrentUser,
    db: DbSession,
) -> dict:
    """Accept selected metric candidates and create locked MetricDefinitions.

    Body: { "metrics": [{ "name", "description", "sqlExpression", "category", "isLocked" }] }
    """
    user = await require_permission(Permission.QUERY_DATA, current_user, db)
    org_id = user.organization_id or ""
    connection = await _load_connection(db, connection_id, user.id, user.organization_id or "")

    metrics_data = body.get("metrics", [])
    if not metrics_data:
        raise HTTPException(status_code=400, detail="No metrics provided.")

    from app.db.models import MetricDefinition

    created = 0
    for m in metrics_data:
        name = m.get("name", "").strip()
        sql_expr = m.get("sqlExpression", "").strip()
        if not name or not sql_expr:
            continue

        metric = MetricDefinition(
            name=name,
            description=m.get("description", ""),
            sql_expression=sql_expr,
            category=m.get("category", "general"),
            connection_id=connection_id,
            organization_id=org_id,
            user_id=user.id,
            is_locked=m.get("isLocked", True),
        )
        db.add(metric)
        created += 1

    await db.commit()
    logger.info("Accepted %d metrics for connection %s", created, connection_id)
    return {"created": created}


async def _load_connection(
    db: DbSession,
    connection_id: uuid.UUID,
    user_id: uuid.UUID,
    org_id: str = "",
) -> DatabaseConnection:
    """Load a connection and verify ownership + org isolation.

    Both user_id AND organization_id are checked to prevent cross-tenant access.
    """
    filters = [
        DatabaseConnection.id == connection_id,
        DatabaseConnection.user_id == user_id,
    ]
    if org_id:
        filters.append(DatabaseConnection.organization_id == org_id)

    stmt = select(DatabaseConnection).where(*filters)
    result = await db.execute(stmt)
    connection = result.scalar_one_or_none()
    if connection is None:
        raise HTTPException(status_code=404, detail="Connection not found.")
    return connection
