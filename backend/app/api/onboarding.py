"""Customer onboarding — setup guide and DB connection instructions."""

from __future__ import annotations

from fastapi import APIRouter

router = APIRouter(prefix="/onboarding", tags=["onboarding"])


@router.get("/setup-guide")
async def get_setup_guide() -> dict:
    """Return setup instructions for customers connecting their database."""
    return {
        "title": "Connect Your Database to Ceaser",
        "steps": [
            {
                "step": 1,
                "title": "Create a read-only database user",
                "description": "For security, create a dedicated read-only user for Ceaser. This ensures we can only READ your data — never modify or delete it.",
                "instructions": {
                    "postgresql": [
                        "CREATE USER ceaser_readonly WITH PASSWORD 'your-secure-password';",
                        "GRANT CONNECT ON DATABASE your_database TO ceaser_readonly;",
                        "GRANT USAGE ON SCHEMA public TO ceaser_readonly;",
                        "GRANT SELECT ON ALL TABLES IN SCHEMA public TO ceaser_readonly;",
                        "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO ceaser_readonly;",
                    ],
                    "mysql": [
                        "CREATE USER 'ceaser_readonly'@'%' IDENTIFIED BY 'your-secure-password';",
                        "GRANT SELECT ON your_database.* TO 'ceaser_readonly'@'%';",
                        "FLUSH PRIVILEGES;",
                    ],
                },
            },
            {
                "step": 2,
                "title": "Whitelist our IP address",
                "description": "If your database has firewall rules, allow connections from our server IP.",
                "note": "For testing on localhost, skip this step.",
            },
            {
                "step": 3,
                "title": "Add connection in Ceaser",
                "description": "Go to Connections → Add Connection. Enter your database details using the read-only user credentials.",
            },
            {
                "step": 4,
                "title": "Test the connection",
                "description": "Click 'Test Connection' to verify. We'll introspect your schema and you can start querying immediately.",
            },
        ],
        "security_notes": [
            "All database credentials are encrypted at rest using AES-256 (Fernet).",
            "Only SELECT queries are executed — INSERT, UPDATE, DELETE are blocked at multiple layers.",
            "Query results are streamed directly to your browser — they are not stored on our servers unless you save a report.",
            "All queries are logged in the audit trail for compliance.",
        ],
    }
