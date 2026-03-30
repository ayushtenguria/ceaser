"""Application configuration loaded from environment variables."""

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Central settings object — values are pulled from env vars / .env file."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # ── Database ────────────────────────────────────────────────────────
    database_url: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/ceaser"

    # ── LLM keys ───────────────────────────────────────────────────────
    gemini_api_key: str = ""
    anthropic_api_key: str = ""

    # ── Clerk auth ─────────────────────────────────────────────────────
    clerk_publishable_key: str = ""
    clerk_secret_key: str = ""
    clerk_jwks_url: str = "https://your-clerk-domain/.well-known/jwks.json"

    # ── Encryption (Fernet key for stored DB credentials) ──────────────
    encryption_key: str = ""

    # ── Dev mode ────────────────────────────────────────────────────────
    dev_mode: bool = False  # Set DEV_MODE=true in .env for local development

    # ── Super admin ────────────────────────────────────────────────────
    # Override via SUPER_ADMIN_EMAILS env var in production
    super_admin_emails: list[str] = []  # Set via SUPER_ADMIN_EMAILS env var

    # ── LLM model names ──────────────────────────────────────────────
    gemini_model: str = "gemini-2.0-flash"            # Heavy tier: SQL, Python, analysis, reports
    gemini_model_light: str = "gemini-2.0-flash"       # Light tier: router, verifier, suggestions, memory
    # When more models are enabled on your API key, upgrade to:
    # gemini_model: gemini-2.5-flash (or gemini-3-flash when available)
    # gemini_model_light: gemini-2.0-flash-lite (or gemini-3.1-flash-lite)
    claude_model: str = "claude-sonnet-4-20250514"

    # ── CORS ───────────────────────────────────────────────────────────
    cors_origins: list[str] = ["http://localhost:5173"]

    # ── Agent tuning (override per deployment) ────────────────────────
    max_retries: int = 3                 # Max agent retry attempts
    sql_row_limit: int = 1000            # Default LIMIT added to SQL queries
    sandbox_timeout_seconds: int = 30    # Python sandbox execution timeout
    cell_timeout_seconds: int = 120      # Notebook cell execution timeout
    max_excel_rows: int = 2_000_000      # Max rows to load from Excel files
    query_timeout_seconds: int = 60      # Per-query timeout in agent graph
    dev_fallback_email: str = "admin@ceaser.local"  # Dev mode user email

    # ── Payment gateway ──────────────────────────────────────────────────
    payment_provider: str = ""  # "stripe", "razorpay", or "cashfree"

    # Stripe
    stripe_secret_key: str = ""
    stripe_publishable_key: str = ""
    stripe_webhook_secret: str = ""
    stripe_price_map: str = ""  # "starter:price_xxx,business:price_yyy"

    # Razorpay
    razorpay_key_id: str = ""
    razorpay_key_secret: str = ""
    razorpay_webhook_secret: str = ""
    razorpay_plan_map: str = ""  # "starter:plan_xxx,business:plan_yyy"

    # Cashfree
    cashfree_app_id: str = ""
    cashfree_secret_key: str = ""
    cashfree_webhook_secret: str = ""
    cashfree_plan_map: str = ""  # "starter:plan_xxx,business:plan_yyy"
    cashfree_sandbox: bool = False

    # ── Notifications ────────────────────────────────────────────────
    slack_webhook_url: str = ""
    slack_bot_token: str = ""
    sendgrid_api_key: str = ""
    sendgrid_from_email: str = "reports@ceaser.app"

    # ── Snowflake ─────────────────────────────────────────────────────
    snowflake_account: str = ""
    snowflake_user: str = ""
    snowflake_password: str = ""
    snowflake_warehouse: str = ""
    snowflake_database: str = ""
    snowflake_schema: str = ""

    # ── BigQuery ──────────────────────────────────────────────────────
    bigquery_project_id: str = ""
    bigquery_credentials_json: str = ""  # base64-encoded service account JSON

    # ── Neo4j (Graph RAG schema layer) ──────────────────────────────
    neo4j_uri: str = ""
    neo4j_username: str = "neo4j"
    neo4j_password: str = ""

    # ── Storage ──────────────────────────────────────────────────────
    storage_backend: str = "local"  # "local" or "supabase"
    supabase_url: str = ""
    supabase_service_key: str = ""
    supabase_bucket: str = "ceaser-files"


@lru_cache
def get_settings() -> Settings:
    """Return a cached singleton of the application settings."""
    return Settings()
