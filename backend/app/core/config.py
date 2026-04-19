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

    database_url: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/ceaser"

    gemini_api_key: str = ""
    anthropic_api_key: str = ""

    clerk_publishable_key: str = ""
    clerk_secret_key: str = ""
    clerk_jwks_url: str = "https://your-clerk-domain/.well-known/jwks.json"

    encryption_key: str = ""

    dev_mode: bool = False

    super_admin_emails: list[str] = []

    gemini_model: str = "gemini-2.0-flash"
    gemini_model_light: str = "gemini-2.0-flash"
    claude_model: str = "claude-sonnet-4-20250514"

    cors_origins: list[str] = ["http://localhost:5173"]

    max_retries: int = 3
    sql_row_limit: int = 1000
    sandbox_timeout_seconds: int = 30
    cell_timeout_seconds: int = 120
    max_excel_rows: int = 2_000_000
    query_timeout_seconds: int = 60
    dev_fallback_email: str = "admin@ceaser.local"

    payment_provider: str = ""

    stripe_secret_key: str = ""
    stripe_publishable_key: str = ""
    stripe_webhook_secret: str = ""
    stripe_price_map: str = ""

    razorpay_key_id: str = ""
    razorpay_key_secret: str = ""
    razorpay_webhook_secret: str = ""
    razorpay_plan_map: str = ""

    cashfree_app_id: str = ""
    cashfree_secret_key: str = ""
    cashfree_webhook_secret: str = ""
    cashfree_plan_map: str = ""
    cashfree_sandbox: bool = False

    slack_webhook_url: str = ""
    slack_bot_token: str = ""
    sendgrid_api_key: str = ""
    sendgrid_from_email: str = "reports@ceaser.app"

    snowflake_account: str = ""
    snowflake_user: str = ""
    snowflake_password: str = ""
    snowflake_warehouse: str = ""
    snowflake_database: str = ""
    snowflake_schema: str = ""

    bigquery_project_id: str = ""
    bigquery_credentials_json: str = ""

    neo4j_uri: str = ""
    neo4j_username: str = "neo4j"
    neo4j_password: str = ""

    storage_backend: str = "local"
    supabase_url: str = ""
    supabase_service_key: str = ""
    supabase_bucket: str = "ceaser-files"

    # Async file-processing pipeline (Lambda via SQS). Leave empty to fall back
    # to inline processing for small files only.
    aws_region: str = "us-east-1"
    sqs_queue_url: str = ""
    hmac_shared_secret: str = ""
    parquet_s3_bucket: str = ""

    # Sandbox execution via Lambda. When set, Python code execution is offloaded
    # to this Lambda function instead of running as a subprocess on EC2.
    # Set to the Lambda function name (e.g. "ceaser-sandbox-executor").
    sandbox_lambda_function: str = ""

    # Fargate file processing. When set, file uploads are processed by a
    # Fargate task running the full excel orchestrator pipeline.
    fargate_cluster: str = ""
    fargate_task_definition: str = ""
    fargate_subnets: str = ""  # Comma-separated subnet IDs
    fargate_security_group: str = ""


@lru_cache
def get_settings() -> Settings:
    """Return a cached singleton of the application settings."""
    return Settings()
