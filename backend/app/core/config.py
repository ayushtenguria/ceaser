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
    dev_mode: bool = True

    # ── CORS ───────────────────────────────────────────────────────────
    cors_origins: list[str] = ["http://localhost:5173"]


@lru_cache
def get_settings() -> Settings:
    """Return a cached singleton of the application settings."""
    return Settings()
