"""Settings, from the environment (prefix INTEGRATION_). Secrets are never settings: they're files
under secrets_dir (secrets.py), so they rotate without a restart and never sit in the environment."""

from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="INTEGRATION_")

    config_path: str = "integration.yaml"
    contracts_dir: str = "contracts"  # written by the SDLC tool, shipped in the image
    secrets_dir: str = "/var/run/secrets/valxel"  # one file per secret, mounted by the platform
    environment: str = "dev"
    product: str = ""
    # The product's Postgres for the inbox (webhook deliveries seen) and outbox (events to publish).
    database_url: str = ""
    kafka_bootstrap: str = ""
    kafka_config: dict[str, str] = {}  # security.protocol, sasl.*: as the platform's kafka-user gives them
    # Audit trail (audit.py): "kafka" in deployed environments, "log" locally. Required (data isn't
    # accepted or sent without its record) for payments and PHI unless set.
    audit_sink: str = "log"
    audit_kafka_bootstrap: str = ""
    audit_topic: str = "audit.v1"
    audit_kafka_config: dict[str, str] = {}
    audit_required: bool | None = None
    # Feature flags (flags.py): the product's Flipt over OFREP. Unset: every flag reads its default.
    flags_url: str = ""
    flags_environment: str = ""
    flags_namespace: str = ""
    flags_token: str = ""
    flags_timeout_s: float = 0.25
    flags_cache_s: float = 30.0
    port: int = 8080
