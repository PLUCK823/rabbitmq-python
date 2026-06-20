"""
Configuration management using Pydantic Settings.

This module provides centralized configuration for the Mail Center application,
including RabbitMQ connection settings, exchange/queue names, and retry policies.
"""

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # RabbitMQ Connection
    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"

    # Exchange Settings
    exchange_name: str = "mail.topic"

    # Queue Settings
    prefetch_count: int = 10

    # Retry Settings
    max_retry_count: int = 3
    retry_ttl_seconds: int = 5

    # Logging
    log_level: str = "INFO"


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance.

    Returns:
        Settings: Application settings singleton.
    """
    return Settings()
