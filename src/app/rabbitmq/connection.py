"""
RabbitMQ connection management.

This module provides async connection and channel management for RabbitMQ.
It implements a singleton pattern to reuse connections across the application.

Key Concepts:
    - Connection: A TCP connection to the RabbitMQ broker
    - Channel: A virtual connection within a physical connection
    - Connection pooling improves performance and resource usage
"""

from typing import TYPE_CHECKING

import aio_pika
from aio_pika import Connection, Channel

from app.core.config import get_settings
from app.core.logging import logger

if TYPE_CHECKING:
    pass

# Global connection instance (singleton pattern)
_connection: Connection | None = None


async def get_connection() -> Connection:
    """Get or create a RabbitMQ connection.

    This function implements the singleton pattern to reuse connections.
    If a connection doesn't exist or is closed, it creates a new one.

    Returns:
        Connection: An active RabbitMQ connection.

    Example:
        ```python
        conn = await get_connection()
        # Use connection...
        ```

    Note:
        Connections are expensive to create. Reuse them when possible.
    """
    global _connection

    settings = get_settings()

    if _connection is None or _connection.is_closed:
        logger.info(f"Connecting to RabbitMQ at {settings.rabbitmq_url}")
        _connection = await aio_pika.connect_robust(settings.rabbitmq_url)
        logger.info("Connected to RabbitMQ")

    return _connection


async def get_channel() -> Channel:
    """Get a new channel from the connection.

    Channels are lightweight and can be created per operation.
    Each channel is independent and can have its own settings.

    Returns:
        Channel: A new RabbitMQ channel.

    Example:
        ```python
        channel = await get_channel()
        # Use channel for publishing/consuming...
        ```

    Note:
        Channels are not thread-safe. Create a new channel for each
        concurrent operation.
    """
    connection = await get_connection()
    channel = await connection.channel()
    logger.debug("Created new channel")
    return channel


async def close_connection() -> None:
    """Close the RabbitMQ connection.

    Call this during application shutdown to cleanly disconnect.

    Example:
        ```python
        await close_connection()
        ```
    """
    global _connection

    if _connection is not None and not _connection.is_closed:
        await _connection.close()
        logger.info("Closed RabbitMQ connection")

    _connection = None
