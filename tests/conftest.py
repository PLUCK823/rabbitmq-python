"""
Shared pytest fixtures.

This module provides common fixtures for testing.
"""

import asyncio
from typing import AsyncGenerator, Generator
from unittest.mock import AsyncMock, MagicMock

import pytest

# Event loop fixture for async tests
@pytest.fixture(scope="session")
def event_loop() -> Generator[asyncio.AbstractEventLoop, None, None]:
    """Create an event loop for async tests."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def mock_channel() -> MagicMock:
    """Mock RabbitMQ channel.

    Returns:
        MagicMock: Mocked channel object.
    """
    channel = MagicMock()
    channel.declare_exchange = AsyncMock()
    channel.declare_queue = AsyncMock()
    channel.set_qos = AsyncMock()
    channel.get_exchange = AsyncMock()
    channel.get_queue = AsyncMock()
    return channel


@pytest.fixture
def mock_exchange() -> MagicMock:
    """Mock RabbitMQ exchange.

    Returns:
        MagicMock: Mocked exchange object.
    """
    exchange = MagicMock()
    exchange.publish = AsyncMock()
    return exchange


@pytest.fixture
def mock_queue() -> MagicMock:
    """Mock RabbitMQ queue.

    Returns:
        MagicMock: Mocked queue object.
    """
    queue = MagicMock()
    queue.bind = AsyncMock()
    queue.consume = AsyncMock()
    queue.iterator = AsyncMock()
    return queue


@pytest.fixture
def mock_message() -> MagicMock:
    """Mock RabbitMQ incoming message.

    Returns:
        MagicMock: Mocked message object.
    """
    message = MagicMock()
    message.body = b'{"email": "test@example.com", "subject": "Test", "content": "Hello"}'
    message.ack = AsyncMock()
    message.nack = AsyncMock()
    message.headers = {}
    return message
