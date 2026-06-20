"""
Tests for RabbitMQ consumer.

These tests verify the consumer functionality without requiring
a real RabbitMQ connection.
"""

import contextlib
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.rabbitmq.consumer import BaseConsumer, MarketingConsumer, RegisterConsumer


class MockConsumer(BaseConsumer):
    """Mock consumer for testing."""

    queue_name = "test.queue"
    processed_messages: list

    def __init__(self) -> None:
        """Initialize mock consumer."""
        self.processed_messages = []

    async def process_message(self, body: dict) -> None:
        """Process a message by storing it."""
        self.processed_messages.append(body)


class TestBaseConsumer:
    """Tests for BaseConsumer class."""

    @pytest.mark.asyncio
    async def test_on_message_parses_json(self, mock_message: MagicMock) -> None:
        """Test that on_message parses JSON body."""
        consumer = MockConsumer()

        await consumer.on_message(mock_message)

        # Verify message was processed
        assert len(consumer.processed_messages) == 1
        assert consumer.processed_messages[0]["email"] == "test@example.com"

    @pytest.mark.asyncio
    async def test_on_message_handles_error(self, mock_message: MagicMock) -> None:
        """Test that on_message handles processing errors."""
        consumer = MockConsumer()

        # Make process_message raise an error
        async def failing_process(body: dict) -> None:
            raise Exception("Processing failed")

        consumer.process_message = failing_process

        # Should not raise, just log error
        await consumer.on_message(mock_message)

    @pytest.mark.asyncio
    async def test_start_sets_qos(self, mock_channel: MagicMock, mock_queue: MagicMock) -> None:
        """Test that start sets QoS prefetch."""
        consumer = MockConsumer()

        # Setup mocks
        mock_queue.iterator.return_value.__aenter__ = AsyncMock(
            return_value=iter([])  # Empty iterator to exit immediately
        )
        mock_queue.iterator.return_value.__aexit__ = AsyncMock()

        with (
            patch("app.rabbitmq.consumer.get_channel", return_value=mock_channel),
            patch(
                "app.rabbitmq.consumer.setup_topology",
                return_value=(MagicMock(), {"test.queue": mock_queue}),
            ),
            patch("app.rabbitmq.consumer.asyncio.Future", side_effect=KeyboardInterrupt),
            contextlib.suppress(KeyboardInterrupt),
        ):
            # Run start (will be interrupted immediately)
            await consumer.start(prefetch_count=10)

        # Verify QoS was set
        mock_channel.set_qos.assert_called_once_with(prefetch_count=10)


class TestRegisterConsumer:
    """Tests for RegisterConsumer class."""

    @pytest.mark.asyncio
    async def test_queue_name(self) -> None:
        """Test that RegisterConsumer has correct queue name."""
        consumer = RegisterConsumer()
        assert consumer.queue_name == "register.queue"

    @pytest.mark.asyncio
    async def test_process_message(self) -> None:
        """Test processing a register mail message."""
        consumer = RegisterConsumer()

        body = {
            "email": "user@example.com",
            "subject": "Welcome",
            "content": "Hello!",
        }

        # Should not raise
        await consumer.process_message(body)


class TestMarketingConsumer:
    """Tests for MarketingConsumer class."""

    @pytest.mark.asyncio
    async def test_queue_name(self) -> None:
        """Test that MarketingConsumer has correct queue name."""
        consumer = MarketingConsumer()
        assert consumer.queue_name == "marketing.queue"

    @pytest.mark.asyncio
    async def test_process_message(self) -> None:
        """Test processing a marketing mail message."""
        consumer = MarketingConsumer()

        body = {
            "email": "customer@example.com",
            "subject": "Special Offer",
            "content": "Check this out!",
        }

        # Should not raise
        await consumer.process_message(body)
