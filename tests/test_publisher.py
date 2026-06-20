"""
Tests for RabbitMQ publisher.

These tests verify the publishing functionality without requiring
a real RabbitMQ connection.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from app.rabbitmq.publisher import publish_mail, publish_test_messages
from app.rabbitmq.exchange import MailType


class TestPublishMail:
    """Tests for publish_mail function."""

    @pytest.mark.asyncio
    async def test_publish_register_mail(self, mock_channel: MagicMock, mock_exchange: MagicMock) -> None:
        """Test publishing a registration mail."""
        # Setup mocks
        mock_channel.declare_exchange.return_value = mock_exchange

        with patch("app.rabbitmq.publisher.get_channel", return_value=mock_channel):
            with patch("app.rabbitmq.publisher.setup_topology", return_value=(mock_exchange, {})):
                # Publish mail
                result = await publish_mail(
                    mail_type=MailType.REGISTER,
                    email="user@example.com",
                    subject="Welcome",
                    content="Welcome to our service!",
                )

        # Verify result
        assert result is True

        # Verify exchange.publish was called
        mock_exchange.publish.assert_called_once()

        # Verify routing key
        call_args = mock_exchange.publish.call_args
        assert call_args[1]["routing_key"] == "mail.register"

    @pytest.mark.asyncio
    async def test_publish_marketing_mail(self, mock_channel: MagicMock, mock_exchange: MagicMock) -> None:
        """Test publishing a marketing mail."""
        # Setup mocks
        mock_channel.declare_exchange.return_value = mock_exchange

        with patch("app.rabbitmq.publisher.get_channel", return_value=mock_channel):
            with patch("app.rabbitmq.publisher.setup_topology", return_value=(mock_exchange, {})):
                # Publish mail
                result = await publish_mail(
                    mail_type=MailType.MARKETING,
                    email="customer@example.com",
                    subject="Special Offer",
                    content="Check out our offer!",
                )

        # Verify result
        assert result is True

        # Verify routing key
        call_args = mock_exchange.publish.call_args
        assert call_args[1]["routing_key"] == "mail.marketing"

    @pytest.mark.asyncio
    async def test_publish_message_body_contains_email(self, mock_channel: MagicMock, mock_exchange: MagicMock) -> None:
        """Test that message body contains email."""
        # Setup mocks
        mock_channel.declare_exchange.return_value = mock_exchange

        with patch("app.rabbitmq.publisher.get_channel", return_value=mock_channel):
            with patch("app.rabbitmq.publisher.setup_topology", return_value=(mock_exchange, {})):
                # Publish mail
                await publish_mail(
                    mail_type=MailType.REGISTER,
                    email="test@example.com",
                    subject="Test",
                    content="Hello",
                )

        # Get message from publish call
        call_args = mock_exchange.publish.call_args
        message = call_args[1]["message"]

        # Verify message body
        import json
        body = json.loads(message.body.decode())
        assert body["email"] == "test@example.com"
        assert body["subject"] == "Test"
        assert body["content"] == "Hello"
        assert body["mail_type"] == "register"


class TestPublishTestMessages:
    """Tests for publish_test_messages helper."""

    @pytest.mark.asyncio
    async def test_publish_test_messages_count(self, mock_channel: MagicMock, mock_exchange: MagicMock) -> None:
        """Test that correct number of messages are published."""
        # Setup mocks
        mock_channel.declare_exchange.return_value = mock_exchange

        with patch("app.rabbitmq.publisher.get_channel", return_value=mock_channel):
            with patch("app.rabbitmq.publisher.setup_topology", return_value=(mock_exchange, {})):
                # Publish test messages
                await publish_test_messages(count=3)

        # Verify total messages (register + marketing = 2 per count)
        # 3 * 2 = 6 messages
        assert mock_exchange.publish.call_count == 6