"""
Message publisher for RabbitMQ.

This module provides functionality to publish mail messages to RabbitMQ.
The publisher handles routing key selection based on mail type.

Key Concepts:
    - Publisher: Sends messages to an exchange
    - Routing Key: Determines which queue receives the message
    - Message Properties: Metadata like delivery_mode, content_type, etc.
"""

import json
from typing import Any

from aio_pika import Message

from app.core.logging import logger
from app.rabbitmq.connection import get_channel
from app.rabbitmq.exchange import (
    MailType,
    setup_topology,
)


async def publish_mail(
    mail_type: MailType,
    email: str,
    subject: str,
    content: str,
    **kwargs: Any,
) -> bool:
    """Publish a mail task to RabbitMQ.

    This function demonstrates the core publishing workflow:
        1. Get a channel
        2. Ensure topology exists
        3. Create a message
        4. Publish to exchange with routing key

    Args:
        mail_type: Type of mail (register, verify, marketing, system).
        email: Recipient email address.
        subject: Mail subject.
        content: Mail content.
        **kwargs: Additional fields to include in the message body.

    Returns:
        bool: True if published successfully.

    Example:
        ```python
        await publish_mail(
            mail_type=MailType.REGISTER,
            email="user@example.com",
            subject="Welcome!",
            content="Welcome to our service!",
        )
        ```

    Note:
        Messages are not persistent in Step 1.
        They will be lost if RabbitMQ restarts.
        (Step 2 adds durable queues and persistent messages)
    """
    channel = await get_channel()
    exchange, _ = await setup_topology(channel)

    # Build message body
    body = {
        "email": email,
        "subject": subject,
        "content": content,
        "mail_type": mail_type.value,
        **kwargs,
    }

    # Create message
    # In Step 1, we use simple delivery mode
    # Step 2 will add delivery_mode=DeliveryMode.PERSISTENT
    message = Message(
        body=json.dumps(body).encode(),
        content_type="application/json",
    )

    # Get routing key for this mail type
    routing_key = mail_type.get_routing_key()

    # Publish to exchange
    await exchange.publish(
        message=message,
        routing_key=routing_key,
    )

    logger.info(f"Published mail to {routing_key}: {email}")
    return True


async def publish_test_messages(count: int = 5) -> None:
    """Publish test messages for learning purposes.

    This helper function sends test messages to different queues
    to demonstrate routing behavior.

    Args:
        count: Number of test messages to send for each type.

    Example:
        ```python
        await publish_test_messages(count=10)
        # Sends 10 messages to each queue type
        ```
    """
    for i in range(count):
        # Register mail
        await publish_mail(
            mail_type=MailType.REGISTER,
            email=f"user{i}@example.com",
            subject=f"Welcome User {i}!",
            content=f"Welcome to our service, user{i}!",
        )

        # Marketing mail
        await publish_mail(
            mail_type=MailType.MARKETING,
            email=f"customer{i}@example.com",
            subject=f"Special Offer {i}",
            content=f"Check out our amazing offer #{i}!",
        )

    logger.info(f"Published {count * 2} test messages")
