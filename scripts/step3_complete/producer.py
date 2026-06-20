#!/usr/bin/env python
"""
Step 3: Complete Producer

This script publishes messages to the complete topology with retry support.

Key Concepts:
    - Messages include a retry count in headers
    - Failed messages go to retry.queue with TTL
    - After TTL, messages route back for re-processing
    - After max retries, messages go to DLQ

Usage:
    uv run python scripts/step3_complete/producer.py

Prerequisites:
    1. Run: uv run python scripts/step3_complete/setup_retry.py
"""

import asyncio
import json

import aio_pika
from aio_pika import DeliveryMode

from app.core.logging import logger
from app.rabbitmq.connection import close_connection, get_channel
from app.rabbitmq.exchange import MailType


async def publish_with_retry_headers(
    mail_type: MailType,
    email: str,
    subject: str,
    content: str,
) -> None:
    """Publish a message that supports retry with tracking.

    Messages include headers to track retry count.
    Workers will check and update these headers.

    Args:
        mail_type: Type of mail.
        email: Recipient email.
        subject: Mail subject.
        content: Mail content.
    """
    channel = await get_channel()
    exchange = await channel.get_exchange("mail.topic.complete")

    body = {
        "email": email,
        "subject": subject,
        "content": content,
        "mail_type": mail_type.value,
    }

    # Create message with retry tracking headers
    message = aio_pika.Message(
        body=json.dumps(body).encode(),
        content_type="application/json",
        delivery_mode=DeliveryMode.PERSISTENT,
        headers={
            "x-retry-count": 0,  # Initial retry count
        },
    )

    routing_key = mail_type.get_routing_key()
    await exchange.publish(message=message, routing_key=routing_key)

    logger.info(f"Published message to {routing_key}: {email}")


async def main() -> None:
    """Run the complete producer."""
    print("=" * 60)
    print("Step 3: Complete Producer with Retry Support")
    print("=" * 60)
    print()
    print("This producer sends messages to the complete topology:")
    print("  - Messages have retry count tracking")
    print("  - Failed messages go to retry.queue (TTL 5s)")
    print("  - After retry, messages route back for re-processing")
    print("  - After 3 failures, messages go to DLQ")
    print()

    # Publish test messages
    for i in range(5):
        await publish_with_retry_headers(
            mail_type=MailType.REGISTER,
            email=f"user{i}@example.com",
            subject=f"Welcome User {i}!",
            content="Welcome to our service!",
        )

    print()
    print("Messages published!")
    print()
    print("Run the worker to see retry behavior:")
    print("  uv run python scripts/step3_complete/worker.py")

    await close_connection()


if __name__ == "__main__":
    asyncio.run(main())
