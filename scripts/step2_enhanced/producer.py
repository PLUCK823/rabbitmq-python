#!/usr/bin/env python
"""
Step 2: Enhanced Producer

This script demonstrates enhanced RabbitMQ features:
    - Durable: Exchanges and queues survive broker restarts
    - Persistent Messages: Messages are saved to disk

Key Concepts:
    1. Durable Exchange: Exchange survives RabbitMQ restart
    2. Durable Queue: Queue and its messages survive restart
    3. Persistent Message: Individual message is saved to disk
    4. delivery_mode=2: Marks message as persistent

Usage:
    uv run python scripts/step2_enhanced/producer.py
"""

import asyncio
import json

import aio_pika
from aio_pika import DeliveryMode

from app.rabbitmq.connection import get_channel, close_connection
from app.rabbitmq.exchange import (
    MailType,
    QUEUE_REGISTER,
    QUEUE_MARKETING,
    ROUTING_KEY_REGISTER,
    ROUTING_KEY_MARKETING,
)
from app.core.logging import logger


async def setup_durable_topology(channel: aio_pika.Channel) -> aio_pika.Exchange:
    """Set up durable exchanges and queues.

    Durable means the exchange/queue will survive a RabbitMQ restart.
    This does NOT mean messages are persistent - that's a separate setting.

    Returns:
        Exchange: The durable topic exchange.
    """
    # Declare durable exchange
    exchange = await channel.declare_exchange(
        name="mail.topic.durable",
        type=aio_pika.ExchangeType.TOPIC,
        durable=True,  # Exchange survives restart
    )
    logger.info("Declared DURABLE exchange: mail.topic.durable")

    # Declare durable queues
    queues_config = {
        QUEUE_REGISTER: ROUTING_KEY_REGISTER,
        QUEUE_MARKETING: ROUTING_KEY_MARKETING,
    }

    for queue_name, routing_key in queues_config.items():
        queue = await channel.declare_queue(
            name=f"{queue_name}.durable",
            durable=True,  # Queue survives restart
        )
        await queue.bind(exchange, routing_key=routing_key)
        logger.info(f"Declared DURABLE queue: {queue_name}.durable → {routing_key}")

    return exchange


async def publish_persistent_mail(
    exchange: aio_pika.Exchange,
    mail_type: MailType,
    email: str,
    subject: str,
    content: str,
) -> None:
    """Publish a persistent message.

    Persistent messages are saved to disk and survive RabbitMQ restart.
    This requires:
        1. Durable queue
        2. Persistent message (delivery_mode=2)

    Args:
        exchange: The exchange to publish to.
        mail_type: Type of mail.
        email: Recipient email.
        subject: Mail subject.
        content: Mail content.
    """
    body = {
        "email": email,
        "subject": subject,
        "content": content,
        "mail_type": mail_type.value,
    }

    # Create PERSISTENT message
    message = aio_pika.Message(
        body=json.dumps(body).encode(),
        content_type="application/json",
        delivery_mode=DeliveryMode.PERSISTENT,  # Message is saved to disk
    )

    routing_key = mail_type.get_routing_key()
    await exchange.publish(message=message, routing_key=routing_key)

    logger.info(f"Published PERSISTENT message to {routing_key}: {email}")


async def main() -> None:
    """Run the enhanced producer."""
    print("=" * 60)
    print("Step 2: Enhanced Producer (Durable + Persistent)")
    print("=" * 60)
    print()
    print("New Concepts:")
    print("  1. Durable Exchange: Survives RabbitMQ restart")
    print("  2. Durable Queue: Queue and messages survive restart")
    print("  3. Persistent Message: delivery_mode=2 saves to disk")
    print()
    print("Important:")
    print("  - Durable queue + Persistent message = Message survives restart")
    print("  - Durable queue alone does NOT guarantee message persistence")
    print()

    channel = await get_channel()
    exchange = await setup_durable_topology(channel)

    # Publish persistent messages
    for i in range(5):
        await publish_persistent_mail(
            exchange=exchange,
            mail_type=MailType.REGISTER,
            email=f"user{i}@example.com",
            subject=f"Welcome User {i}!",
            content=f"Welcome to our service!",
        )

        await publish_persistent_mail(
            exchange=exchange,
            mail_type=MailType.MARKETING,
            email=f"customer{i}@example.com",
            subject=f"Special Offer {i}",
            content=f"Check out our offer!",
        )

    print()
    print("Persistent messages published!")
    print("These messages will survive a RabbitMQ restart.")
    print()
    print("Run the enhanced worker to consume:")
    print("  uv run python scripts/step2_enhanced/worker.py")

    await close_connection()


if __name__ == "__main__":
    asyncio.run(main())
