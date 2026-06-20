#!/usr/bin/env python
"""
Step 3: Complete Setup with DLX, TTL, and Delay Queue

This script demonstrates the complete RabbitMQ topology with:
    - Dead Letter Exchange (DLX): Catches failed messages
    - TTL (Time-To-Live): Message expiration
    - Delay Queue: Retry with delay using TTL + DLX

Key Concepts:
    1. DLX: Exchange that receives messages from queues when they expire or are rejected
    2. TTL: Messages expire after a set time
    3. Delay Queue: Messages wait (TTL) then route to DLX for retry
    4. Retry Pattern: Failed message → Delay Queue → Re-process

Usage:
    uv run python scripts/step3_complete/setup_retry.py

Note: Run this once to set up the topology before running producer/worker.
"""

import asyncio

import aio_pika

from app.rabbitmq.connection import get_channel, close_connection
from app.core.config import get_settings
from app.core.logging import logger


async def setup_complete_topology() -> None:
    """Set up the complete RabbitMQ topology with DLX and retry queues.

    Topology:
        mail.topic.complete (Exchange)
            ├── register.queue.complete (Queue) → Worker
            │       └── on reject → retry.queue (TTL 5s)
            │
            ├── retry.queue (Queue, TTL 5s)
            │       └── on expire → mail.retry (DLX)
            │
            └── mail.retry (DLX)
                    └── routes back to register.queue.complete

        dlx.exchange (Dead Letter Exchange)
            └── dlq.queue (Dead Letter Queue)
                    └── Messages after 3 retries
    """
    settings = get_settings()
    channel = await get_channel()

    # ========================================
    # 1. Declare Exchanges
    # ========================================

    # Main topic exchange
    main_exchange = await channel.declare_exchange(
        name="mail.topic.complete",
        type=aio_pika.ExchangeType.TOPIC,
        durable=True,
    )
    logger.info("Declared exchange: mail.topic.complete")

    # Dead Letter Exchange (DLX)
    # This catches messages that have failed max retries
    dlx = await channel.declare_exchange(
        name="dlx.exchange",
        type=aio_pika.ExchangeType.DIRECT,
        durable=True,
    )
    logger.info("Declared DLX: dlx.exchange")

    # Retry exchange (routes messages back for retry)
    retry_exchange = await channel.declare_exchange(
        name="mail.retry",
        type=aio_pika.ExchangeType.DIRECT,
        durable=True,
    )
    logger.info("Declared retry exchange: mail.retry")

    # ========================================
    # 2. Declare Queues
    # ========================================

    # Dead Letter Queue - final destination for failed messages
    dlq = await channel.declare_queue(
        name="dlq.queue",
        durable=True,
    )
    await dlq.bind(dlx, routing_key="dead")
    logger.info("Declared DLQ: dlq.queue")

    # Retry queue with TTL
    # Messages wait here for 5 seconds before being routed back
    retry_queue = await channel.declare_queue(
        name="retry.queue",
        durable=True,
        arguments={
            "x-message-ttl": settings.retry_ttl_seconds * 1000,  # TTL in milliseconds
            "x-dead-letter-exchange": "mail.retry",  # Where expired messages go
            "x-dead-letter-routing-key": "retry",  # Routing key for expired messages
        },
    )
    await retry_queue.bind(main_exchange, routing_key="mail.retry")
    logger.info(
        f"Declared retry.queue with TTL={settings.retry_ttl_seconds}s"
    )

    # Main processing queue
    # When messages fail, they go to retry.queue
    main_queue = await channel.declare_queue(
        name="register.queue.complete",
        durable=True,
        arguments={
            "x-dead-letter-exchange": "",  # Default exchange
            "x-dead-letter-routing-key": "retry.queue",  # Send failed messages to retry queue
        },
    )
    await main_queue.bind(main_exchange, routing_key="mail.register")
    logger.info("Declared main queue: register.queue.complete")

    # ========================================
    # 3. Setup Retry Bindings
    # ========================================

    # Bind retry queue to retry exchange
    # After TTL expires, messages route here for re-processing
    await main_queue.bind(retry_exchange, routing_key="retry")
    logger.info("Bound main queue to retry exchange")

    print()
    print("=" * 60)
    print("Complete Topology Setup")
    print("=" * 60)
    print()
    print("Exchange Structure:")
    print("  mail.topic.complete (Topic)")
    print("      └── mail.register → register.queue.complete")
    print("                             └── [FAIL] → retry.queue (TTL 5s)")
    print("                                              └── [EXPIRE] → mail.retry")
    print("                                                              └── retry → register.queue.complete")
    print()
    print("  dlx.exchange (Direct)")
    print("      └── dead → dlq.queue (after 3 retries)")
    print()
    print("Setup complete! Now run:")
    print("  1. uv run python scripts/step3_complete/worker.py")
    print("  2. uv run python scripts/step3_complete/producer.py")
    print()


async def main() -> None:
    """Run the setup."""
    print("Setting up complete RabbitMQ topology with DLX, TTL, and Delay Queue...")
    print()

    await setup_complete_topology()

    await close_connection()


if __name__ == "__main__":
    asyncio.run(main())
