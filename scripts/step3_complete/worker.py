#!/usr/bin/env python
"""
Step 3: Complete Worker with Retry and DLQ

This script demonstrates the complete retry mechanism:
    - Manual ACK with retry tracking
    - Failed messages go to retry.queue with TTL
    - Messages route back after TTL expires
    - After max retries, messages go to DLQ

Key Concepts:
    1. Retry Count: Track how many times a message has been retried
    2. TTL-based Delay: Messages wait before retry
    3. DLQ: Final destination for permanently failed messages
    4. Headers: Use message headers to store retry count

Usage:
    uv run python scripts/step3_complete/worker.py

Prerequisites:
    1. Run: uv run python scripts/step3_complete/setup_retry.py
"""

import asyncio
import json
import random

import aio_pika
from aio_pika import IncomingMessage, DeliveryMode

from app.rabbitmq.connection import get_channel, close_connection
from app.core.config import get_settings
from app.core.logging import logger


async def process_with_retry(message: IncomingMessage) -> None:
    """Process a message with retry logic.

    This function:
        1. Reads retry count from headers
        2. Processes the message
        3. On failure: increments retry count and rejects
        4. After max retries: sends to DLQ

    Args:
        message: The incoming message.
    """
    settings = get_settings()
    body = json.loads(message.body.decode())
    email = body.get("email", "unknown")

    # Get retry count from headers
    headers = message.headers or {}
    retry_count = headers.get("x-retry-count", 0)

    logger.info(f"Processing message for {email} (retry #{retry_count})")

    try:
        # Simulate random failure (50% chance to demonstrate retry)
        if random.random() < 0.5:
            raise Exception("Simulated processing failure")

        # Simulate processing
        await asyncio.sleep(0.1)

        logger.info(f"Successfully processed: {email}")
        # ACK the message
        await message.ack()

    except Exception as e:
        logger.warning(f"Failed to process {email}: {e}")

        # Check if we've exceeded max retries
        if retry_count >= settings.max_retry_count - 1:
            logger.error(
                f"Max retries ({settings.max_retry_count}) exceeded for {email}, sending to DLQ"
            )
            # NACK without requeue - will go to DLQ
            await message.nack(requeue=False)
        else:
            logger.info(f"Retrying message for {email} (attempt {retry_count + 1})")
            # NACK with requeue - will go to retry.queue
            # Note: The queue's DLX settings handle the retry routing
            await message.nack(requeue=False)

            # Publish to retry queue with updated retry count
            channel = await get_channel()
            retry_exchange = await channel.get_exchange("mail.topic.complete")

            # Update retry count
            new_headers = dict(headers)
            new_headers["x-retry-count"] = retry_count + 1

            retry_message = aio_pika.Message(
                body=message.body,
                content_type="application/json",
                delivery_mode=DeliveryMode.PERSISTENT,
                headers=new_headers,
            )

            # Publish to retry routing key
            await retry_exchange.publish(
                message=retry_message,
                routing_key="mail.retry",
            )


async def main() -> None:
    """Run the complete worker."""
    settings = get_settings()

    print("=" * 60)
    print("Step 3: Complete Worker with Retry and DLQ")
    print("=" * 60)
    print()
    print("Key Concepts:")
    print("  1. Retry Count: Track retries in message headers")
    print("  2. TTL Delay: Messages wait in retry.queue")
    print("  3. DLQ: Failed messages after max retries")
    print()
    print(f"Configuration:")
    print(f"  Max retries: {settings.max_retry_count}")
    print(f"  Retry TTL: {settings.retry_ttl_seconds}s")
    print()
    print("Simulating 50% failure rate to demonstrate retry")
    print()

    channel = await get_channel()

    # Set prefetch
    await channel.set_qos(prefetch_count=settings.prefetch_count)

    # Get the main queue
    queue = await channel.get_queue("register.queue.complete")

    print("Consuming from: register.queue.complete")
    print("Press Ctrl+C to stop")
    print()

    # Start consuming
    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            await process_with_retry(message)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print()
        print("Worker stopped by user")
    finally:
        asyncio.run(close_connection())
