#!/usr/bin/env python
"""
Step 2: Enhanced Worker with ACK and Prefetch

This script demonstrates:
    - Manual ACK: Explicitly acknowledge message processing
    - NACK: Reject messages and optionally requeue
    - Prefetch: Control how many messages a worker can handle concurrently

Key Concepts:
    1. ACK: Confirms successful message processing
    2. NACK: Rejects message, can requeue for retry
    3. Prefetch: Limits unacknowledged messages per worker
    4. Redelivery: NACKed messages can be delivered again

Usage:
    uv run python scripts/step2_enhanced/worker.py

Note: Start RabbitMQ before running this worker.
"""

import asyncio
import json
import random

from aio_pika import IncomingMessage

from app.core.logging import logger
from app.rabbitmq.connection import close_connection, get_channel
from app.rabbitmq.exchange import QUEUE_REGISTER


async def process_with_ack(message: IncomingMessage, queue_name: str) -> None:
    """Process a message with explicit ACK/NACK handling.

    This demonstrates the ACK mechanism:
        - ACK: Message processed successfully, remove from queue
        - NACK: Processing failed, requeue for retry

    Args:
        message: The incoming message.
        queue_name: Name of the queue being consumed.
    """
    async with message.process():
        body = json.loads(message.body.decode())
        email = body.get("email", "unknown")
        subject = body.get("subject", "no subject")

        logger.info(f"[{queue_name}] Processing: {email} - {subject}")

        # Simulate random failure (30% chance)
        # This demonstrates NACK and requeue behavior
        if random.random() < 0.3:
            logger.warning(f"[{queue_name}] Simulated failure for: {email}")
            raise Exception("Simulated processing failure")

        # Simulate processing
        await asyncio.sleep(0.1)

        logger.info(f"[{queue_name}] Successfully processed: {email}")
        # Message is automatically ACKed when exiting context without exception


async def start_worker_with_ack(queue_suffix: str = "durable") -> None:
    """Start a worker with ACK and prefetch control.

    Args:
        queue_suffix: Suffix for queue name (e.g., 'durable').
    """
    channel = await get_channel()

    # Set prefetch count
    # This limits how many unacknowledged messages a worker can have
    # Lower values = fairer distribution among workers
    # Higher values = better throughput but potential imbalance
    prefetch_count = 10
    await channel.set_qos(prefetch_count=prefetch_count)
    logger.info(f"Set prefetch_count={prefetch_count}")

    # Get the queue
    queue_name = f"{QUEUE_REGISTER}.{queue_suffix}"
    queue = await channel.get_queue(queue_name)

    print("=" * 60)
    print("Step 2: Enhanced Worker (ACK + Prefetch)")
    print("=" * 60)
    print()
    print("Key Concepts:")
    print("  1. Manual ACK: Confirm successful processing")
    print("  2. NACK: Reject and requeue failed messages")
    print("  3. Prefetch: Limit concurrent unacknowledged messages")
    print()
    print(f"Consuming from: {queue_name}")
    print(f"Prefetch count: {prefetch_count}")
    print()
    print("Simulating 30% random failure rate to demonstrate NACK")
    print()
    print("Press Ctrl+C to stop")
    print()

    # Start consuming with automatic ACK handling via context manager
    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            await process_with_ack(message, queue_name)


async def main() -> None:
    """Run the enhanced worker."""
    try:
        await start_worker_with_ack()
    except KeyboardInterrupt:
        print()
        print("Worker stopped by user")
    finally:
        await close_connection()


if __name__ == "__main__":
    asyncio.run(main())
