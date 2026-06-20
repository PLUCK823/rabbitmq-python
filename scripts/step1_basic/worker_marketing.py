#!/usr/bin/env python
"""
Step 1: Marketing Worker

This script demonstrates consuming messages from a different queue.

Key Concepts:
    1. Multiple Queues: Different queues for different message types
    2. Routing: Messages with 'mail.marketing' routing key go here
    3. Multiple Workers: You can run multiple workers simultaneously

Usage:
    uv run python scripts/step1_basic/worker_marketing.py

Note: Start RabbitMQ before running this worker.
"""

import asyncio

from app.rabbitmq.connection import close_connection
from app.rabbitmq.consumer import MarketingConsumer


async def main() -> None:
    """Run the marketing mail worker."""
    print("=" * 60)
    print("Step 1: Marketing Worker")
    print("=" * 60)
    print()
    print("This worker consumes messages from 'marketing.queue'")
    print("It processes marketing emails.")
    print()
    print("Key Concepts:")
    print("  - Multiple queues can run concurrently")
    print("  - Each worker handles one type of message")
    print("  - Routing keys determine which queue receives the message")
    print()

    consumer = MarketingConsumer()

    try:
        await consumer.start(prefetch_count=10)
    except KeyboardInterrupt:
        print()
        print("Worker stopped by user")
    finally:
        await close_connection()


if __name__ == "__main__":
    asyncio.run(main())
