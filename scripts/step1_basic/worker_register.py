#!/usr/bin/env python
"""
Step 1: Register Worker

This script demonstrates the basics of consuming messages from RabbitMQ.

Key Concepts:
    1. Consumer: Subscribes to a queue and processes messages
    2. Queue: Stores messages until consumed
    3. auto_ack: Messages are automatically acknowledged when delivered

Usage:
    uv run python scripts/step1_basic/worker_register.py

Note: Start RabbitMQ before running this worker.
"""

import asyncio

from app.rabbitmq.consumer import RegisterConsumer
from app.rabbitmq.connection import close_connection


async def main() -> None:
    """Run the register mail worker."""
    print("=" * 60)
    print("Step 1: Register Worker")
    print("=" * 60)
    print()
    print("This worker consumes messages from 'register.queue'")
    print("It processes registration emails.")
    print()
    print("Key Concepts:")
    print("  - Consumer subscribes to a queue")
    print("  - Messages are processed one at a time")
    print("  - auto_ack=True: Messages acknowledged automatically")
    print()

    consumer = RegisterConsumer()

    try:
        await consumer.start(prefetch_count=10)
    except KeyboardInterrupt:
        print()
        print("Worker stopped by user")
    finally:
        await close_connection()


if __name__ == "__main__":
    asyncio.run(main())
