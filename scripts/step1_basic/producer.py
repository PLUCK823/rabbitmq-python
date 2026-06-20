#!/usr/bin/env python
"""
Step 1: Basic Producer

This script demonstrates the basics of publishing messages to RabbitMQ.

Key Concepts:
    1. Producer: Sends messages to an exchange
    2. Exchange: Routes messages to queues based on routing keys
    3. Routing Key: Determines message destination (e.g., mail.register)
    4. Topic Exchange: Supports wildcard routing patterns

Usage:
    uv run python scripts/step1_basic/producer.py
"""

import asyncio

from app.rabbitmq.publisher import publish_test_messages
from app.rabbitmq.connection import close_connection


async def main() -> None:
    """Run the producer to send test messages."""
    print("=" * 60)
    print("Step 1: Basic Producer")
    print("=" * 60)
    print()
    print("This producer will send test messages to RabbitMQ.")
    print("Messages will be routed to different queues based on routing keys:")
    print("  - mail.register  → register.queue")
    print("  - mail.marketing → marketing.queue")
    print()

    # Publish test messages
    await publish_test_messages(count=5)

    print()
    print("Messages published successfully!")
    print("Run a worker to consume these messages:")
    print("  uv run python scripts/step1_basic/worker_register.py")
    print("  uv run python scripts/step1_basic/worker_marketing.py")

    # Clean up
    await close_connection()


if __name__ == "__main__":
    asyncio.run(main())
