"""
Message consumer for RabbitMQ.

This module provides base consumer functionality for processing mail messages.
Consumers (also called Workers) receive messages from queues and process them.

Key Concepts:
    - Consumer: Subscribes to a queue and processes incoming messages
    - ACK: Acknowledgment sent to RabbitMQ to confirm message was processed
    - NACK: Negative acknowledgment (will be covered in Step 2)

In Step 1, we use auto_ack=True for simplicity.
The message is automatically acknowledged when delivered.
"""

import asyncio
import json
from abc import ABC, abstractmethod
from typing import Any

from aio_pika.abc import AbstractIncomingMessage

from app.core.logging import logger
from app.rabbitmq.connection import get_channel
from app.rabbitmq.exchange import QUEUE_MARKETING, QUEUE_REGISTER, setup_topology


class BaseConsumer(ABC):
    """Abstract base class for message consumers.

    Subclass this to create specific consumers for different queue types.
    Implement the process_message() method to define custom processing logic.

    Example:
        ```python
        class RegisterConsumer(BaseConsumer):
            queue_name = QUEUE_REGISTER

            async def process_message(self, body: dict) -> None:
                print(f"Processing registration: {body['email']}")
        ```
    """

    queue_name: str = ""

    async def on_message(self, message: AbstractIncomingMessage) -> None:
        """Callback when a message is received.

        This method handles message parsing and error handling.
        Override on_message() if you need custom error handling.

        Args:
            message: The incoming RabbitMQ message.

        Note:
            In Step 1, we use auto_ack=True, so no manual ACK needed.
            Messages are acknowledged automatically when delivered.
        """
        try:
            # Parse message body
            body = json.loads(message.body.decode())
            logger.info(f"Received message from {self.queue_name}: {body.get('email')}")

            # Process the message
            await self.process_message(body)

            logger.info(f"Successfully processed message from {self.queue_name}")

        except Exception as e:
            logger.error(f"Error processing message: {e}")
            # In Step 2, we'll add NACK and retry logic here

    @abstractmethod
    async def process_message(self, body: dict[str, Any]) -> None:
        """Process a message. Must be implemented by subclasses.

        Args:
            body: The parsed message body as a dictionary.

        Example:
            ```python
            async def process_message(self, body: dict) -> None:
                email = body['email']
                subject = body['subject']
                # Send the email...
            ```
        """
        pass

    async def start(self, prefetch_count: int = 10) -> None:
        """Start consuming messages from the queue.

        Args:
            prefetch_count: Maximum unacknowledged messages.
                           Higher values = better throughput but more memory.

        Example:
            ```python
            consumer = RegisterConsumer()
            await consumer.start()
            # Now consuming messages...
            ```

        Note:
            This method runs forever, listening for messages.
            Press Ctrl+C to stop.
        """
        channel = await get_channel()

        # Set prefetch count
        # This limits how many messages the worker can process concurrently
        # Higher values can improve throughput but increase memory usage
        await channel.set_qos(prefetch_count=prefetch_count)

        # Setup topology if not exists
        _, queues = await setup_topology(channel)

        # Get the queue
        queue = queues[self.queue_name]

        logger.info(f"Starting consumer for {self.queue_name}...")
        logger.info("Press Ctrl+C to stop")

        # Start consuming
        # In Step 1, we use auto_ack=True for simplicity
        # Step 2 will implement manual ACK
        await queue.consume(self.on_message)

        # Keep running
        try:
            await asyncio.Future()
        except KeyboardInterrupt:
            logger.info(f"Stopping consumer for {self.queue_name}")


class RegisterConsumer(BaseConsumer):
    """Consumer for registration mail queue.

    Processes messages from register.queue.

    Example:
        ```python
        consumer = RegisterConsumer()
        await consumer.start()
        ```
    """

    queue_name = QUEUE_REGISTER

    async def process_message(self, body: dict[str, Any]) -> None:
        """Process a registration mail message.

        Args:
            body: Message containing email, subject, content.
        """
        email = body.get("email", "unknown")
        subject = body.get("subject", "no subject")

        logger.info(f"[REGISTER] Sending to {email}: {subject}")

        # Simulate sending email
        await asyncio.sleep(0.1)  # Simulate network delay

        logger.info(f"[REGISTER] Mail sent to {email}")


class MarketingConsumer(BaseConsumer):
    """Consumer for marketing mail queue.

    Processes messages from marketing.queue.

    Example:
        ```python
        consumer = MarketingConsumer()
        await consumer.start()
        ```
    """

    queue_name = QUEUE_MARKETING

    async def process_message(self, body: dict[str, Any]) -> None:
        """Process a marketing mail message.

        Args:
            body: Message containing email, subject, content.
        """
        email = body.get("email", "unknown")
        subject = body.get("subject", "no subject")

        logger.info(f"[MARKETING] Sending to {email}: {subject}")

        # Simulate sending email
        await asyncio.sleep(0.1)  # Simulate network delay

        logger.info(f"[MARKETING] Mail sent to {email}")
