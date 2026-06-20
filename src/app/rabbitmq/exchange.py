"""
RabbitMQ Exchange and Queue declarations.

This module defines the RabbitMQ topology for the Mail Center application.
It declares exchanges, queues, and their bindings.

Key Concepts:
    - Exchange: Receives messages from producers and routes them to queues
    - Queue: Stores messages until consumed by a worker
    - Binding: Links a queue to an exchange with a routing key pattern
    - Topic Exchange: Routes messages based on routing key patterns (wildcards supported)

Routing Keys:
    - mail.register  → register.queue
    - mail.verify    → verify.queue
    - mail.marketing → marketing.queue
    - mail.system    → system.queue
"""

from enum import Enum

import aio_pika
from aio_pika import Exchange, Queue, Channel

from app.core.config import get_settings
from app.core.logging import logger


class MailType(str, Enum):
    """Supported mail types with corresponding routing keys."""

    REGISTER = "register"
    VERIFY = "verify"
    MARKETING = "marketing"
    SYSTEM = "system"

    def get_routing_key(self) -> str:
        """Get the routing key for this mail type.

        Returns:
            str: The routing key in format 'mail.{type}'.
        """
        return f"mail.{self.value}"


# Queue names
QUEUE_REGISTER = "register.queue"
QUEUE_VERIFY = "verify.queue"
QUEUE_MARKETING = "marketing.queue"
QUEUE_SYSTEM = "system.queue"

# Routing key patterns
ROUTING_KEY_REGISTER = "mail.register"
ROUTING_KEY_VERIFY = "mail.verify"
ROUTING_KEY_MARKETING = "mail.marketing"
ROUTING_KEY_SYSTEM = "mail.system"


async def declare_exchange(channel: Channel) -> Exchange:
    """Declare the main topic exchange.

    A Topic Exchange routes messages to queues based on routing key patterns.
    Wildcards in bindings:
        - * (star) matches exactly one word
        - # (hash) matches zero or more words

    Args:
        channel: RabbitMQ channel.

    Returns:
        Exchange: The declared exchange.

    Example:
        ```python
        channel = await get_channel()
        exchange = await declare_exchange(channel)
        # Now you can publish messages to this exchange
        ```
    """
    settings = get_settings()

    exchange = await channel.declare_exchange(
        name=settings.exchange_name,
        type=aio_pika.ExchangeType.TOPIC,
        durable=False,  # Will be True in Step 2
    )

    logger.info(f"Declared exchange: {settings.exchange_name} (type: TOPIC)")
    return exchange


async def declare_queues(channel: Channel) -> dict[str, Queue]:
    """Declare all queues for different mail types.

    Each queue receives messages for a specific mail type.

    Args:
        channel: RabbitMQ channel.

    Returns:
        dict[str, Queue]: Mapping of queue names to Queue objects.

    Example:
        ```python
        queues = await declare_queues(channel)
        register_queue = queues[QUEUE_REGISTER]
        ```
    """
    queues = {}

    queue_names = [
        QUEUE_REGISTER,
        QUEUE_VERIFY,
        QUEUE_MARKETING,
        QUEUE_SYSTEM,
    ]

    for queue_name in queue_names:
        queue = await channel.declare_queue(
            name=queue_name,
            durable=False,  # Will be True in Step 2
        )
        queues[queue_name] = queue
        logger.info(f"Declared queue: {queue_name}")

    return queues


async def setup_bindings(
    exchange: Exchange,
    queues: dict[str, Queue],
) -> None:
    """Bind queues to the exchange with routing keys.

    This is where we define the routing rules:
        - register.queue  ← mail.register
        - verify.queue    ← mail.verify
        - marketing.queue ← mail.marketing
        - system.queue    ← mail.system

    Args:
        exchange: The topic exchange.
        queues: Dictionary of queue name to Queue object.

    Example:
        ```python
        await setup_bindings(exchange, queues)
        # Now messages with routing key 'mail.register' will go to register.queue
        ```
    """
    # Define bindings: queue_name → routing_key
    bindings = {
        QUEUE_REGISTER: ROUTING_KEY_REGISTER,
        QUEUE_VERIFY: ROUTING_KEY_VERIFY,
        QUEUE_MARKETING: ROUTING_KEY_MARKETING,
        QUEUE_SYSTEM: ROUTING_KEY_SYSTEM,
    }

    for queue_name, routing_key in bindings.items():
        queue = queues[queue_name]
        await queue.bind(exchange, routing_key=routing_key)
        logger.info(f"Bound {queue_name} to {routing_key}")


async def setup_topology(channel: Channel) -> tuple[Exchange, dict[str, Queue]]:
    """Set up the complete RabbitMQ topology.

    This is the main entry point for setting up exchanges, queues, and bindings.
    Call this once when your application starts.

    Args:
        channel: RabbitMQ channel.

    Returns:
        tuple: (Exchange, dict of Queue name to Queue object)

    Example:
        ```python
        channel = await get_channel()
        exchange, queues = await setup_topology(channel)
        # Topology is ready for publishing/consuming
        ```
    """
    logger.info("Setting up RabbitMQ topology...")

    exchange = await declare_exchange(channel)
    queues = await declare_queues(channel)
    await setup_bindings(exchange, queues)

    logger.info("RabbitMQ topology setup complete")
    return exchange, queues
