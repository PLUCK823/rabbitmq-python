"""
Simulated mail sender service.

This module provides a mock mail sender for demonstration purposes.
It simulates sending emails with a configurable failure rate.
"""

import random

from app.core.logging import logger


class MailSender:
    """Simulated mail sender with configurable failure rate.

    This class demonstrates how a real mail sender might work,
    with random failures to simulate real-world issues.

    Example:
        ```python
        sender = MailSender(failure_rate=0.3)
        success = await sender.send(
            email="user@example.com",
            subject="Welcome",
            content="Hello!"
        )
        ```
    """

    def __init__(self, failure_rate: float = 0.3) -> None:
        """Initialize the mail sender.

        Args:
            failure_rate: Probability of failure (0.0 to 1.0).
        """
        self.failure_rate = failure_rate

    async def send(
        self,
        email: str,
        subject: str,
        _content: str,
    ) -> bool:
        """Send an email (simulated).

        Args:
            email: Recipient email address.
            subject: Email subject.
            content: Email content.

        Returns:
            bool: True if sent successfully, False if failed.

        Raises:
            Exception: Simulated SMTP failure.
        """
        logger.info(f"Attempting to send mail to {email}: {subject}")

        # Simulate network delay
        # In real world, this would be an actual SMTP call

        # Random failure simulation
        if random.random() < self.failure_rate:
            logger.warning(f"Failed to send mail to {email} (simulated failure)")
            raise Exception(f"SMTP Error: Failed to send to {email}")

        logger.info(f"Successfully sent mail to {email}")
        return True


# Global mail sender instance
mail_sender = MailSender(failure_rate=0.3)
