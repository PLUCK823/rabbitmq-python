"""
Logging configuration for the Mail Center application.

Provides structured logging setup with configurable log levels.
"""

import logging
import sys

from app.core.config import get_settings


def setup_logging() -> logging.Logger:
    """Configure and return the application logger.

    Returns:
        logging.Logger: Configured logger instance.
    """
    settings = get_settings()

    logger = logging.getLogger("mail-center")
    logger.setLevel(getattr(logging, settings.log_level.upper()))

    # Create console handler
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(getattr(logging, settings.log_level.upper()))

    # Create formatter
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)

    # Add handler to logger
    logger.addHandler(handler)

    return logger


# Global logger instance
logger = setup_logging()
