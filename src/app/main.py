"""
Mail Center - FastAPI Application Entry Point.

This is the main entry point for the Mail Center API.
It initializes the FastAPI application and sets up the RabbitMQ topology.

Usage:
    uv run uvicorn app.main:app --reload

Or run directly:
    uv run python -m app.main
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.router import api_router
from app.core.logging import logger
from app.rabbitmq.connection import get_channel, close_connection
from app.rabbitmq.exchange import setup_topology
from app.services.task_store import task_store


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager.

    Handles startup and shutdown events.
    """
    # Startup
    logger.info("Starting Mail Center API...")

    # Initialize task store
    await task_store.init()

    # Setup RabbitMQ topology
    channel = await get_channel()
    await setup_topology(channel)

    logger.info("Mail Center API started successfully")

    yield

    # Shutdown
    logger.info("Shutting down Mail Center API...")
    await close_connection()
    await task_store.close()
    logger.info("Mail Center API shut down complete")


# Create FastAPI application
app = FastAPI(
    title="Mail Center",
    description="RabbitMQ Learning Project - Mail Task Center API",
    version="0.1.0",
    lifespan=lifespan,
)

# Include API routers
app.include_router(api_router)


@app.get("/")
async def root() -> dict:
    """Root endpoint.

    Returns:
        dict: Welcome message.
    """
    return {
        "message": "Welcome to Mail Center API",
        "docs": "/docs",
        "health": "/health",
    }


@app.get("/health")
async def health() -> dict:
    """Health check endpoint.

    Returns:
        dict: Health status.
    """
    return {"status": "healthy"}


def main() -> None:
    """Run the application with uvicorn."""
    import uvicorn

    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
    )


if __name__ == "__main__":
    main()
