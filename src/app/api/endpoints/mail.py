"""
Mail API endpoints.

This module provides REST API endpoints for submitting and querying mail tasks.
"""

from uuid import UUID

from fastapi import APIRouter, HTTPException

from app.models.schemas import (
    MailRequest,
    MailTaskResponse,
    TaskStatusResponse,
    TaskStatus,
)
from app.services.task_store import task_store
from app.rabbitmq.publisher import publish_mail
from app.rabbitmq.exchange import MailType as RabbitMailType
from app.core.logging import logger

router = APIRouter(prefix="/api/mail", tags=["mail"])


@router.post("/send", response_model=MailTaskResponse)
async def send_mail(request: MailRequest) -> MailTaskResponse:
    """Submit a mail task for processing.

    This endpoint:
        1. Creates a task record in the database
        2. Publishes the task to RabbitMQ
        3. Returns the task ID for status tracking

    Args:
        request: Mail request containing type, email, subject, content.

    Returns:
        MailTaskResponse: Contains the task ID.

    Example:
        ```bash
        curl -X POST http://localhost:8000/api/mail/send \\
            -H "Content-Type: application/json" \\
            -d '{
                "mail_type": "register",
                "email": "user@example.com",
                "subject": "Welcome!",
                "content": "Welcome to our service!"
            }'
        ```
    """
    # Create task in database
    task_id = await task_store.create_task(
        mail_type=request.mail_type,
        email=request.email,
        subject=request.subject,
        content=request.content,
    )

    # Update status to processing
    await task_store.update_status(task_id, TaskStatus.PROCESSING)

    # Publish to RabbitMQ
    await publish_mail(
        mail_type=RabbitMailType(request.mail_type.value),
        email=request.email,
        subject=request.subject,
        content=request.content,
        task_id=str(task_id),
    )

    logger.info(f"Submitted mail task {task_id} for {request.email}")

    return MailTaskResponse(task_id=task_id)


@router.get("/task/{task_id}", response_model=TaskStatusResponse)
async def get_task_status(task_id: UUID) -> TaskStatusResponse:
    """Get the status of a mail task.

    Args:
        task_id: Unique task identifier.

    Returns:
        TaskStatusResponse: Current task status and details.

    Raises:
        HTTPException: 404 if task not found.

    Example:
        ```bash
        curl http://localhost:8000/api/mail/task/123e4567-e89b-12d3-a456-426614174000
        ```
    """
    task = await task_store.get_task(task_id)

    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    return TaskStatusResponse(
        task_id=task.task_id,
        status=task.status,
        mail_type=task.mail_type,
        email=task.email,
        retry_count=task.retry_count,
        created_at=task.created_at,
        updated_at=task.updated_at,
        error_message=task.error_message,
    )
