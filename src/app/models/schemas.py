"""
Pydantic schemas for API request/response models.

This module defines the data models for the Mail Center API.
"""

from datetime import datetime
from enum import StrEnum
from uuid import UUID

from pydantic import BaseModel, EmailStr, Field


class MailType(StrEnum):
    """Supported mail types."""

    REGISTER = "register"
    VERIFY = "verify"
    MARKETING = "marketing"
    SYSTEM = "system"


class TaskStatus(StrEnum):
    """Task processing status."""

    PENDING = "pending"
    PROCESSING = "processing"
    SUCCESS = "success"
    FAILED = "failed"


class MailRequest(BaseModel):
    """Request schema for sending a mail."""

    mail_type: MailType = Field(..., description="Type of mail to send")
    email: EmailStr = Field(..., description="Recipient email address")
    subject: str = Field(..., min_length=1, max_length=200, description="Mail subject")
    content: str = Field(..., min_length=1, description="Mail content")

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "mail_type": "register",
                    "email": "user@example.com",
                    "subject": "Welcome!",
                    "content": "Welcome to our service!",
                }
            ]
        }
    }


class MailTaskResponse(BaseModel):
    """Response schema for mail task submission."""

    task_id: UUID = Field(..., description="Unique task identifier")
    message: str = Field(default="Mail task submitted successfully")


class TaskStatusResponse(BaseModel):
    """Response schema for task status query."""

    task_id: UUID
    status: TaskStatus
    mail_type: MailType | None = None
    email: str | None = None
    retry_count: int = Field(default=0)
    created_at: datetime | None = None
    updated_at: datetime | None = None
    error_message: str | None = None


class TaskRecord(BaseModel):
    """Internal task record for storage."""

    task_id: UUID
    mail_type: MailType
    email: str
    subject: str
    content: str
    status: TaskStatus = TaskStatus.PENDING
    retry_count: int = 0
    created_at: datetime
    updated_at: datetime
    error_message: str | None = None
