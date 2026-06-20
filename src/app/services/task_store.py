"""
Task status store using SQLite.

This module provides async SQLite storage for task status tracking.
It demonstrates how to persist task state across application restarts.
"""

import json
from datetime import datetime
from pathlib import Path
from uuid import UUID, uuid4

import aiosqlite

from app.models.schemas import (
    MailType,
    TaskRecord,
    TaskStatus,
)
from app.core.logging import logger


class TaskStore:
    """Async SQLite-based task status store.

    This class provides persistent storage for task records.
    Uses SQLite for simplicity and demonstration purposes.

    Example:
        ```python
        store = TaskStore("tasks.db")
        await store.init()

        task_id = await store.create_task(
            mail_type=MailType.REGISTER,
            email="user@example.com",
            subject="Welcome",
            content="Hello!"
        )

        task = await store.get_task(task_id)
        await store.update_status(task_id, TaskStatus.SUCCESS)
        ```
    """

    def __init__(self, db_path: str = "data/tasks.db") -> None:
        """Initialize the task store.

        Args:
            db_path: Path to SQLite database file.
        """
        self.db_path = db_path
        self._db: aiosqlite.Connection | None = None

    async def init(self) -> None:
        """Initialize the database and create tables.

        Must be called before any other operations.
        """
        # Ensure directory exists
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)

        self._db = await aiosqlite.connect(self.db_path)

        # Create tasks table
        await self._db.execute("""
            CREATE TABLE IF NOT EXISTS tasks (
                task_id TEXT PRIMARY KEY,
                mail_type TEXT NOT NULL,
                email TEXT NOT NULL,
                subject TEXT NOT NULL,
                content TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                retry_count INTEGER DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                error_message TEXT
            )
        """)
        await self._db.commit()

        logger.info(f"Task store initialized: {self.db_path}")

    async def close(self) -> None:
        """Close the database connection."""
        if self._db:
            await self._db.close()
            logger.info("Task store closed")

    async def create_task(
        self,
        mail_type: MailType,
        email: str,
        subject: str,
        content: str,
    ) -> UUID:
        """Create a new task record.

        Args:
            mail_type: Type of mail.
            email: Recipient email.
            subject: Mail subject.
            content: Mail content.

        Returns:
            UUID: Unique task identifier.
        """
        if not self._db:
            raise RuntimeError("Database not initialized")

        task_id = uuid4()
        now = datetime.utcnow().isoformat()

        await self._db.execute(
            """
            INSERT INTO tasks
            (task_id, mail_type, email, subject, content, status, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(task_id),
                mail_type.value,
                email,
                subject,
                content,
                TaskStatus.PENDING.value,
                now,
                now,
            ),
        )
        await self._db.commit()

        logger.info(f"Created task {task_id} for {email}")
        return task_id

    async def get_task(self, task_id: UUID) -> TaskRecord | None:
        """Get a task by ID.

        Args:
            task_id: Task identifier.

        Returns:
            TaskRecord | None: The task record, or None if not found.
        """
        if not self._db:
            raise RuntimeError("Database not initialized")

        async with self._db.execute(
            "SELECT * FROM tasks WHERE task_id = ?",
            (str(task_id),),
        ) as cursor:
            row = await cursor.fetchone()

        if not row:
            return None

        return self._row_to_record(row)

    async def update_status(
        self,
        task_id: UUID,
        status: TaskStatus,
        error_message: str | None = None,
    ) -> None:
        """Update task status.

        Args:
            task_id: Task identifier.
            status: New status.
            error_message: Optional error message for failed tasks.
        """
        if not self._db:
            raise RuntimeError("Database not initialized")

        now = datetime.utcnow().isoformat()

        await self._db.execute(
            """
            UPDATE tasks
            SET status = ?, updated_at = ?, error_message = ?
            WHERE task_id = ?
            """,
            (status.value, now, error_message, str(task_id)),
        )
        await self._db.commit()

        logger.info(f"Updated task {task_id} to {status.value}")

    async def increment_retry(self, task_id: UUID) -> int:
        """Increment retry count for a task.

        Args:
            task_id: Task identifier.

        Returns:
            int: New retry count.
        """
        if not self._db:
            raise RuntimeError("Database not initialized")

        now = datetime.utcnow().isoformat()

        await self._db.execute(
            """
            UPDATE tasks
            SET retry_count = retry_count + 1, updated_at = ?
            WHERE task_id = ?
            """,
            (now, str(task_id)),
        )
        await self._db.commit()

        # Get new retry count
        task = await self.get_task(task_id)
        return task.retry_count if task else 0

    def _row_to_record(self, row: tuple) -> TaskRecord:
        """Convert database row to TaskRecord.

        Args:
            row: Database row tuple.

        Returns:
            TaskRecord: The task record.
        """
        return TaskRecord(
            task_id=UUID(row[0]),
            mail_type=MailType(row[1]),
            email=row[2],
            subject=row[3],
            content=row[4],
            status=TaskStatus(row[5]),
            retry_count=row[6],
            created_at=datetime.fromisoformat(row[7]),
            updated_at=datetime.fromisoformat(row[8]),
            error_message=row[9],
        )


# Global task store instance
task_store = TaskStore()
