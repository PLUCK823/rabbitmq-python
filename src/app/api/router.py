"""
API router aggregation.

This module aggregates all API routers into a single router.
"""

from fastapi import APIRouter

from app.api.endpoints import mail

api_router = APIRouter()

# Include all endpoint routers
api_router.include_router(mail.router)
