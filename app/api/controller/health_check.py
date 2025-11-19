# app/api/controller/health_check.py
from fastapi import APIRouter, Request
from platform_common.logging.logging import get_logger, set_request_context

router = APIRouter()
logger = get_logger("health")


@router.get("/")
async def health_check(request: Request):
    # Optional: bind request-specific info
    set_request_context(
        request_id=str(request.headers.get("x-request-id", "unknown")),
        user_id="test-user",
        session_id="abc123",
    )

    logger.info("Health check successful!", path=request.url.path)

    return {"status": "ok"}
