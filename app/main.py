# app/main.py
from fastapi import FastAPI

from platform_common.logging.logging import get_logger
from platform_common.middleware.request_id_middleware import RequestIDMiddleware
from platform_common.middleware.auth_middleware import AuthMiddleware
from platform_common.exception_handling.handlers import add_exception_handlers
from app.api.controller.health_check import router as health_router
from app.services.pubsub_worker import start_subscriber, stop_subscriber

logger = get_logger("video.main")

app = FastAPI(title="ed-video-processing-service")
app.add_middleware(RequestIDMiddleware)
app.add_middleware(AuthMiddleware)
add_exception_handlers(app)


@app.on_event("startup")
async def on_startup() -> None:
    logger.info("Starting ed-video-processing-service...")
    start_subscriber()


@app.on_event("shutdown")
async def on_shutdown() -> None:
    logger.info("Shutting down ed-video-processing-service...")
    stop_subscriber()
