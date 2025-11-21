# app/main.py
from fastapi import FastAPI

from platform_common.logging.logging import get_logger
from app.api.controller.health_check import router as health_router
from app.services.pubsub_worker import start_subscriber, stop_subscriber

logger = get_logger("video.main")

app = FastAPI(title="ed-video-processing-service")


@app.on_event("startup")
async def on_startup() -> None:
    logger.info("Starting ed-video-processing-service...")
    start_subscriber()


@app.on_event("shutdown")
async def on_shutdown() -> None:
    logger.info("Shutting down ed-video-processing-service...")
    stop_subscriber()
