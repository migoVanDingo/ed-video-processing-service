# app/pubsub_worker.py
from __future__ import annotations

import os
import threading
from typing import Mapping

from platform_common.gcp.pubsub import (
    PubSubSubscriberConfig,
    BasePubSubSubscriber,
)
from platform_common.logging.logging import get_logger

logger = get_logger("video.pubsub")

_subscriber_thread: threading.Thread | None = None
_subscriber: "VideoProcessingSubscriber" | None = None


class VideoProcessingSubscriber(BasePubSubSubscriber):
    """
    Subscriber for video processing jobs.

    Expects Pub/Sub messages whose JSON payload includes at least:
      - bucket: GCS bucket name
      - name: object name/path in that bucket

    We'll later extend this schema (datastore_id, file_id, job_type, etc.)
    """

    def handle_message(self, payload: dict, attributes: Mapping[str, str]) -> None:
        bucket = payload.get("bucket")
        name = payload.get("name") or payload.get(
            "object"
        )  # cope with GCS-style events
        content_type = payload.get("contentType") or attributes.get("content_type")

        if not bucket or not name:
            logger.error(
                "Received video job without bucket/name",
                payload=payload,
                attributes=dict(attributes),
            )
            # Raise to force nack/retry
            raise ValueError("Missing bucket or name in video job payload")

        logger.info(
            "Received video processing job",
            bucket=bucket,
            name=name,
            content_type=content_type,
            attributes=dict(attributes),
        )

        # 🔜 TODO: wire this to ffprobe pipeline
        # e.g.
        # from app.video.ffprobe_runner import run_ffprobe_for_gcs_object
        #
        # metadata = run_ffprobe_for_gcs_object(bucket=bucket, object_name=name)
        # persist / publish metadata somewhere (DB, Pub/Sub, Redis, etc.)

        # For now, just log and return — BasePubSubSubscriber will ack on success.
        logger.debug(
            "Finished handling video processing job (no-op stub for now)",
            bucket=bucket,
            name=name,
        )


def start_subscriber() -> None:
    """
    Start the Pub/Sub subscriber in a background thread.
    Called from FastAPI startup event.
    """
    global _subscriber_thread, _subscriber

    if _subscriber_thread and _subscriber_thread.is_alive():
        logger.info("VideoProcessingSubscriber already running")
        return

    project_id = os.getenv("GOOGLE_CLOUD_PROJECT_ID") or os.getenv("GCP_PROJECT_ID")
    if not project_id:
        logger.error(
            "Cannot start VideoProcessingSubscriber: "
            "GOOGLE_CLOUD_PROJECT_ID or GCP_PROJECT_ID must be set"
        )
        return

    subscription_id = os.getenv(
        "VIDEO_PROCESSING_SUBSCRIPTION_ID",
        "video-processing-sub",  # default; adjust to your real sub name
    )

    config = PubSubSubscriberConfig(
        subscription_id=subscription_id,
        project_id=project_id,
        ack_on_decode_error=True,
    )

    _subscriber = VideoProcessingSubscriber(config)

    _subscriber_thread = threading.Thread(
        target=_subscriber.run_forever,
        name="video-processing-subscriber",
        daemon=True,
    )
    _subscriber_thread.start()

    logger.info(
        "Started VideoProcessingSubscriber thread",
        project_id=project_id,
        subscription_id=subscription_id,
    )


def stop_subscriber() -> None:
    """
    Stop the Pub/Sub subscriber gracefully.
    Called from FastAPI shutdown event.
    """
    global _subscriber_thread, _subscriber

    if _subscriber is not None:
        logger.info("Stopping VideoProcessingSubscriber...")
        _subscriber.stop()

    if _subscriber_thread and _subscriber_thread.is_alive():
        _subscriber_thread.join(timeout=10)
        logger.info("VideoProcessingSubscriber thread joined")

    _subscriber = None
    _subscriber_thread = None
