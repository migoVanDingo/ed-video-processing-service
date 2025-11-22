# app/pubsub_worker.py
from __future__ import annotations

import base64
import json
import os
import threading
from typing import Mapping, Tuple, Optional

from platform_common.gcp.pubsub import (
    PubSubSubscriberConfig,
    BasePubSubSubscriber,
)
from platform_common.logging.logging import get_logger

logger = get_logger("video.pubsub")

_subscriber_thread: threading.Thread | None = None
_subscriber: "VideoProcessingSubscriber" | None = None


def _parse_object_key_metadata(
    object_key: str,
) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Parse datastore_id and upload_session_id from an object key like:

        raw/datastore/{datastore_id}/session/{upload_session_id}/{filename}

    Returns (datastore_id, upload_session_id, filename).
    """
    if not object_key:
        return None, None, None

    parts = object_key.strip("/").split("/")

    datastore_id: Optional[str] = None
    upload_session_id: Optional[str] = None
    filename: Optional[str] = parts[-1] if parts else None

    for i, part in enumerate(parts):
        if part == "datastore" and i + 1 < len(parts):
            datastore_id = parts[i + 1]
        elif part == "session" and i + 1 < len(parts):
            upload_session_id = parts[i + 1]

    return datastore_id, upload_session_id, filename


class VideoProcessingSubscriber(BasePubSubSubscriber):
    """
    Subscriber for video processing jobs.

    BasePubSubSubscriber will already:
      - pull messages
      - base64-decode data
      - json.loads into `payload`

    So here we just accept `payload` as a dict.
    """

    def handle_message(
        self,
        payload: dict,
        attributes: Mapping[str, str] | None = None,
    ) -> bool:
        try:
            logger.info(
                "Received Pub/Sub payload=%r attributes=%r",
                payload,
                attributes or {},
            )

            bucket = (
                payload.get("bucket")
                or payload.get("source_bucket")
                or payload.get("file", {}).get("bucket")
            )

            # accept multiple field names, including "object_key"
            object_key = (
                payload.get("name")
                or payload.get("object_name")
                or payload.get("object_key")
                or payload.get("file", {}).get("name")
            )

            if not bucket or not object_key:
                logger.error(
                    "Received video job without bucket/name attributes=%r payload=%r",
                    attributes or {},
                    payload,
                )
                # ACK so we don't loop this broken message
                return True

            # Parse IDs from the object_key path, if possible
            parsed_datastore_id, parsed_upload_session_id, filename = (
                _parse_object_key_metadata(object_key)
            )

            datastore_id = payload.get("datastore_id") or parsed_datastore_id
            upload_session_id = (
                payload.get("upload_session_id") or parsed_upload_session_id
            )
            file_id = payload.get("file_id")
            media_type = payload.get("media_type")
            task_type = payload.get("task_type")

            logger.info(
                "Handling video job: "
                "bucket=%s object_key=%s datastore_id=%s upload_session_id=%s "
                "file_id=%s media_type=%s task_type=%s",
                bucket,
                object_key,
                datastore_id,
                upload_session_id,
                file_id,
                media_type,
                task_type,
            )

            # TODO: hand off to your actual processing pipeline here

            return True
        except Exception:
            logger.exception("Unhandled error in video job handler")
            # NACK so transient bugs can be retried, but avoid crashing the worker
            return False


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
