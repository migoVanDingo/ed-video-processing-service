# app/pubsub_worker.py
from __future__ import annotations

import base64
import json
import os
import threading
import asyncio
from datetime import datetime, timezone
from typing import Mapping, Tuple, Optional, Callable, Any, Dict

from app.types.video_task_context import VideoTaskContext
from app.task.handler.video_transcode import handle_video_transcode_preview
from app.task.handler.video_inspect import handle_video_inspect

from platform_common.gcp.pubsub import (
    PubSubSubscriberConfig,
    BasePubSubSubscriber,
)
from platform_common.logging.logging import get_logger

# DB bits – adjust paths if yours differ
from platform_common.db.session import get_session
from platform_common.models.file import File

# IMPORTANT: import handlers so their module-level decorators run
import app.task.handler.video_inspect  # noqa: F401
import app.task.handler.video_transcode  # noqa: F401

logger = get_logger("video.pubsub")

_subscriber_thread: threading.Thread | None = None
_subscriber: "VideoProcessingSubscriber" | None = None

VideoTaskHandler = Callable[[VideoTaskContext], bool]
TASK_HANDLERS: dict[str, VideoTaskHandler] = {
    "video-transcode-preview": handle_video_transcode_preview,
    "video-inspect": handle_video_inspect,
}


async def _mark_file_failed_async(
    ctx: VideoTaskContext,
    reason: str | None = None,
) -> None:
    """
    Persist a failure on the File record: status='failed' and a failure meta block.
    """
    async for session in get_session():
        if not ctx.file_id:
            logger.warning("Cannot mark file failed: missing file_id in ctx=%r", ctx)
            return

        file: File | None = await session.get(File, ctx.file_id)
        if not file:
            logger.warning(
                "Cannot mark file failed: File not found for file_id=%s",
                ctx.file_id,
            )
            return

        file.status = "failed"
        file.meta = file.meta or {}

        failure_meta: Dict[str, Any] = file.meta.get("failure") or {}
        if reason:
            failure_meta["reason"] = reason
        failure_meta["updated_at"] = datetime.now(timezone.utc).isoformat()

        file.meta["failure"] = failure_meta

        session.add(file)
        await session.commit()
        return  # only use first yielded session


def _mark_file_failed(ctx: VideoTaskContext, reason: str | None = None) -> None:
    """
    Sync wrapper for _mark_file_failed_async, safe to call from handle_message.
    """
    try:
        asyncio.run(_mark_file_failed_async(ctx, reason))
    except Exception:
        logger.exception("Failed to persist file failure for file_id=%s", ctx.file_id)


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

            ctx = VideoTaskContext(
                bucket=bucket,
                object_key=object_key,
                datastore_id=datastore_id,
                upload_session_id=upload_session_id,
                file_id=file_id,
                media_type=media_type,
                task_type=task_type,
                raw_payload=payload,
                attributes=attributes,
            )

            if not task_type:
                logger.warning(
                    "Video job missing task_type; ignoring. ctx=%r",
                    ctx,
                )
                return True  # nothing more to do

            handler = TASK_HANDLERS.get(task_type)
            if not handler:
                # No handler registered (e.g. optional tasks not implemented yet)
                # Just ACK; do not mark the file as failed.
                return True

            try:
                ok = handler(ctx)
            except Exception as e:
                # Handler blew up: mark file failed and NACK for retry.
                logger.exception(
                    "Unhandled error in handler for task_type=%s file_id=%s",
                    task_type,
                    file_id,
                )
                _mark_file_failed(
                    ctx,
                    reason=f"{type(e).__name__}: {e}",
                )
                return False  # NACK -> Pub/Sub may retry

            if not ok:
                # Handler explicitly signalled failure
                logger.error(
                    "Handler for task_type=%s returned False; marking file failed. ctx=%r",
                    task_type,
                    ctx,
                )
                _mark_file_failed(ctx, reason="handler returned False")
                return False  # NACK -> allow retry if desired

            # Success
            return True

        except Exception:
            logger.exception("Unhandled error in video job handler")
            # We *don't* know which part failed safely enough to mark the file,
            # so just NACK and let retries / DLQ handle it.
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
