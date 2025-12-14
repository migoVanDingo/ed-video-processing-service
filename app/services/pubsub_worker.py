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
from typing import Any, Dict
from sqlalchemy import select, func
from platform_common.models.upload_session import UploadSession  # assuming this exists

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

REQUIRED_VIDEO_TASKS = ["video-inspect", "video-transcode-preview"]


async def _maybe_update_upload_session_status_async(
    session,
    upload_session_id: str,
) -> None:
    """
    Check all files in this upload session and update the session's status.

    Simple rule:
      - If any file.status == 'failed' -> session.status = 'failed'
      - Else if all files.status == 'ready' and there is at least one file
        -> session.status = 'ready'
      - Else -> session.status = 'processing'
    """
    # 1) Get all file statuses for this session
    result = await session.execute(
        select(File.status).where(File.upload_session_id == upload_session_id)
    )
    statuses = list(result.scalars().all())

    if not statuses:
        # No files yet; leave session.status as-is
        return

    if any(s == "failed" for s in statuses):
        new_status = "failed"
    elif all(s == "ready" for s in statuses):
        new_status = "ready"
    else:
        new_status = "processing"

    # 2) Load the UploadSession row
    upload_session: UploadSession | None = await session.get(
        UploadSession,
        upload_session_id,
    )
    if not upload_session:
        logger.warning(
            "UploadSession not found for id=%s when updating status",
            upload_session_id,
        )
        return

    if upload_session.status == new_status:
        return  # nothing changed

    upload_session.status = new_status
    session.add(upload_session)
    await session.commit()

    logger.info(
        "UploadSession %s transitioned to status=%s",
        upload_session_id,
        new_status,
    )

    # 🔹 NEW: trigger dashboard updates when session is done
    if new_status in ("ready", "failed"):
        # import inside function to avoid circular imports
        from app.graphql.dashboard.subscription import publish_datastore_update

        await publish_datastore_update(upload_session.datastore_id)


def _mark_file_task_success(ctx: VideoTaskContext) -> None:
    try:
        asyncio.run(_mark_file_task_success_async(ctx))
    except Exception:
        logger.exception(
            "Failed to persist file task success for file_id=%s",
            ctx.file_id,
        )


async def _mark_file_task_success_async(ctx: VideoTaskContext) -> None:
    """
    Mark a specific task for this file as succeeded.
    If all required tasks are done, mark file.status='ready' and
    maybe update the upload_session status.
    """
    async for session in get_session():
        if not ctx.file_id:
            logger.warning(
                "Cannot mark file task success: missing file_id in ctx=%r",
                ctx,
            )
            return

        file: File | None = await session.get(File, ctx.file_id)
        if not file:
            logger.warning(
                "Cannot mark file task success: File not found for file_id=%s",
                ctx.file_id,
            )
            return

        # 1) Update task status in meta
        file.meta = file.meta or {}
        tasks: Dict[str, Any] = file.meta.get("tasks") or {}

        task_type = ctx.task_type
        if not task_type:
            logger.warning(
                "Cannot mark task success without task_type; ctx=%r",
                ctx,
            )
            return

        tasks[task_type] = {
            "status": "ready",
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        file.meta["tasks"] = tasks

        # 2) If all required tasks are ready, mark file.status='ready'
        all_ready = all(
            tasks.get(t, {}).get("status") == "ready" for t in REQUIRED_VIDEO_TASKS
        )

        if all_ready:
            file.status = "ready"

        session.add(file)
        await session.commit()

        # 3) If file has an upload_session, maybe update its status
        upload_session_id = file.upload_session_id or ctx.upload_session_id
        if upload_session_id:
            await _maybe_update_upload_session_status_async(
                session,
                upload_session_id,
            )

        return  # use only first yielded session


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

        upload_session_id = file.upload_session_id or ctx.upload_session_id
        if upload_session_id:
            await _maybe_update_upload_session_status_async(
                session,
                upload_session_id,
            )

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


async def _maybe_mark_file_processing_async(ctx: VideoTaskContext) -> None:
    """
    If the file exists and is still in a pre-terminal state,
    mark file.status='processing' and maybe update the upload_session status.
    """
    async for session in get_session():
        if not ctx.file_id:
            logger.warning(
                "Cannot mark file processing: missing file_id in ctx=%r",
                ctx,
            )
            return

        file: File | None = await session.get(File, ctx.file_id)
        if not file:
            logger.warning(
                "Cannot mark file processing: File not found for file_id=%s",
                ctx.file_id,
            )
            return

        # Don't downgrade terminal states
        if file.status in ("ready", "failed"):
            return

        if file.status != "processing":
            file.status = "processing"
            session.add(file)
            await session.commit()

            logger.info(
                "File %s transitioned to status=processing",
                ctx.file_id,
            )

            # Keep upload_session in sync
            upload_session_id = file.upload_session_id or ctx.upload_session_id
            if upload_session_id:
                await _maybe_update_upload_session_status_async(
                    session,
                    upload_session_id,
                )

        return  # only use first yielded session


def _maybe_mark_file_processing(ctx: VideoTaskContext) -> None:
    """
    Sync wrapper for _maybe_mark_file_processing_async, safe to call from handle_message.
    """
    try:
        asyncio.run(_maybe_mark_file_processing_async(ctx))
    except Exception:
        logger.exception(
            "Failed to mark file processing for file_id=%s",
            ctx.file_id,
        )


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
                # Just ACK; do not mark the file as failed or processing.
                return True

            # 🔹 NEW: mark file as processing now that we have real work to do
            _maybe_mark_file_processing(ctx)

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
                logger.error(
                    "Handler for task_type=%s returned False; marking file failed. ctx=%r",
                    task_type,
                    ctx,
                )
                _mark_file_failed(ctx, reason="handler returned False")
                return False  # NACK

            # Success: mark this task as done and maybe update session
            _mark_file_task_success(ctx)

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
