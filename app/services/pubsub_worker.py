# app/pubsub_worker.py
from __future__ import annotations

import os
import threading
import asyncio
from datetime import datetime, timezone
from typing import Mapping, Optional, Callable, Any, Dict, Tuple

from sqlalchemy import select

from app.types.video_task_context import VideoTaskContext
from app.task.handler.video_transcode import handle_video_transcode_preview
from app.task.handler.video_inspect import handle_video_inspect

from platform_common.models.upload_session import UploadSession
from platform_common.gcp.pubsub import PubSubSubscriberConfig, BasePubSubSubscriber
from platform_common.logging.logging import get_logger

from platform_common.db.session import get_session
from platform_common.models.file import File

# IMPORTANT: import handlers so their module-level decorators run (if you use decorators elsewhere)
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


# ──────────────────────────────────────────────────────────────
# Helpers to make JSON/meta updates reliably persist
# (avoids in-place mutation issues with JSON/JSONB columns)
# ──────────────────────────────────────────────────────────────
def _meta_copy(file: File) -> Dict[str, Any]:
    return dict(file.meta or {})


def _nested_copy(meta: Dict[str, Any], key: str) -> Dict[str, Any]:
    val = meta.get(key) or {}
    return dict(val) if isinstance(val, dict) else {}


async def _maybe_update_upload_session_status_async(
    session, upload_session_id: str
) -> None:
    """
    Check all files in this upload session and update the session's status.

    Simple rule:
      - If any file.status == 'failed' -> session.status = 'failed'
      - Else if all files.status == 'ready' and there is at least one file -> session.status = 'ready'
      - Else -> session.status = 'processing'
    """
    result = await session.execute(
        select(File.status).where(File.upload_session_id == upload_session_id)
    )
    statuses = list(result.scalars().all())

    if not statuses:
        return

    if any(s == "failed" for s in statuses):
        new_status = "failed"
    elif all(s == "ready" for s in statuses):
        new_status = "ready"
    else:
        new_status = "processing"

    upload_session: UploadSession | None = await session.get(
        UploadSession, upload_session_id
    )
    if not upload_session:
        logger.warning(
            "UploadSession not found for id=%s when updating status", upload_session_id
        )
        return

    if upload_session.status == new_status:
        return

    upload_session.status = new_status
    session.add(upload_session)
    await session.commit()

    logger.info(
        "UploadSession %s transitioned to status=%s", upload_session_id, new_status
    )


def _mark_file_task_success(ctx: VideoTaskContext) -> None:
    try:
        asyncio.run(_mark_file_task_success_async(ctx))
    except Exception:
        logger.exception(
            "Failed to persist file task success for file_id=%s", ctx.file_id
        )


async def _mark_file_task_success_async(ctx: VideoTaskContext) -> None:
    """
    Mark a specific task for this file as succeeded.
    If all required tasks are done, mark file.status='ready' and maybe update the upload_session status.
    """
    async for session in get_session():
        if not ctx.file_id:
            logger.warning(
                "Cannot mark file task success: missing file_id in ctx=%r", ctx
            )
            return

        file: File | None = await session.get(File, ctx.file_id)
        if not file:
            logger.warning(
                "Cannot mark file task success: File not found for file_id=%s",
                ctx.file_id,
            )
            return

        task_type = ctx.task_type
        if not task_type:
            logger.warning("Cannot mark task success without task_type; ctx=%r", ctx)
            return

        # ✅ Copy → modify → reassign to ensure JSON persistence
        meta = _meta_copy(file)
        tasks = dict((meta.get("tasks") or {}))

        tasks[task_type] = {
            "status": "ready",
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }

        meta["tasks"] = tasks
        file.meta = meta  # <-- critical (top-level reassignment)

        # 2) If all required tasks are ready, mark file.status='ready'
        all_ready = all(
            tasks.get(t, {}).get("status") == "ready" for t in REQUIRED_VIDEO_TASKS
        )
        if all_ready:
            file.status = "ready"

        session.add(file)
        await session.commit()

        # 3) Keep UploadSession in sync
        upload_session_id = file.upload_session_id or ctx.upload_session_id
        if upload_session_id:
            await _maybe_update_upload_session_status_async(session, upload_session_id)

        return


async def _mark_file_failed_async(
    ctx: VideoTaskContext, reason: str | None = None
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
                "Cannot mark file failed: File not found for file_id=%s", ctx.file_id
            )
            return

        file.status = "failed"

        # ✅ Copy → modify → reassign
        meta = _meta_copy(file)
        failure_meta = dict(meta.get("failure") or {})
        if reason:
            failure_meta["reason"] = reason
        failure_meta["updated_at"] = datetime.now(timezone.utc).isoformat()
        meta["failure"] = failure_meta
        file.meta = meta

        session.add(file)
        await session.commit()

        upload_session_id = file.upload_session_id or ctx.upload_session_id
        if upload_session_id:
            await _maybe_update_upload_session_status_async(session, upload_session_id)

        return


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
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Parse datastore_id and upload_session_id from an object key like:

        raw/datastore/{datastore_id}/session/{upload_session_id}/file/{file_id}/{filename}

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
    If the file exists and is still in a pre-terminal state, mark file.status='processing'
    and maybe update the upload_session status.
    """
    async for session in get_session():
        if not ctx.file_id:
            logger.warning(
                "Cannot mark file processing: missing file_id in ctx=%r", ctx
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

            logger.info("File %s transitioned to status=processing", ctx.file_id)

            upload_session_id = file.upload_session_id or ctx.upload_session_id
            if upload_session_id:
                await _maybe_update_upload_session_status_async(
                    session, upload_session_id
                )

        return


def _maybe_mark_file_processing(ctx: VideoTaskContext) -> None:
    """
    Sync wrapper for _maybe_mark_file_processing_async, safe to call from handle_message.
    """
    try:
        asyncio.run(_maybe_mark_file_processing_async(ctx))
    except Exception:
        logger.exception("Failed to mark file processing for file_id=%s", ctx.file_id)


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
        self, payload: dict, attributes: Mapping[str, str] | None = None
    ) -> bool:
        try:
            logger.info(
                "Received Pub/Sub payload=%r attributes=%r", payload, attributes or {}
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
                return True  # ACK broken message so it doesn't loop forever

            task_type = payload.get("task_type")  # ✅ define early

            # ✅ HARD SAFETY: only process raw uploads to prevent Eventarc loops
            # (Curated outputs often trigger object.finalized again.)
            if not object_key.startswith("raw/"):
                logger.info(
                    "Ignoring non-raw object: %s (task=%s)", object_key, task_type
                )
                return True

            parsed_datastore_id, parsed_upload_session_id, _filename = (
                _parse_object_key_metadata(object_key)
            )

            datastore_id = payload.get("datastore_id") or parsed_datastore_id
            upload_session_id = (
                payload.get("upload_session_id") or parsed_upload_session_id
            )
            file_id = payload.get("file_id")
            media_type = payload.get("media_type")

            logger.info(
                "Handling video job: bucket=%s object_key=%s datastore_id=%s upload_session_id=%s "
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
                logger.warning("Video job missing task_type; ignoring. ctx=%r", ctx)
                return True

            handler = TASK_HANDLERS.get(task_type)
            if not handler:
                # Optional tasks not implemented in this service; ACK and move on.
                return True

            # Mark processing (only if we have a file_id)
            _maybe_mark_file_processing(ctx)

            try:
                ok = handler(ctx)
            except Exception as e:
                logger.exception(
                    "Unhandled error in handler for task_type=%s file_id=%s",
                    task_type,
                    file_id,
                )
                _mark_file_failed(ctx, reason=f"{type(e).__name__}: {e}")
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
            return True

        except Exception:
            logger.exception("Unhandled error in video job handler")
            return False  # NACK


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
            "Cannot start VideoProcessingSubscriber: GOOGLE_CLOUD_PROJECT_ID or GCP_PROJECT_ID must be set"
        )
        return

    subscription_id = os.getenv(
        "VIDEO_PROCESSING_SUBSCRIPTION_ID", "video-processing-sub"
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
        "Started VideoProcessingSubscriber thread project_id=%s subscription_id=%s",
        project_id,
        subscription_id,
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
