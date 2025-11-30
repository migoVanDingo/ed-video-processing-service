# app/task/handler/video_transcode.py
from __future__ import annotations

import asyncio
import os
import subprocess
import tempfile
from platform_common.utils.time_helpers import get_current_epoch
from typing import Any, Dict

from google.cloud import storage

from platform_common.db.session import get_session
from platform_common.models.file import File
from platform_common.logging.logging import get_logger

from app.types.video_task_context import VideoTaskContext
from app.task.registry.video_task_registry import video_task_handler

logger = get_logger("video.transcode")
storage_client = storage.Client()


def _download_gcs_to_temp(bucket: str, object_key: str, suffix: str = ".mp4") -> str:
    """
    Download the GCS object to a local temp file and return the file path.
    Caller is responsible for deleting the file.
    """
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
    tmp.close()

    blob = storage_client.bucket(bucket).blob(object_key)
    logger.info("Downloading gs://%s/%s to %s", bucket, object_key, tmp.name)
    blob.download_to_filename(tmp.name)

    return tmp.name


def _upload_temp_to_gcs(local_path: str, bucket: str, object_key: str) -> None:
    """
    Upload a local file to GCS at the given bucket/object_key.
    """
    blob = storage_client.bucket(bucket).blob(object_key)
    logger.info("Uploading %s to gs://%s/%s", local_path, bucket, object_key)
    blob.upload_from_filename(local_path)


def _run_ffmpeg_normalize(src_path: str, dst_path: str) -> None:
    """
    Run ffmpeg to normalize the video into a canonical preview/source format.

    This is a simple example:
      - H.264 video
      - AAC audio
      - Max ~1080p while preserving aspect ratio
    """
    cmd = [
        "ffmpeg",
        "-y",  # overwrite output
        "-i",
        src_path,
        # Example scaling filter: max 1920x1080, preserve aspect ratio
        "-vf",
        "scale='min(1920,iw)':'min(1080,ih)':force_original_aspect_ratio=decrease",
        "-c:v",
        "libx264",
        "-preset",
        "medium",
        "-crf",
        "23",
        "-c:a",
        "aac",
        "-b:a",
        "128k",
        dst_path,
    ]
    logger.info("Running ffmpeg: %s", " ".join(cmd))
    result = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if result.returncode != 0:
        logger.error(
            "ffmpeg failed (code %s): %s",
            result.returncode,
            result.stderr.decode("utf-8") if result.stderr else "",
        )
        raise RuntimeError("ffmpeg normalization failed")


# app/task/handler/video_transcode.py


async def _is_already_normalized(ctx: VideoTaskContext) -> bool:
    """
    Async helper: check if this file is already normalized.
    """
    async for session in get_session():
        file = await session.get(File, ctx.file_id)
        if not file or not file.meta:
            return False

        video_meta: Dict[str, Any] = file.meta.get("video") or {}
        status = video_meta.get("status")
        if status == "normalized":
            logger.info(
                "File file_id=%s already normalized (status=normalized); skipping.",
                ctx.file_id,
            )
            return True

        return False  # we only ever want the first yielded session

    # Fallback (shouldn’t normally be hit)
    return False


async def _update_file_after_transcode_async(
    ctx: VideoTaskContext,
    curated_object_key: str,
    profile: str = "source-1080p",
) -> None:
    """
    Async helper: persist normalized location + status into File.meta
    and promote File.bucket/object_key to curated.
    """
    async for session in get_session():
        file: File | None = await session.get(File, ctx.file_id)
        if not file:
            logger.warning(
                "File not found after transcode for file_id=%s; skipping DB update.",
                ctx.file_id,
            )
            return

        file.meta = file.meta or {}

        # Ensure raw info is stored
        raw_meta: Dict[str, Any] = file.meta.get("raw") or {}
        raw_meta.setdefault("bucket", ctx.bucket)
        raw_meta.setdefault("object_key", ctx.object_key)
        file.meta["raw"] = raw_meta

        # Video normalization metadata
        video_meta: Dict[str, Any] = file.meta.get("video") or {}
        video_meta["normalized"] = {
            "bucket": ctx.bucket,
            "object_key": curated_object_key,
            "profile": profile,
            "completed_at": get_current_epoch(),
        }
        video_meta["status"] = "normalized"
        file.meta["video"] = video_meta
        file.status = "ready"

        # Promote the file to point at curated
        file.bucket = ctx.bucket
        file.object_key = curated_object_key

        session.add(file)
        await session.commit()
        return  # only one session, so we’re done


@video_task_handler("video-transcode-preview")
def handle_video_transcode_preview(ctx: VideoTaskContext) -> bool:
    """
    Normalize the raw video into a curated 'source' asset and promote the File.

    Reads from:
      gs://<bucket>/<raw object_key>

    Writes to:
      gs://<bucket>/curated/datastore/<datastore_id>/file/<file_id>/source/<file_id>-normalized.mp4

    Updates File.meta["raw"], File.meta["video"]["normalized"], and File.bucket/object_key.
    """
    if ctx.media_type != "video":
        logger.warning(
            "video-transcode-preview received non-video media_type=%s ctx=%r",
            ctx.media_type,
            ctx,
        )
        return True

    if not ctx.file_id:
        logger.error("video-transcode-preview missing file_id ctx=%r", ctx)
        return True

    if not ctx.datastore_id:
        logger.error("video-transcode-preview missing datastore_id ctx=%r", ctx)
        return True

    raw_tmp: str | None = None
    normalized_tmp: str | None = None

    try:
        # Optional idempotence check: if already normalized, skip work.
        try:
            if asyncio.run(_is_already_normalized(ctx)):
                return True
        except RuntimeError:
            # If an event loop weirdness happens, log and continue without idempotence.
            logger.exception("Error checking normalization status; continuing anyway.")

        # 1) Download raw file
        raw_tmp = _download_gcs_to_temp(ctx.bucket, ctx.object_key)

        # 2) Allocate temp path for normalized file
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".mp4")
        tmp.close()
        normalized_tmp = tmp.name

        # 3) Run ffmpeg normalization
        _run_ffmpeg_normalize(raw_tmp, normalized_tmp)

        # 4) Compute curated object key
        curated_object_key = (
            f"curated/datastore/{ctx.datastore_id}/file/{ctx.file_id}/source/"
            f"{ctx.file_id}-normalized.mp4"
        )

        # 5) Upload normalized video to curated path
        _upload_temp_to_gcs(normalized_tmp, ctx.bucket, curated_object_key)

        # 6) Update File in DB (meta + bucket/object_key)
        asyncio.run(
            _update_file_after_transcode_async(
                ctx,
                curated_object_key=curated_object_key,
                profile="source-1080p",
            )
        )

        logger.info(
            "video-transcode-preview completed for file_id=%s curated=%s",
            ctx.file_id,
            curated_object_key,
        )
        return True

    except Exception:
        logger.exception(
            "Unexpected error in video-transcode-preview for file_id=%s ctx=%r",
            ctx.file_id,
            ctx,
        )
        # Let the subscriber NACK so this can be retried on transient failures
        return False

    finally:
        # Clean up temp files
        for path in (raw_tmp, normalized_tmp):
            if path:
                try:
                    os.remove(path)
                except OSError:
                    logger.warning("Failed to remove temp file %s", path)
