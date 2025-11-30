# app/video_inspect.py
from __future__ import annotations
import asyncio
import json
import subprocess
import tempfile
from typing import Any, Dict, Optional

from google.cloud import storage
from platform_common.logging.logging import get_logger
from platform_common.db.session import get_session
from platform_common.models.file import File
from platform_common.utils.time_helpers import get_current_epoch

from app.services.pubsub_worker import (
    VideoTaskContext,
)  # adjust import

from app.task.registry.video_task_registry import video_task_handler

logger = get_logger("video.inspect")

storage_client = storage.Client()  # reuse across calls


def _download_gcs_object_to_tempfile(bucket: str, object_key: str) -> str:
    """
    Download the GCS object to a local temp file and return the file path.
    Caller is responsible for deleting the file when done.
    """
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".mp4")
    tmp.close()

    blob = storage_client.bucket(bucket).blob(object_key)
    logger.info("Downloading gs://%s/%s to %s", bucket, object_key, tmp.name)
    blob.download_to_filename(tmp.name)

    return tmp.name


def _run_ffprobe(path: str) -> Dict[str, Any]:
    """
    Run ffprobe on the given file path and return parsed JSON.
    """
    cmd = [
        "ffprobe",
        "-v",
        "quiet",
        "-print_format",
        "json",
        "-show_streams",
        "-show_format",
        path,
    ]
    logger.info("Running ffprobe: %s", " ".join(cmd))
    result = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=True,
    )
    return json.loads(result.stdout.decode("utf-8"))


def _extract_video_metadata(ffprobe_result: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize ffprobe JSON into a compact, DB-friendly dict.
    """
    streams = ffprobe_result.get("streams", [])
    format_ = ffprobe_result.get("format", {})

    video_stream = next(
        (s for s in streams if s.get("codec_type") == "video"),
        None,
    )

    if not video_stream:
        raise ValueError("No video stream found in ffprobe output")

    width = video_stream.get("width")
    height = video_stream.get("height")
    codec_name = video_stream.get("codec_name")
    duration = float(format_.get("duration")) if format_.get("duration") else None
    bit_rate = int(format_.get("bit_rate")) if format_.get("bit_rate") else None

    # fps is often "num/den" in r_frame_rate
    r_frame_rate = video_stream.get("r_frame_rate") or "0/0"
    try:
        num_str, den_str = r_frame_rate.split("/")
        num = float(num_str)
        den = float(den_str) if float(den_str) != 0 else 1.0
        fps = num / den if den else None
    except Exception:
        fps = None

    # rotation metadata may be in tags or side_data_list
    rotation: Optional[int] = None
    tags = video_stream.get("tags") or {}
    if "rotate" in tags:
        try:
            rotation = int(tags["rotate"])
        except ValueError:
            rotation = None

    return {
        "width": width,
        "height": height,
        "codec_name": codec_name,
        "duration_seconds": duration,
        "bit_rate": bit_rate,
        "fps": fps,
        "rotation": rotation,
        "container_format": format_.get("format_name"),
    }


async def _save_video_inspect_metadata_async(
    ctx: VideoTaskContext,
    metadata: Dict[str, Any],
) -> None:
    """
    Async bit: uses async get_session and async ORM operations.
    """
    # get_session() is an async generator (FastAPI-style dependency),
    # so we iterate it instead of using "async with".
    async for session in get_session():
        file = await session.get(File, ctx.file_id)
        if not file:
            logger.warning(
                "File not found for file_id=%s; skipping DB update",
                ctx.file_id,
            )
            return

        file.meta = file.meta or {}

        # Raw location (ensure stored)
        raw_meta = file.meta.get("raw") or {}
        raw_meta.setdefault("bucket", ctx.bucket)
        raw_meta.setdefault("object_key", ctx.object_key)
        file.meta["raw"] = raw_meta

        # Video inspect metadata
        video_meta = file.meta.get("video") or {}
        video_meta["inspect"] = {
            **metadata,
            "inspected_at": get_current_epoch(),
        }
        file.meta["video"] = video_meta

        session.add(file)
        await session.commit()
        return  # important: only use the first yielded session


@video_task_handler("video-inspect")
def handle_video_inspect(ctx: VideoTaskContext) -> bool:
    if ctx.media_type != "video":
        logger.warning(
            "video-inspect received non-video media_type=%s ctx=%r",
            ctx.media_type,
            ctx,
        )
        return True

    if not ctx.file_id:
        logger.error("video-inspect missing file_id ctx=%r", ctx)
        return True

    tmp_path = None
    try:
        tmp_path = _download_gcs_object_to_tempfile(ctx.bucket, ctx.object_key)
        ffprobe_result = _run_ffprobe(tmp_path)
        metadata = _extract_video_metadata(ffprobe_result)

        logger.info(
            "video-inspect metadata file_id=%s metadata=%r",
            ctx.file_id,
            metadata,
        )

        # <-- This is the important part: run the async DB helper from sync code.
        asyncio.run(_save_video_inspect_metadata_async(ctx, metadata))

        return True

    except Exception:
        logger.exception(
            "Unexpected error in video-inspect for file_id=%s ctx=%r",
            ctx.file_id,
            ctx,
        )
        return False

    finally:
        if tmp_path:
            import os

            try:
                os.remove(tmp_path)
            except OSError:
                logger.warning("Failed to remove temp file %s", tmp_path)
