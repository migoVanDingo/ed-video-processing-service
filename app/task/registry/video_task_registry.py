# app/task/registry.py
from __future__ import annotations

from typing import Callable, Dict

from app.types.video_task_context import VideoTaskContext

VideoTaskHandler = Callable[[VideoTaskContext], bool]

# Central registry of task_type -> handler
TASK_HANDLERS: Dict[str, VideoTaskHandler] = {}


def video_task_handler(task_type: str):
    """
    Decorator to register a handler for a given task_type.
    Handlers should accept a single VideoTaskContext and return bool.
    """

    def decorator(fn: VideoTaskHandler) -> VideoTaskHandler:
        TASK_HANDLERS[task_type] = fn
        return fn

    return decorator
