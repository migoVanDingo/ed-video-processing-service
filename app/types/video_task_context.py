from dataclasses import dataclass
from typing import Mapping


@dataclass
class VideoTaskContext:
    bucket: str
    object_key: str
    datastore_id: str | None
    upload_session_id: str | None
    file_id: str | None
    media_type: str | None
    task_type: str | None
    raw_payload: dict
    attributes: Mapping[str, str] | None = None
