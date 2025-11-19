from platform_common.pubsub import RedisPublisher, PubSubEvent
from platform_common.utils.enums import EventType
from app.core.config import settings

publisher = RedisPublisher(redis_url=settings.REDIS_URL)


async def publish_task_event(event_type: EventType, payload: dict):
    event = PubSubEvent(event_type=event_type, payload=payload)
    await publisher.publish(topic="tasks", event=event)
