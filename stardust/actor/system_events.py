from dataclasses import dataclass
from .actor_ref import ActorRef
from typing import Any, Optional


class SystemEvent:
    pass


@dataclass
class MessageEvent(SystemEvent):
    sender: ActorRef
    message: Any
    context_code: Optional[int] = None


@dataclass
class ActorDeathEvent(SystemEvent):
    actor_address: str
