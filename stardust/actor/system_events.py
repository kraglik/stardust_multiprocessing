from dataclasses import dataclass
from .actor_ref import ActorRef
from typing import Any, Optional


class SystemEvent:
    pass


@dataclass(frozen=True)
class MessageEvent(SystemEvent):
    sender: ActorRef
    target: ActorRef
    message: Any
    context_code: Optional[int] = None


@dataclass(frozen=True)
class ActorDeathEvent(SystemEvent):
    actor_address: str
