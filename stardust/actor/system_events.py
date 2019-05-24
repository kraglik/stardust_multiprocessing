from dataclasses import dataclass
from .actor_ref import ActorRef
from .actor import Actor
from typing import Any, Optional, Type, Tuple, Dict


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
    actor_ref: ActorRef
    sender: ActorRef


@dataclass(frozen=True)
class ActorSpawnEvent(SystemEvent):
    actor_type: Type[Actor]
    parent_ref: ActorRef
    address: str
    args: Tuple[Any, ...]
    kwargs: Dict[str, Any]


@dataclass(frozen=True)
class ActorSpawnNotificationEvent(SystemEvent):
    address: str
    error: Optional[Exception] = None


@dataclass(frozen=True)
class StopExecution(SystemEvent):
    pass
