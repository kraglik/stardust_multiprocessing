from dataclasses import dataclass
from .actor_ref import ActorRef
from typing import Any, Type, List, Dict, Tuple, Optional
import stardust.actor


class ActorEvent:
    pass


@dataclass
class SendEvent(ActorEvent):
    sender: ActorRef
    message: Any
    target: ActorRef
    context_code: Optional[int] = None


@dataclass
class AskEvent(ActorEvent):
    sender: ActorRef
    context_code: int
    message: Any
    target: ActorRef


@dataclass
class ResponseEvent(ActorEvent):
    message: Any


@dataclass
class SpawnEvent(ActorEvent):
    parent: ActorRef
    actor_type: Type[stardust.actor.Actor]
    args: Tuple[Any, ...]
    kwargs: dict
    address: str


@dataclass
class KillEvent(ActorEvent):
    sender: ActorRef
    target: ActorRef


class StashEvent(ActorEvent):
    pass


class UnstashEvent(ActorEvent):
    pass
