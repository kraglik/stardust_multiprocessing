from dataclasses import dataclass

from stardust.actor.system_messages import StartupMessage
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
class ActorLifecycleEvent(SystemEvent):
    pass


@dataclass(frozen=True)
class ActorDeathEvent(ActorLifecycleEvent):
    actor_ref: ActorRef
    sender: ActorRef


@dataclass(frozen=True)
class StartupEvent(ActorLifecycleEvent):
    message = StartupMessage()
    sender: ActorRef


@dataclass(frozen=True)
class ActorSpawnEvent(ActorLifecycleEvent):
    actor_type: Type[Actor]
    parent_ref: ActorRef
    address: str
    args: Tuple[Any, ...]
    kwargs: Dict[str, Any]


@dataclass(frozen=True)
class ActorSpawnNotificationEvent(ActorLifecycleEvent):
    address: str
    error: Optional[Exception] = None


class ActorLocationEvent(SystemEvent):
    pass


@dataclass(frozen=True)
class ActorLocationChanged(ActorLocationEvent):
    actor_ref: ActorRef


@dataclass(frozen=True)
class ActorLocationRequest(ActorLocationEvent):
    actor_ref: ActorRef
    process_idx: int


@dataclass(frozen=True)
class ActorProcessLocationEvent(ActorLocationEvent):
    process_idx: int
    actor_ref: ActorRef


@dataclass(frozen=True)
class ActorNetworkLocationEvent(ActorLocationEvent):
    system_address: str
    actor_ref: ActorRef


@dataclass(frozen=True)
class StopExecution(SystemEvent):
    pass


@dataclass(frozen=True)
class ExecutionStopped(SystemEvent):
    pass


@dataclass(frozen=True)
class StopSystemExecution(StopExecution):
    pass
