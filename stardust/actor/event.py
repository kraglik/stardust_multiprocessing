from dataclasses import dataclass
from .actor_ref import ActorRef
from typing import Any, Optional


@dataclass
class Event:
    sender: ActorRef
    message: Any
    context_code: Optional[int] = None
