from typing import Optional

from .actor import Actor
from .actor_ref import ActorRef
from .mailbox import Mailbox
from .event import Event
from .actor_events import (
    ActorEvent, SendEvent, AskEvent, ResponseEvent,
    SpawnEvent, KillEvent,
    StashEvent, UnstashEvent
)


class Atom:
    def __init__(self, actor: Actor, mailbox: Mailbox):
        self.__actor = actor
        self.__mailbox = mailbox

    @property
    def actor(self) -> Actor:
        return self.__actor

    @property
    def mailbox(self) -> Mailbox:
        return self.__mailbox

    def enqueue(self, event: Event):
        self.__mailbox.enqueue(event)

    def dequeue(self) -> Optional[Event]:
        return self.__mailbox.dequeue()

    def awaits_execution(self) -> bool:
        return len(self.__mailbox) > 0

    def execute(self):
        """
        TODO: IMPLEMENT
        :return: None
        """



