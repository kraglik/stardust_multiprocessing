from typing import Optional

from stardust.actor import Actor
from .actor_ref import ActorRef
from .mailbox import Mailbox
from .system_events import MessageEvent
from .actor_events import (
    ActorEvent, SendEvent, AskEvent, ResponseEvent,
    SpawnEvent, KillEvent,
    StashEvent, UnstashEvent
)


class Status:
    pass


Done = Status()


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

    def enqueue(self, event: MessageEvent):
        self.__mailbox.enqueue(event)

    def dequeue(self) -> Optional[MessageEvent]:
        return self.__mailbox.dequeue()

    def awaits_execution(self) -> bool:
        return len(self.__mailbox) > 0

    def execute(self):
        event = self.dequeue()

        if event is not None:
            generator = self.actor.behavior(event.message, event.sender)

            if generator is not None:
                try:
                    actor_event = next(generator)

                    while True:
                        if isinstance(actor_event, SpawnEvent):
                            actor_ref = yield actor_event

                            actor_event = generator.send(actor_ref)
                            continue

                        elif isinstance(actor_event, KillEvent)\
                                or isinstance(actor_event, SendEvent):

                            yield actor_event
                            actor_event = next(generator)

                except StopIteration:
                    pass

        yield Done



