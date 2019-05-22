from .actor_ref import ActorRef
from typing import Callable, Any, Generator, Union, Type
from .actor_events import (
    ActorEvent, SendEvent, AskEvent, ResponseEvent,
    SpawnEvent, KillEvent,
    StashEvent, UnstashEvent
)


class Actor:
    """

    """

    def __init__(self, address: str, parent_address: str):
        self.__address: str = address
        self.__ref: ActorRef = ActorRef(address)
        self.__parent: ActorRef = ActorRef(parent_address)
        self.__context: int = hash(self.__address)  # I don't care right now
        self.__behavior: Union[Callable[[Actor, Any, ActorRef], None], Generator[ActorEvent, Any, Any]] = self.receive
        self.__previous_behavior = None

    @property
    def address(self) -> str:
        """
        :return: An address of current actor in the actor system.
        """
        return self.__address

    @property
    def ref(self) -> ActorRef:
        """
        :return: ActorRef that points to the current actor.
        """
        return self.__ref

    @property
    def parent(self) -> ActorRef:
        """
        :return: ActorRef that points to an actor that spawned current one.
        """
        return self.__parent

    @property
    def context_code(self) -> int:
        """
        Context code is required in order to make 'ask' pattern possible (or just easy to implement).
        When an actor asks another actor, it appends its context code to a message, and then waits
        for a message with the same context code, that was sent from an actor that was asked earlier.
        Until that moment, all incoming messages except the awaited response, are ignored.
        :return: Context code value.
        """
        return self.__context

    @property
    def behavior(self) -> Union[Callable[['Actor', Any, ActorRef], None], Generator[ActorEvent, Any, Any]]:
        return self.__behavior

    def next_context(self):
        self.__context += 1

    def receive(self, message: Any, sender: ActorRef):
        raise NotImplementedError()

    def send(self, target: ActorRef, message: Any) -> SendEvent:
        return SendEvent(
            target=target,
            sender=self.ref,
            message=message
        )

    def ask(self, target: ActorRef, message: Any) -> AskEvent:
        return AskEvent(
            target=target,
            sender=self.ref,
            message=message,
            context_code=self.__context
        )

    def respond(self, message: Any) -> ResponseEvent:
        return ResponseEvent(
            message=message
        )

    def spawn(self, actor_type: Type, *args, **kwargs) -> SpawnEvent:
        return SpawnEvent(
            parent=self.ref,
            actor_type=actor_type,
            args=args,
            kwargs=kwargs
        )

    def kill(self, actor_ref: ActorRef) -> KillEvent:
        return KillEvent(
            sender=self.ref,
            target=actor_ref
        )

    def become(self, behavior: Union[Callable[['Actor', Any, ActorRef], None], Generator[ActorEvent, Any, Any]]):
        assert callable(behavior), 'Behavior must be callable.'

        self.__previous_behavior = self.__behavior
        self.__behavior = behavior

    def unbecome(self):
        self.__behavior = self.__previous_behavior
        self.__previous_behavior = None

    def stash(self) -> StashEvent:
        return StashEvent()

    def unstash(self) -> UnstashEvent:
        return UnstashEvent()

