import random
from .system_messages import StartupMessage
from .system_events import MessageEvent
from .actor import Actor
from .actor_ref import ActorRef
from .atom import Atom, Done
from .mailbox import Mailbox
from .actor_events import (
    ActorEvent, SendEvent, AskEvent, ResponseEvent,
    SpawnEvent, KillEvent,
    StashEvent, UnstashEvent
)


class ActorSystem:
    def __init__(self):
        self.actors_by_names = dict()
        self.actors_count = dict()
        self.executing_actors = dict()
        self.candidates = set()

        self.system_ref = ActorRef('System')

        self._previous_candidate = None

    def spawn(self, actor_class, parent_actor=None, *args, **kwargs):
        class_name = actor_class.__name__

        if actor_class.__name__ not in self.actors_count:
            self.actors_count[class_name] = 1
        else:
            self.actors_count[class_name] += 1

        address = class_name + '-' + str(self.actors_count[class_name])

        parent_address = 'system' if parent_actor is None else parent_actor.address

        actor = actor_class(address=address, parent_address=parent_address, *args, **kwargs)

        mailbox = Mailbox(
            actor_address=address,
            initial_mailbox=[
                MessageEvent(sender=self.system_ref, message=StartupMessage())
            ]
        )

        atom = Atom(actor, mailbox)

        actor_ref = ActorRef(address=address)

        self.actors_by_names[address] = atom

        self.candidates.add(atom)

        return actor_ref

    def kill(self, actor_ref):
        address = actor_ref.address

        if address in self.executing_actors:
            del self.executing_actors[address]

        if address in self.actors_by_names:
            del self.actors_by_names[address]

    def send(self, actor_ref, message, sender_ref=None):
        event = MessageEvent(message=message, sender=sender_ref)

        target = self.actors_by_names[actor_ref.address]

        target.enqueue(event)

        self.candidates.add(target)

    def run(self):
        while len(self.candidates) > 0:
            candidates = self.candidates - {self._previous_candidate}

            if len(candidates) < 1:
                candidates = self.candidates

            candidate = random.choices(list(candidates), k=1)[0]

            while len(candidate.mailbox) == 0:
                self.candidates.remove(candidate)

                candidates = self.candidates - {self._previous_candidate}

                if len(candidates) < 1:
                    candidates = self.candidates

                if len(candidates) < 1:
                    return

                candidate = random.choices(list(candidates), k=1)[0]

            self._previous_candidate = candidate

            generator = candidate.execute()

            actor_event = next(generator)

            while actor_event != Done:
                if isinstance(actor_event, SpawnEvent):
                    actor_ref = self.spawn(
                        actor_class=actor_event.actor_type,
                        parent_actor=actor_event.parent,
                        *actor_event.args,
                        **actor_event.kwargs
                    )

                    actor_event = generator.send(actor_ref)
                    continue

                elif isinstance(actor_event, KillEvent):
                    self.kill(actor_event.target)

                elif isinstance(actor_event, SendEvent):
                    self.send(
                        actor_ref=actor_event.target,
                        message=actor_event.message,
                        sender_ref=actor_event.sender
                    )

                actor_event = next(generator)
