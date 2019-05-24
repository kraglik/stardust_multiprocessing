import threading

from queue import Queue as ThreadingQueue
from multiprocessing import Queue as InterprocessQueue
from typing import Dict, Set, Generator, Type, Optional, Callable

from stardust.actor import ActorRef
from stardust.actor.actor import Actor
from stardust.actor.atom import Atom
from stardust.actor.pipe import Pipe
from stardust.actor.system_events import (
    SystemEvent,
    MessageEvent,
    ActorSpawnEvent, ActorSpawnNotificationEvent, ActorDeathEvent,
    StopExecution)
from stardust.actor.mailbox import Mailbox
from stardust.actor.system_messages import StartupMessage


class ExecutorEventManager(threading.Thread):
    def __init__(self,
                 system_ref: ActorRef,
                 atom_by_name: Dict[str, Atom],
                 local_addresses: Set[str],
                 candidates: Set[Atom],
                 candidates_lock: threading.Lock,
                 suspended_atoms: Dict[str, Generator],
                 suspended_atoms_lock: threading.Lock,
                 execution_condition: threading.Condition,
                 pipe: Pipe,
                 stop: Callable[[], None],
                 *args, **kwargs):

        super(ExecutorEventManager, self).__init__(*args, **kwargs)

        self.system_ref = system_ref
        self.atom_by_name = atom_by_name
        self.local_addresses = local_addresses

        self.pipe = pipe

        self.candidates = candidates
        self.candidates_lock = candidates_lock

        self.suspended_atoms = suspended_atoms
        self.suspended_atoms_lock = suspended_atoms_lock

        self.execution_condition = execution_condition

        self.stop = stop

    def spawn(self, event: ActorSpawnEvent) -> None:

        exception = None

        try:

            actor: Actor = event.actor_type(
                address=event.address,
                parent=self.system_ref if not event.parent_ref else event.parent_ref,
                *event.args,
                **event.kwargs
            )
            mailbox = Mailbox(
                actor_address=event.address,
                initial_mailbox=[
                    StartupMessage()
                ]
            )

            atom = Atom(actor=actor, mailbox=mailbox)

        except Exception as e:
            exception = e

        else:
            self.atom_by_name[event.address] = atom
            self.candidates.add(atom)

            self.execution_condition.notify_all()

        self.pipe.child_output_queue.put(
            ActorSpawnNotificationEvent(
                address=event.address,
                error=exception
            )
        )

    def kill_actor(self, event: ActorDeathEvent):
        # TODO: WRITE SOFT KILL METHOD (like PoisonPill in Akka)
        if event.actor_ref.address in self.atom_by_name:
            atom = self.atom_by_name[event.actor_ref.address]

            self.candidates_lock.acquire()
            self.candidates.remove(atom)
            self.candidates_lock.release()

            del self.atom_by_name[event.actor_ref.address]
            del self.suspended_atoms[event.actor_ref.address]

    def send(self, event: MessageEvent):
        actor_ref = event.target

        if actor_ref.address in self.atom_by_name:
            atom = self.atom_by_name[actor_ref.address]
            atom.enqueue(event)

            self.candidates_lock.acquire()
            self.candidates.add(atom)
            self.candidates_lock.release()

            self.execution_condition.notify_all()

    def run(self) -> None:

        while True:
            event: SystemEvent = self.pipe.child_input_queue.get()

            if isinstance(event, ActorSpawnEvent):
                self.spawn(event)

            elif isinstance(event, ActorDeathEvent):
                self.kill_actor(event)

            elif isinstance(event, MessageEvent):
                self.send(event)

            elif isinstance(event, StopExecution):
                break

        self.stop()

