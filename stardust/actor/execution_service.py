import threading
import uuid

from concurrent.futures import Future, wait
from threading import Thread, Event as ThreadingEvent, Condition, Lock, RLock, Barrier, Semaphore
from queue import Queue
from typing import List, Dict, Set, Type, Generator, Optional

from .actor_ref import ActorRef
from .actor import Actor
from .atom import Atom
from .serialized_atom import SerializedAtom
from .mailbox import Mailbox
from .system_messages import StartupMessage
from .system_events import MessageEvent, ActorDeathEvent
from .pipe import Pipe


class ExecutionService(Thread):
    def __init__(self, pipe: Pipe, system_address: str, *args, **kwargs):
        super(ExecutionService, self).__init__(*args, **kwargs)

        self.pipe: Pipe = pipe
        self.atom_by_name: Dict[str, Atom] = dict()
        self.local_addresses: Set[str] = set()
        self.suspended_atoms: Dict[str, Generator] = dict()
        self.system_address: str = system_address

        self.candidates = set()

    def spawn(self, actor_class: Type[Actor], parent: Optional[ActorRef], *args, **kwargs) -> ActorRef:
        address = parent.address + '/' + str(uuid.uuid1())

        actor = actor_class(
            address=address,
            parent_address=self.system_address if not parent else parent.address,
            *args,
            **kwargs
        )
        mailbox = Mailbox(
            actor_address=address,
            initial_mailbox=[
                StartupMessage()
            ]
        )

        atom = Atom(actor=actor, mailbox=mailbox)

        self.atom_by_name[address] = atom
        self.candidates.add(atom)

        return ActorRef(address=address)

    def kill(self, actor_ref: ActorRef):
        if actor_ref.address in self.atom_by_name:
            atom = self.atom_by_name[actor_ref.address]
            self.candidates.remove(atom)

            del self.atom_by_name[actor_ref.address]
            del self.suspended_atoms[actor_ref.address]

            self.pipe.child_output_queue.put(ActorDeathEvent(actor_address=actor_ref.address))

    def send(self, actor_ref: ActorRef, event: MessageEvent):
        if actor_ref.address in self.atom_by_name:
            atom = self.atom_by_name[actor_ref.address]

            atom.enqueue(event)
            self.candidates.add(atom)

        else:
            self.pipe.child_output_queue.put(event)

    def create_temporary_atom(self, actor_address: str) -> Atom:
        # TODO: IMPLEMENT MIGRATIONS AND LOAD-BALANCING
        pass

    def serialize(self, actor_ref: ActorRef) -> Optional[SerializedAtom]:
        # TODO: IMPLEMENT MIGRATIONS AND LOAD-BALANCING
        pass

    def deserialize(self, serialized_atom: SerializedAtom) -> ActorRef:
        # TODO: IMPLEMENT MIGRATIONS AND LOAD-BALANCING
        pass

    def run(self) -> None:
        pass

