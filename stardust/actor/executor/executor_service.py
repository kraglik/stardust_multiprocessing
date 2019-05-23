import threading
import multiprocessing as mp
import uuid

from concurrent.futures import Future, wait
from .executor_event_manager import IncomingEventManager, OutgoingEventManager, ThreadingQueue
from queue import Queue
from typing import List, Dict, Set, Type, Generator, Optional

from stardust.actor.actor_ref import ActorRef
from stardust.actor.actor import Actor
from stardust.actor.atom import Atom
from stardust.actor.serialized_atom import SerializedAtom
from stardust.actor.mailbox import Mailbox
from stardust.actor.system_messages import StartupMessage
from stardust.actor.system_events import MessageEvent, ActorDeathEvent, SystemEvent
from stardust.actor.pipe import Pipe


class ExecutorService(mp.Process):
    def __init__(self, pipe: Pipe, system_address: str, *args, **kwargs):
        super(ExecutorService, self).__init__(*args, **kwargs)

        self.pipe: Pipe = pipe
        self.atom_by_name: Dict[str, Atom] = dict()
        self.local_addresses: Set[str] = set()
        self.suspended_atoms: Dict[str, Generator] = dict()
        self.system_address: str = system_address

        self.incoming_events: ThreadingQueue = ThreadingQueue()
        self.outgoing_events: ThreadingQueue = ThreadingQueue()

        self.candidates = set()

        self.incoming_event_manager = IncomingEventManager(
            executor_incoming_event_queue=self.pipe.child_input_queue,
            incoming_events_queue=self.incoming_events
        )

        self.outgoing_event_manager = OutgoingEventManager(
            executor_outgoing_event_queue=self.pipe.child_output_queue,
            incoming_event_queue=self.incoming_events,
            outgoing_event_queue=self.outgoing_events,
            local_actors=self.atom_by_name
        )

    def spawn(self, actor_class: Type[Actor], parent: Optional[ActorRef], *args, **kwargs) -> ActorRef:
        address = parent.address + '/' + actor_class.__name__ + '-' + str(uuid.uuid1())

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

    def kill_actor(self, actor_ref: ActorRef):
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
        self.incoming_event_manager.run()
        self.outgoing_event_manager.run()

        while True:
            event: SystemEvent = self.incoming_events.get()

            # TODO: PROCESS EVENT

