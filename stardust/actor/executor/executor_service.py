import random
import threading
import multiprocessing as mp
from typing import Dict, Set, Generator, Optional

from stardust.actor.actor_events import (
    ActorEvent,
    SendEvent, AskEvent,
    SpawnEvent, KillEvent
)
from stardust.actor.system_events import (
    SystemEvent,
    MessageEvent,
    ActorSpawnEvent,
    ActorDeathEvent)
from stardust.actor.actor_ref import ActorRef
from stardust.actor.atom import Atom, Done
from stardust.actor.executor.executor_event_manager import ExecutorEventManager

from stardust.actor.pipe import Pipe


class ExecutorService(mp.Process):
    def __init__(self, pipe: Pipe, system_ref: ActorRef, *args, **kwargs):
        super(ExecutorService, self).__init__(*args, **kwargs)

        self.pipe: Pipe = pipe
        self.system_ref = system_ref

        self.atom_by_name: Dict[str, Atom] = dict()
        self.local_addresses: Set[str] = set()

        self.suspended_atoms: Dict[str, Generator] = dict()
        self.suspended_atoms_lock = threading.Lock()

        self.candidates = set()
        self.candidates_lock = threading.Lock()

        self.execution_condition = threading.Condition()
        self.running: bool = True

        def stop():
            self.running = False

        self.event_manager = ExecutorEventManager(
            system_ref=system_ref,
            atom_by_name=self.atom_by_name,
            local_addresses=self.local_addresses,
            candidates=self.candidates,
            candidates_lock=self.candidates_lock,
            suspended_atoms=self.suspended_atoms,
            suspended_atoms_lock=self.suspended_atoms_lock,
            execution_condition=self.execution_condition,
            pipe=self.pipe,
            stop=stop
        )

        self._previous_candidate: Optional[Atom] = None

    def run(self) -> None:

        self.event_manager.run()

        with self.execution_condition:
            while True:
                if len(self.candidates) == 0:
                    self.execution_condition.wait()

                if not self.running:
                    break

                self.candidates_lock.acquire()

                candidates = self.candidates - {self._previous_candidate if len(self.candidates) > 1 else None}
                candidate: Atom = random.choices(candidates, k=1)[0]
                self.candidates.remove(candidate)

                self.candidates_lock.release()

                generator = candidate.execute()

                actor_event: ActorEvent = next(generator)

                while actor_event != Done:
                    if isinstance(actor_event, SendEvent):
                        self.pipe.child_output_queue.put_nowait(
                            MessageEvent(
                                sender=actor_event.sender,
                                target=actor_event.target,
                                message=actor_event.message,
                                context_code=actor_event.context_code
                            )
                        )

                    elif isinstance(actor_event, SpawnEvent):
                        actor_ref = ActorRef(actor_event.address)

                        self.pipe.child_output_queue.put_nowait(
                            ActorSpawnEvent(
                                actor_type=actor_event.actor_type,
                                parent_ref=actor_event.parent,
                                address=actor_event.address,
                                args=actor_event.args,
                                kwargs=actor_event.kwargs
                            )
                        )

                        generator.send(actor_ref)

                    elif isinstance(actor_event, KillEvent):
                        self.pipe.child_output_queue.put_nowait(
                            ActorDeathEvent(
                                actor_ref=actor_event.target,
                                sender=actor_event.sender
                            )
                        )

        self.finalize()

    def finalize(self):
        self.event_manager.join()



