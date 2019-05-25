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
    ActorDeathEvent, ExecutionStopped)
from stardust.actor.actor_ref import ActorRef
from stardust.actor.atom import Atom, Done
from stardust.actor.executor.executor_event_manager import ExecutorEventManager

from stardust.actor.pipe import Pipe


class ExecutorService(mp.Process):
    def __init__(self,

                 process_idx: int,
                 pipe: Pipe,
                 system_ref: ActorRef,
                 process_idx_to_queue: Dict[int, mp.Queue],

                 *args, **kwargs):

        super(ExecutorService, self).__init__(*args, **kwargs)

        self.process_idx = process_idx
        self.pipe: Pipe = pipe
        self.system_ref = system_ref
        self.process_idx_to_queue = process_idx_to_queue

        self.actor_to_process: Dict[str, int] = dict()
        self.actor_to_process_lock = threading.Lock()
        self.atom_by_name: Dict[str, Atom] = dict()
        self.local_addresses: Set[str] = set()

        self.suspended_atoms: Dict[str, Generator] = dict()
        self.suspended_atoms_lock = threading.Lock()

        self.candidates = set()
        self.candidates_lock = threading.Lock()

        self.execution_condition = threading.Condition()
        self.running: bool = True

        self.event_manager = ExecutorEventManager(
            process_idx=self.process_idx,
            system_ref=system_ref,
            atom_by_name=self.atom_by_name,
            local_addresses=self.local_addresses,
            candidates=self.candidates,
            candidates_lock=self.candidates_lock,
            suspended_atoms=self.suspended_atoms,
            suspended_atoms_lock=self.suspended_atoms_lock,
            execution_condition=self.execution_condition,
            actor_to_process=self.actor_to_process,
            actor_to_process_lock=self.actor_to_process_lock,
            pipe=self.pipe,
            stop=self.stop
        )

        self._previous_candidate: Optional[Atom] = None

    def stop(self):
        self.running = False

        with self.execution_condition:
            self.execution_condition.notify_all()

        self.pipe.child_output_queue.put(ExecutionStopped())

    def run(self) -> None:

        self.event_manager.start()

        with self.execution_condition:
            while self.running:
                if len(self.candidates) == 0:
                    self.execution_condition.wait()

                if not self.running:
                    break

                self.candidates_lock.acquire()

                candidates = self.candidates - {self._previous_candidate}

                if len(candidates) == 0:
                    candidates = self.candidates

                    if len(candidates) == 0:
                        self.candidates_lock.release()
                        continue

                candidate: Atom = random.sample(candidates, 1)[0]

                if len(candidate.mailbox) < 2:
                    self.candidates.remove(candidate)

                self.candidates_lock.release()

                generator = candidate.execute()

                actor_event: ActorEvent = next(generator)

                while actor_event != Done:
                    if isinstance(actor_event, SendEvent):
                        queue = self.pipe.child_output_queue

                        if actor_event.target.address in self.local_addresses:
                            queue = self.pipe.child_input_queue

                        else:
                            process_idx = None

                            self.actor_to_process_lock.acquire()

                            if actor_event.target.address in self.actor_to_process:
                                process_idx = self.actor_to_process[actor_event.target.address]

                            self.actor_to_process_lock.release()

                            if process_idx:
                                queue = self.process_idx_to_queue[process_idx]

                        queue.put(
                            MessageEvent(
                                sender=actor_event.sender,
                                target=actor_event.target,
                                message=actor_event.message,
                                context_code=actor_event.context_code
                            )
                        )

                        actor_event = next(generator)

                    elif isinstance(actor_event, SpawnEvent):
                        actor_ref = ActorRef(actor_event.address)

                        self.pipe.child_output_queue.put(
                            ActorSpawnEvent(
                                actor_type=actor_event.actor_type,
                                parent_ref=actor_event.parent,
                                address=actor_event.address,
                                args=actor_event.args,
                                kwargs=actor_event.kwargs
                            )
                        )

                        actor_event = generator.send(actor_ref)

                    elif isinstance(actor_event, KillEvent):
                        self.pipe.child_output_queue.put(
                            ActorDeathEvent(
                                actor_ref=actor_event.target,
                                sender=actor_event.sender
                            )
                        )

                        actor_event = next(generator)

        self.finalize()

    def finalize(self):
        self.event_manager.join()



