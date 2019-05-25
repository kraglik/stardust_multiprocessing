import random
import threading
import multiprocessing as mp
from typing import Dict, Callable, List, Optional

from .pipe import Pipe
import uuid
from queue import Queue as ThreadingQueue
from .system_events import (
    MessageEvent,
    ExecutionStopped, StopSystemExecution, StopExecution,
    ActorLifecycleEvent, ActorSpawnEvent, ActorDeathEvent , ActorSpawnNotificationEvent
)
from .actor import Actor
from .actor_ref import ActorRef
from .executor.executor_service import ExecutorService


class IncomingEventManager(threading.Thread):
    def __init__(self,

                 incoming_queue: mp.Queue,

                 message_event_queue: ThreadingQueue,
                 message_event_queue_lock: threading.Lock,

                 system_event_queue: ThreadingQueue,
                 system_event_queue_lock: threading.Lock,

                 running: Callable[[], bool],

                 *args, **kwargs):

        super(IncomingEventManager, self).__init__(*args, **kwargs)

        self.incoming_queue = incoming_queue

        self.message_event_queue = message_event_queue
        self.message_event_queue_lock = message_event_queue_lock

        self.system_event_queue = system_event_queue
        self.system_event_queue_lock = system_event_queue_lock

        self.running = running

    def run(self) -> None:
        while self.running():
            event = self.incoming_queue.get()

            if isinstance(event, MessageEvent):
                # ------------------------------------------------------------------------------------------------------
                self.message_event_queue_lock.acquire()
                # ======================================================================================================

                self.message_event_queue.put(event)

                # ======================================================================================================
                self.message_event_queue_lock.release()
                # ------------------------------------------------------------------------------------------------------

            elif isinstance(event, ExecutionStopped):
                break

            else:
                # ------------------------------------------------------------------------------------------------------
                self.system_event_queue_lock.acquire()
                # ======================================================================================================

                self.system_event_queue.put(event)

                # ======================================================================================================
                self.system_event_queue_lock.release()
                # ------------------------------------------------------------------------------------------------------


class OutgoingEventManager(threading.Thread):
    def __init__(self,

                 message_event_queue: ThreadingQueue,

                 actor_to_process: Dict[str, int],
                 actor_to_process_lock: threading.Lock,

                 process_to_queue: Dict[int, mp.Queue],

                 message_cache: Dict[str, List[MessageEvent]],
                 message_cache_lock: threading.Lock,

                 running: Callable[[], bool],

                 *args, **kwargs):

        super(OutgoingEventManager, self).__init__(*args, **kwargs)

        self.message_event_queue = message_event_queue

        self.actor_to_process = actor_to_process
        self.actor_to_process_lock = actor_to_process_lock

        self.process_to_queue = process_to_queue

        self.message_cache = message_cache
        self.message_cache_lock = message_cache_lock

        self.running = running

    def run(self) -> None:
        while self.running():
            event = self.message_event_queue.get()

            if isinstance(event, MessageEvent):
                cached = False

                # ------------------------------------------------------------------------------------------------------
                self.message_cache_lock.acquire()
                # ======================================================================================================

                if event.target.address in self.message_cache:
                    self.message_cache[event.target.address].append(event)
                    cached = True

                # ======================================================================================================
                self.message_cache_lock.release()
                # ------------------------------------------------------------------------------------------------------

                if not cached:
                    # --------------------------------------------------------------------------------------------------
                    self.actor_to_process_lock.acquire()
                    # ==================================================================================================

                    if event.target.address in self.actor_to_process:

                        process_idx = self.actor_to_process[event.target.address]
                        self.process_to_queue[process_idx].put(event)

                    else:
                        # ----------------------------------------------------------------------------------------------
                        self.message_cache_lock.acquire()
                        # ==============================================================================================

                        if event.target.address not in self.message_cache:
                            self.message_cache[event.target.address] = []

                        self.message_cache[event.target.address].append(event)

                        # ==============================================================================================
                        self.message_cache_lock.release()
                        # ----------------------------------------------------------------------------------------------

                    # ==================================================================================================
                    self.actor_to_process_lock.release()
                    # --------------------------------------------------------------------------------------------------

            elif isinstance(event, StopSystemExecution):
                break


class SystemEventManager(threading.Thread):
    def __init__(self,

                 actor_to_process: Dict[str, int],
                 actor_to_process_lock: threading.Lock,

                 process_to_pipe: Dict[int, Pipe],

                 system_event_queue: ThreadingQueue,
                 system_event_queue_lock: threading.Lock,

                 message_event_queue: ThreadingQueue,
                 message_event_queue_lock: threading.Lock,

                 message_cache: Dict[str, List[MessageEvent]],
                 message_cache_lock: threading.Lock,

                 running: Callable[[], bool],

                 *args, **kwargs):

        super(SystemEventManager, self).__init__(*args, **kwargs)

        self.actor_to_process = actor_to_process
        self.actor_to_process_lock = actor_to_process_lock

        self.process_actors_count: Dict[int, Dict[str, int]] = {
            process_idx: dict() for process_idx in process_to_pipe.keys()
        }

        self.process_to_pipe = process_to_pipe

        self.system_event_queue = system_event_queue
        self.system_event_queue_lock = system_event_queue_lock

        self.message_event_queue = message_event_queue
        self.message_event_queue_lock = message_event_queue_lock

        self.message_cache = message_cache
        self.message_cache_lock = message_cache_lock

        self.running = running

    def schedule_spawn(self, event: ActorSpawnEvent):
        # --------------------------------------------------------------------------------------------------------------
        self.message_cache_lock.acquire()
        # ==============================================================================================================

        if event.address not in self.message_cache:
            self.message_cache[event.address] = []

        # ==============================================================================================================
        self.message_cache_lock.release()
        # --------------------------------------------------------------------------------------------------------------

        actor_typename = event.actor_type.__name__

        counts = []

        for process_idx in self.process_actors_count.keys():
            if actor_typename not in self.process_actors_count[process_idx]:
                self.process_actors_count[process_idx][actor_typename] = 0

            counts.append(self.process_actors_count[process_idx][actor_typename])

        target_process_idx = counts.index(min(counts))

        self.process_actors_count[target_process_idx][actor_typename] += 1

        # --------------------------------------------------------------------------------------------------------------
        self.actor_to_process_lock.acquire()
        # ==============================================================================================================

        self.actor_to_process[event.address] = target_process_idx

        # ==============================================================================================================
        self.actor_to_process_lock.release()
        # --------------------------------------------------------------------------------------------------------------

        self.process_to_pipe[target_process_idx].parent_output_queue.put(event)

    def flush_cache(self, event: ActorSpawnNotificationEvent):
        # --------------------------------------------------------------------------------------------------------------
        self.message_cache_lock.acquire()
        # ==============================================================================================================

        messages = self.message_cache[event.address]
        del self.message_cache[event.address]

        # ==============================================================================================================
        self.message_cache_lock.release()
        # --------------------------------------------------------------------------------------------------------------

        # --------------------------------------------------------------------------------------------------------------
        self.actor_to_process_lock.acquire()
        # ==============================================================================================================

        process_idx = self.actor_to_process[event.address]

        # ==============================================================================================================
        self.actor_to_process_lock.release()
        # --------------------------------------------------------------------------------------------------------------

        queue = self.process_to_pipe[process_idx].parent_output_queue

        for message in messages:
            queue.put(message)

    def schedule_kill(self, event: ActorDeathEvent):
        self.actor_to_process_lock.acquire()

        if event.actor_ref.address in self.actor_to_process:
            process_idx = self.actor_to_process[event.actor_ref.address]
            queue = self.process_to_pipe[process_idx].parent_output_queue

            queue.put(event)

        self.actor_to_process_lock.release()

    def run(self) -> None:

        while self.running():
            event = self.system_event_queue.get()

            if isinstance(event, StopSystemExecution):
                break

            elif isinstance(event, ActorLifecycleEvent):
                if isinstance(event, ActorSpawnEvent):
                    self.schedule_spawn(event)

                elif isinstance(event, ActorDeathEvent):
                    self.schedule_kill(event)

                elif isinstance(event, ActorSpawnNotificationEvent):
                    self.flush_cache(event)

            else:
                # TODO: IMPLEMENT
                pass

        for process_queue in self.process_to_pipe.values():
            process_queue.parent_output_queue.put(StopExecution())


class ActorSystem:
    def __init__(self, name=None):
        self.running = True

        self.actor_to_process: Dict[str, int] = dict()
        self.actor_to_process_lock = threading.Lock()

        # ActorAddress -> [Event]. Used in case when actor is not yet started, but has already received some messages.
        self.message_cache: Dict[str, List[MessageEvent]] = dict()
        self.message_cache_lock = threading.Lock()

        self.system_ref = ActorRef(name or f"System-{uuid.uuid1()}")

        self.message_event_queue: ThreadingQueue = ThreadingQueue()
        self.message_event_queue_lock = threading.Lock()

        self.system_event_queue: ThreadingQueue = ThreadingQueue()
        self.system_event_queue_lock = threading.Lock()

        self.process_to_pipe: Dict[int, Pipe] = {
            process_idx: Pipe()
            for process_idx in range(mp.cpu_count() * 2)
        }

        self.process_to_queue = {
            process_idx: pipe.child_input_queue
            for process_idx, pipe in self.process_to_pipe.items()
        }

        self.workers = [
            ExecutorService(
                pipe=pipe,
                system_ref=self.system_ref
            )
            for pipe in self.process_to_pipe.values()
        ]

        self.incoming_events_managers = [
            IncomingEventManager(
                incoming_queue=pipe.child_output_queue,
                message_event_queue=self.message_event_queue,
                message_event_queue_lock=self.message_event_queue_lock,
                system_event_queue=self.system_event_queue,
                system_event_queue_lock=self.system_event_queue_lock,
                running=lambda: self.running
            )
            for pipe in self.process_to_pipe.values()
        ]

        self.outgoing_event_manager = OutgoingEventManager(
            actor_to_process=self.actor_to_process,
            actor_to_process_lock=self.actor_to_process_lock,
            message_event_queue=self.message_event_queue,
            process_to_queue=self.process_to_queue,
            message_cache=self.message_cache,
            message_cache_lock=self.message_cache_lock,
            running=lambda: self.running
        )

        self.system_event_manager = SystemEventManager(
            actor_to_process=self.actor_to_process,
            actor_to_process_lock=self.actor_to_process_lock,
            process_to_pipe=self.process_to_pipe,
            message_cache=self.message_cache,
            message_cache_lock=self.message_cache_lock,
            message_event_queue=self.message_event_queue,
            message_event_queue_lock=self.message_event_queue_lock,
            system_event_queue=self.system_event_queue,
            system_event_queue_lock=self.system_event_queue_lock,
            running=lambda: self.running
        )

    def spawn(self, actor_class, name=None, *args, **kwargs):
        address = name or f"{self.system_ref.address}/{actor_class.__name__}-{uuid.uuid1()}"

        actor_spawn_event = ActorSpawnEvent(
            actor_type=actor_class,
            address=address,
            parent_ref=self.system_ref,
            args=args,
            kwargs=kwargs
        )

        self.system_event_queue.put(actor_spawn_event)

        return ActorRef(address)

    def kill(self, actor_ref):

        actor_death_event = ActorDeathEvent(
            actor_ref=actor_ref,
            sender=self.system_ref
        )

        self.system_event_queue.put(actor_death_event)

    def send(self, actor_ref, message):
        self.message_event_queue_lock.acquire()

        self.message_event_queue.put(
            MessageEvent(
                sender=self.system_ref,
                target=actor_ref,
                message=message
            )
        )

        self.message_event_queue_lock.release()

    def run(self):
        for worker in self.workers:
            worker.start()

        for incoming_events_manager in self.incoming_events_managers:
            incoming_events_manager.start()

        self.outgoing_event_manager.start()
        self.system_event_manager.start()

    def stop(self):
        # --------------------------------------------------------------------------------------------------------------
        self.system_event_queue_lock.acquire()
        # ==============================================================================================================

        self.system_event_queue.put(StopSystemExecution())

        # ==============================================================================================================
        self.system_event_queue_lock.release()
        # --------------------------------------------------------------------------------------------------------------

        for worker in self.workers:
            worker.join()

        for incoming_events_manager in self.incoming_events_managers:
            incoming_events_manager.join()

        self.system_event_manager.join()

        self.message_event_queue.put(StopSystemExecution())
        self.outgoing_event_manager.join()
