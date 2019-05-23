import threading

from queue import Queue as ThreadingQueue
from multiprocessing import Queue as InterprocessQueue
from typing import Dict

from stardust.actor.atom import Atom
from stardust.actor.system_events import SystemEvent, MessageEvent


class IncomingEventManager(threading.Thread):
    def __init__(self,
                 executor_incoming_event_queue: ThreadingQueue,
                 incoming_events_queue: InterprocessQueue,
                 *args, **kwargs):

        super(IncomingEventManager, self).__init__(*args, **kwargs)

        self.executor_incoming_event_queue = executor_incoming_event_queue
        self.incoming_events_queue = incoming_events_queue

    def run(self) -> None:

        while True:
            event = self.executor_incoming_event_queue.get()
            self.incoming_events_queue.put(event)


class OutgoingEventManager(threading.Thread):
    def __init__(self,
                 executor_outgoing_event_queue: ThreadingQueue,
                 outgoing_event_queue: InterprocessQueue,
                 incoming_event_queue: InterprocessQueue,
                 local_actors: Dict[str, Atom],
                 *args, **kwargs):

        super(OutgoingEventManager, self).__init__(*args, **kwargs)

        self.executor_outgoing_event_queue = executor_outgoing_event_queue
        self.outgoing_event_queue = outgoing_event_queue
        self.incoming_event_queue = incoming_event_queue

        self.local_actors = local_actors

    def run(self) -> None:

        while True:
            event: SystemEvent = self.outgoing_event_queue.get()

            if isinstance(event, MessageEvent) and event.target.address in self.local_actors:
                self.incoming_event_queue.put(event)

            else:
                self.executor_outgoing_event_queue.put(event)

