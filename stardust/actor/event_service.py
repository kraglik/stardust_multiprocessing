from concurrent.futures import Future, wait
import threading
from threading import Thread, Event, Condition, Lock, RLock, Barrier, Semaphore
from queue import Queue


class EventService(Thread):
    def __init__(self, *args, **kwargs):
        super(EventService, self).__init__(*args, **kwargs)

    def run(self) -> None:
        pass
