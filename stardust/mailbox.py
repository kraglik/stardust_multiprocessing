from typing import Optional
from .event import Event


class Mailbox:
    def __init__(self, initial_mailbox: Optional[list] = None):
        self.__events = []
        self.__events_in_queue = 0

        if initial_mailbox:
            self.__events.extend(initial_mailbox)
            self.__events_in_queue += len(initial_mailbox)

    def enqueue(self, event: Event):
        self.__events.append(event)
        self.__events_in_queue += 1

    def dequeue(self) -> Optional[Event]:
        return None if self.__events_in_queue == 0 else self.__events.pop(0)

    def __len__(self):
        return self.__events_in_queue
