from typing import Optional, List
from .system_events import MessageEvent


class Mailbox:
    def __init__(self, actor_address: str, initial_mailbox: Optional[list] = None):
        self.__address = actor_address
        self.__events = []
        self.__events_in_queue = 0

        if initial_mailbox:
            self.__events.extend(initial_mailbox)
            self.__events_in_queue += len(initial_mailbox)

    def enqueue(self, event: MessageEvent):
        self.__events.append(event)
        self.__events_in_queue += 1

    def dequeue(self) -> Optional[MessageEvent]:
        event = None if self.__events_in_queue == 0 else self.__events.pop(0)
        self.__events_in_queue = max(self.__events_in_queue - 1, 0)

        return event

    @property
    def actor_address(self):
        return self.__address

    @property
    def events(self) -> List[MessageEvent]:
        return self.__events

    def __len__(self):
        return self.__events_in_queue

    def __str__(self):
        return f"Mailbox(Actor '{self.__address}')"

    def __repr__(self):
        return str(self)
