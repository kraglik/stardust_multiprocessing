from dataclasses import dataclass


@dataclass
class SerializedAtom:
    address: str
    actor_data: str
    mailbox_data: str
