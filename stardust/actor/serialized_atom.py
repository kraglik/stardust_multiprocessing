from dataclasses import dataclass


@dataclass
class SerializedAtom:
    address: str
    actor_data: str
    mailbox_data: str

    def __str__(self):
        return f"SerializedAtom(Actor({self.address}))"

    def __repr__(self):
        return str(self)
