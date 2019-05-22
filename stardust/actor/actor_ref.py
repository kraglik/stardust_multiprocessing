class ActorRef:
    def __init__(self, address: str):
        self._address = address

    @property
    def address(self):
        return self._address

    def __str__(self):
        return f"ActorRef({self._address})"

    def __repr__(self):
        return str(self)
