class ActorRef:
    def __init__(self, address: str):
        self._address = address

    @property
    def address(self):
        return self._address
