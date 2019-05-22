class SystemMessage:
    pass


class StartupMessage(SystemMessage):
    def __str__(self):
        return "StartupMessage()"

    def __repr__(self):
        return str(self)


class PoisonPillMessage(SystemMessage):
    def __str__(self):
        return "PoisonPillMessage()"

    def __repr__(self):
        return str(self)
