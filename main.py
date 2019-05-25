import time

from stardust import actor
from stardust.actor import system_messages


class Pong(actor.Actor):
    def receive(self, message, sender):
        if isinstance(message, str):
            yield self.send(sender, 'pong')


class Ping(actor.Actor):
    def __init__(self, *args, **kwargs):
        super(Ping, self).__init__(*args, **kwargs)
        self.pong_ref = None

    def receive(self, message, sender):

        if isinstance(message, str):
            print(message)

            self.kill(self.pong_ref)

            del self.pong_ref

        elif isinstance(message, system_messages.StartupMessage):
            self.pong_ref = yield self.spawn(Pong)

            print('ping')
            yield self.send(self.pong_ref, 'ping')


def main():
    system = actor.ActorSystem()
    system.run()

    system.spawn(Ping)

    time.sleep(0.25)

    system.stop()


if __name__ == '__main__':
    main()


