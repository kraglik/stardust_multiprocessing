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
        self.n = 100_000

    def receive(self, message, sender):

        if isinstance(message, str):
            # print(message)

            if self.n == 0:
                self.kill(self.pong_ref)

                del self.pong_ref

            else:
                yield self.send(self.pong_ref, 'ping')
                self.n -= 1

        elif isinstance(message, system_messages.StartupMessage):
            self.pong_ref = yield self.spawn(Pong)

            # print('ping')
            yield self.send(self.pong_ref, 'ping')
            self.n -= 1


def main():
    system = actor.ActorSystem()
    system.run()

    for i in range(8):
        system.spawn(Ping)

    time.sleep(60)

    system.stop()


if __name__ == '__main__':
    main()


