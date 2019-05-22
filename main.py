from stardust import actor
from stardust.actor import system_messages


class Pong(actor.Actor):
    def receive(self, message, sender):
        if isinstance(message, str):
            yield self.send(sender, 'pong')


class Ping(actor.Actor):
    def receive(self, message, sender):

        if isinstance(message, str):
            print(message)
            self.kill(self.ponger)
            del self.ponger

        elif isinstance(message, system_messages.StartupMessage):
            self.ponger = yield self.spawn(Pong)

            print('ping')
            yield self.send(self.ponger, 'ping')


def main():
    system = actor.ActorSystem()

    system.spawn(Ping)

    system.run()


if __name__ == '__main__':
    main()



