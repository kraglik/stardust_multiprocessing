import time
import stardust


class Ping(stardust.Actor):
    def __init__(self, *args, **kwargs):
        super(Ping, self).__init__(*args, **kwargs)
        self.n = 100

    def receive(self, message, sender):
        if isinstance(message, str):
            self.n -= 1

            if self.n % 50 == 0:
                print(self.n)

            if self.n == 0:
                yield self.kill(sender)
                print('done')

            else:
                yield self.send(sender, 'ping')

        elif isinstance(message, stardust.StartupMessage):
            pong = yield self.spawn(Pong)
            yield self.send(pong, 'ping')


class Pong(stardust.Actor):
    def receive(self, message, sender):
        if isinstance(message, str):
            yield self.send(sender, 'pong')


def main():
    system = stardust.ActorSystem(
        name="Ping-Pong",
        config=stardust.SystemConfig(
            num_processes=6
        )
    )
    system.run()

    for _ in range(8):
        system.spawn(Ping)

    time.sleep(15)

    system.stop()


if __name__ == '__main__':
    main()


