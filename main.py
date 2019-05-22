import random
import inspect


class ActorRef:
    def __init__(self, address: str):
        self._address = address

    @property
    def address(self):
        return self._address


class Actor:
    def __init__(self, address, parent_address):
        self.__address = address
        self.__parent_address = parent_address

    @property
    def address(self):
        return self.__address

    @property
    def parent(self):
        return ActorRef(self.__parent_address)

    def receive(self, message, sender):
        raise NotImplementedError()


class Pong(Actor):
    def receive(self, message, sender):
        if isinstance(message, str):
            yield ('send', sender, 'pong')


class Ping(Actor):
    def receive(self, message, sender):

        if isinstance(message, str):
            print(message)
            yield ('kill', self.ponger)

        elif isinstance(message, StartupMessage):
            self.ponger = yield ('create', Pong)

            print('ping')
            yield ('send', self.ponger, 'ping')


class Event:
    def __init__(self, message, sender):
        self._message = message
        self._sender = sender

    @property
    def message(self):
        return self._message

    @property
    def sender(self):
        return self._sender


class StartupMessage:
    pass


class Atom:
    def __init__(self, actor: Actor, mailbox: list):
        self.mailbox = mailbox
        self.actor = actor

    def enqueue(self, event):
        self.mailbox.append(event)

    def dequeue(self):
        return None if len(self.mailbox) < 1 else self.mailbox.pop(0)

    def execute(self):

        event = self.dequeue()

        if event is not None:
            if inspect.isgeneratorfunction(self.actor.receive):
                generator = self.actor.receive(event.message, event.sender)

                try:
                    actor_event = next(generator)

                    while True:
                        if actor_event[0] == 'create':
                            actor_ref = yield (*actor_event, ActorRef(self.actor.address))

                            actor_event = generator.send(actor_ref)
                            continue

                        elif actor_event[0] == 'kill':
                            yield actor_event
                            actor_event = next(generator)

                        elif actor_event[0] == 'send':
                            yield (*actor_event, ActorRef(self.actor.address))
                            actor_event = next(generator)

                except StopIteration:
                    pass

        yield 'done'


class ActorSystem:
    def __init__(self):
        self.actors_by_names = dict()
        self.actors_count = dict()
        self.executing_actors = dict()
        self.candidates = set()

        self._previous_candidate = None

    def spawn(self, actor_class, parent_actor=None, *args, **kwargs):
        class_name = actor_class.__name__

        if actor_class.__name__ not in self.actors_count:
            address = class_name + '-' + '1'
            self.actors_count[class_name] = 1

        else:
            self.actors_count[class_name] += 1

            address = class_name + '-' + str(self.actors_count[class_name])

        parent_address = 'system' if parent_actor is None else parent_actor.address

        actor = actor_class(address=address, parent_address=parent_address, *args, **kwargs)
        atom = Atom(actor, [Event(sender=None, message=StartupMessage())])

        actor_ref = ActorRef(address=address)

        self.actors_by_names[address] = atom

        self.candidates.add(atom)

        return actor_ref

    def kill(self, actor_ref):
        address = actor_ref.address

        if address in self.executing_actors:
            del self.executing_actors[address]

        if address in self.actors_by_names:
            del self.actors_by_names[address]

    def send(self, actor_ref, message, sender_ref=None):
        event = Event(message=message, sender=sender_ref)

        target = self.actors_by_names[actor_ref.address]

        target.enqueue(event)

        self.candidates.add(target)

    def run(self):
        while len(self.candidates) > 0:
            candidates = self.candidates - {self._previous_candidate}

            if len(candidates) < 1:
                candidates = self.candidates

            candidate = random.choices(list(candidates), k=1)[0]

            while len(candidate.mailbox) == 0:
                self.candidates.remove(candidate)

                candidates = self.candidates - {self._previous_candidate}

                if len(candidates) < 1:
                    candidates = self.candidates

                if len(candidates) < 1:
                    return

                candidate = random.choices(list(candidates), k=1)[0]

            self._previous_candidate = candidate

            generator = candidate.execute()

            actor_event = next(generator)

            while actor_event != 'done':
                if actor_event[0] == 'create':
                    actor_ref = self.spawn(
                        actor_class=actor_event[1],
                        parent_actor=actor_event[2]
                    )

                    actor_event = generator.send(actor_ref)
                    continue

                elif actor_event[0] == 'kill':
                    self.kill(actor_event[1])

                elif actor_event[0] == 'send':
                    self.send(
                        actor_ref=actor_event[1],
                        message=actor_event[2],
                        sender_ref=actor_event[3]
                    )

                actor_event = next(generator)


def main():
    system = ActorSystem()

    system.spawn(Ping)

    system.run()


if __name__ == '__main__':
    main()



