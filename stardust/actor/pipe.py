import queue


class Pipe:
    def __init__(self):
        self.in_queue = queue.Queue()
        self.out_queue = queue.Queue()

    @property
    def parent_queues(self):
        return self.in_queue, self.out_queue

    @property
    def child_queues(self):
        return self.out_queue, self.in_queue

    @property
    def parent_output_queue(self):
        return self.out_queue

    @property
    def parent_input_queue(self):
        return self.in_queue

    @property
    def child_input_queue(self):
        return self.out_queue

    @property
    def child_output_queue(self):
        return self.in_queue
