from multiprocessing.queues import Queue
from typing import Optional


class Pipe:
    def __init__(self, in_queue: Optional[Queue] = None, out_queue: Optional[Queue] = None):
        self.in_queue = in_queue or Queue()
        self.out_queue = out_queue or Queue()

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
