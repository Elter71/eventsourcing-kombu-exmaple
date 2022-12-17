from kombu import Queue, Exchange
from kombu.mixins import ConsumerMixin

class Worker:
    def __init__(self, queue: Queue, func, ):
        self.queue = queue,
        self.func = func

    def get_consumer(self, Consumer, channel):
        return Consumer(queues=self.queue,
                         accept=['pickle', 'json'],
                         callbacks=[self.func])

class MainWorker(ConsumerMixin):

    def __init__(self, connection, logger):
        self.connection = connection
        self.workers = []
        self.logger = logger

    def get_consumers(self, Consumer, channel):
        return list(map(lambda w: w.get_consumer(Consumer, channel), self.workers))

    def add_exchange(self, exchange: Exchange):
        def inner(func):
            func(exchange)
            return self
        return inner

    def add_queue(self, queue: Queue):
        def inner(func):
            def wrapper(body, message):
                self.logger.info('Got message in: {queue} with body:{message}'.format(queue=queue, message=body))
                func(body)
                message.ack()
            return self.workers.append(Worker(queue, wrapper))

        return inner

