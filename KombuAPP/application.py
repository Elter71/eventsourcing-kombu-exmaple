from kombu import Connection, Queue
from typing import Callable
from kombu.log import get_logger

from KombuAPP.workermanager import WorkerManager, ConsumerConfig


class KombuAPP:
    def __init__(self, connection: Connection, logger=get_logger('kombu-app')):
        self.worker_manager = WorkerManager(connection, logger)
        self.logger = logger
        self.consumer_config = None

    def consumer(self, consumer_config: ConsumerConfig = ConsumerConfig()):
        self.consumer_config = consumer_config

        def decorator_crete_consumer(func):
            return func

        return decorator_crete_consumer


    def simple_queue(self, name='', exchange=None, routing_key='',
              channel=None, bindings=None, on_declared=None,
              **kwargs) -> Callable:
        result = self.worker_manager.add_queue(
            queue=Queue(name, exchange, routing_key, channel, bindings, on_declared, **kwargs),
            consumer_config=self.consumer_config)
        self.consumer_config = None
        return result

    def queue(self, queue: Queue) -> Callable:
        result = self.worker_manager.add_queue(
            queue=queue,
            consumer_config=self.consumer_config)
        self.consumer_config = None
        return result

    def run(self):
        self.worker_manager.run()
