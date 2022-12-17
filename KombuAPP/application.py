from kombu import Connection, Queue
from typing import Callable
from kombu.log import get_logger

from KombuAPP.mainworker import MainWorker


class KombuAPP:
    def __init__(self, connection: Connection, logger=get_logger('kombu-app')):
        self.router = MainWorker(connection, logger)
        self.logger = logger

    def queue(self, name='', exchange=None, routing_key='',
              channel=None, bindings=None, on_declared=None,
              **kwargs) -> Callable:
        return self.router.add_queue(queue=Queue(name, exchange, routing_key, channel, bindings, on_declared, **kwargs))

    def run(self):
        self.router.run()
