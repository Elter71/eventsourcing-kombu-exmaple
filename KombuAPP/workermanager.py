import functools

from kombu import Queue
from kombu.mixins import ConsumerMixin


class ConsumerConfig:
    """Message consumer config.

      Arguments:
          no_ack (bool): see :attr:`no_ack`.
          auto_declare (bool): see :attr:`auto_declare`
          prefetch_count (int): see :attr:`prefetch_count`.
          accept (list): see :attr:`accept`
      """

    #: Flag for automatic message acknowledgment.
    #: If enabled the messages are automatically acknowledged by the
    #: broker.  This can increase performance but means that you
    #: have no control of when the message is removed.
    #:
    #: Disabled by default.
    no_ack = None

    #: By default all entities will be declared at instantiation, if you
    #: want to handle this manually you can set this to :const:`False`.
    auto_declare = True

    #: List of accepted content-types.
    #:
    #: An exception will be raised if the consumer receives
    #: a message with an untrusted content type.
    #: By default all content-types are accepted, but not if
    #: :func:`kombu.disable_untrusted_serializers` was called,
    #: in which case only json is allowed.
    accept = None

    #: Initial prefetch count
    #:
    #: If set, the consumer will set the prefetch_count QoS value at startup.
    #: Can also be changed using :meth:`qos`.
    prefetch_count = None

    def __init__(self, no_ack=None, auto_declare=None, accept=None, prefetch_count=None, tag_prefix=None):
        self.no_ack = no_ack
        self.tag_prefix = tag_prefix
        self.prefetch_count = prefetch_count
        self.accept = accept
        if auto_declare is not None:
            self.auto_declare = auto_declare

    @property
    def config(self):
        return {'no_ack': self.no_ack, 'tag_prefix': self.tag_prefix, 'prefetch_count': self.prefetch_count,
                'accept': self.accept, 'auto_declare': self.auto_declare}


class Worker:
    def __init__(self, queue: Queue, func, consumer_config: ConsumerConfig = None):
        self.queue = queue,
        self.func = func
        self.consumer_config = ConsumerConfig() if consumer_config is None else consumer_config

    def get_consumer(self, Consumer, channel):
        return Consumer(queues=self.queue, callbacks=[self.func], **self.consumer_config.config)


class WorkerManager(ConsumerMixin):

    def __init__(self, connection, logger):
        self.connection = connection
        self.workers = []
        self.logger = logger

    def get_consumers(self, Consumer, channel):
        return list(map(lambda w: w.get_consumer(Consumer, channel), self.workers))

    def on_consume_ready(self, connection, channel, consumers, **kwargs):
        self.logger.info('on_consume_ready: {queue} with consumer config: ${config}'.format(queue=channel,
                                                                                            config=consumers))
        pass

    def add_queue(self, queue: Queue, consumer_config=None):
        def decorator_add_queue(func):
            def callback(body, message):
                self.logger.info('Got message in: {queue} with body:{message}'.format(queue=queue, message=body))
                func(body, message)

            return self.workers.append(Worker(queue, callback, consumer_config))

        return decorator_add_queue
