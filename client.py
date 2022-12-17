from kombu.pools import producers

from queues import task_exchange

priority_to_routing_key = {
    'high': 'hipri',
    'mid': 'midpri',
    'low': 'lopri',
}


def send_as_task(connection, fun, args=(), kwargs={}, priority='mid'):
    payload = {'fun': fun, 'args': args, 'kwargs': kwargs}
    routing_key = priority_to_routing_key[priority]

    with producers[connection].acquire(block=True) as producer:
        producer.publish(payload,
                         serializer='pickle',
                         compression='bzip2',
                         exchange=task_exchange,
                         declare=[task_exchange],
                         routing_key=routing_key)


if __name__ == '__main__':
    from kombu import Connection

    from tasks import hello_task

    connection = Connection('amqps://fgpuiadn:CDXiIDzXvRD5cw9YPQ6GnpP0ZB49lhH8@kebnekaise.lmq.cloudamqp.com/fgpuiadn')
    send_as_task(connection, fun=hello_task, args=('Kombu',), kwargs={},
                 priority='high')