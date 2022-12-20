from kombu.pools import producers, ProducerPool

from queues import task_exchange

priority_to_routing_key = {
    'high': 'hipri',
    'mid': 'midpri',
    'low': 'lopri',
}


def send_as_task(connection, fun, kwargs={}, priority='mid'):
    payload = {'fun': "fun", 'args': "", 'kwargs': kwargs}
    routing_key = priority_to_routing_key[priority]

    with producers[connection].acquire(block=True) as producer:
        producer.publish(payload,
                         exchange=task_exchange,
                         declare=[task_exchange],
                         routing_key='app_queue')


if __name__ == '__main__':
    from kombu import Connection

    from tasks import hello_task

    connection = Connection('amqp://guest:guest@localhost:5672//')
    send_as_task(connection, fun=hello_task,  kwargs={},
                 priority='high')

    import datetime

    # from kombu import Connection
    #
    # with Connection('amqp://guest:guest@localhost:5672//') as conn:
    #
    #     simple_queue = conn.Producer()
    #     simple_queue.publish("hello", exchange=task_exchange, routing_key="app_queue")
    #     simple_queue.close()
    #     # message = f'helloworld, sent at {datetime.datetime.today()}'
    #     # simple_queue.put(message)
    #     # print(f'Sent: {message}')
    #     # simple_queue.close()