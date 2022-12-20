from kombu import Connection
from KombuAPP.application import KombuAPP
from KombuAPP.workermanager import ConsumerConfig
from kombu.utils.debug import setup_logging

from queues import app_queue

connection = Connection('amqp://guest:guest@localhost')

app = KombuAPP(connection)


@app.consumer(ConsumerConfig(auto_declare=True))
@app.queue(app_queue)
def test(body, message):
    print(body)
    print(message)
    message.ack()



if __name__ == '__main__':
    # setup root logger
    setup_logging(loglevel='INFO', loggers=[''])
    try:
        app.run()
    except KeyboardInterrupt:
        print('bye bye')
