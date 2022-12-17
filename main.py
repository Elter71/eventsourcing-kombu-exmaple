from kombu import Connection, Exchange, Queue

from KombuAPP.application import KombuAPP

connection = Connection('amqps://fgpuiadn:CDXiIDzXvRD5cw9YPQ6GnpP0ZB49lhH8@kebnekaise.lmq.cloudamqp.com/fgpuiadn')

app = KombuAPP(connection)

@app.queue('app_queue')
def test(body):
    print(body)

if __name__ == '__main__':
    from kombu.utils.debug import setup_logging

    # setup root logger
    setup_logging(loglevel='INFO', loggers=[''])
    try:
        app.run()
    except KeyboardInterrupt:
        print('bye bye')

#
# input_queue = Queue('input_data', durable=True)
#
#
# def process_media(body, message):
#     print(body)
#     message.ack()
#
#
# def start_connection():
#     with connection.Consumer([input_queue], callbacks=[process_media]) as consumer:
#         # while True:
#             connection.drain_events()
#
#
# # Press the green button in the gutter to run the script.
# if __name__ == '__main__':
#     start_connection()
#
# # See PyCharm help at https://www.jetbrains.com/help/pycharm/
