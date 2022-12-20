from kombu import Exchange, Queue
# http://rubybunny.info/articles/exchanges.html
task_exchange = Exchange('app', type='direct')
app_queue = Queue('app_queue_SS', task_exchange, routing_key='app_queue')
app_queue2 = Queue('app_queue_SS2', task_exchange, routing_key='app_queue_2')