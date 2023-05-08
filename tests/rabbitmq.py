from datetime import datetime
from typing import List

from amqpworker.app import App
from amqpworker.connections import AMQPConnection
from amqpworker.rabbitmq import RabbitMQMessage
from amqpworker.routes import AMQPRouteOptions

amqp_conn = AMQPConnection(hostname='127.0.0.1', username='guest', password='guest', port=5672)

app = App(connections=[amqp_conn])


@app.amqp.consume(
    ['test'],
    options=AMQPRouteOptions(bulk_size=1024 * 8, bulk_flush_interval=2)
)
def _handler(msgs: List[RabbitMQMessage]):
    print(f"Recv {len(msgs)} {datetime.now().isoformat()}")

from loguru import logger


@app.run_every(1)
def produce(*args, **kwargs):
    # logger.error("tick produce")
    amqp_conn.put(data={'msg': 'ok'}, routing_key='test')

app.run()
