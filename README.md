# 🐰amqp-worker

English | [简体中文](https://git.loom.run/Coder/amqp-worker/src/branch/master/README_zh.md)


amqp-worker is a Python-based multi-threaded RabbitMQ consumer framework. It allows you to consume messages more efficiently and stably.

## Features

- Batch consumption: process messages in batches, improve consumption efficiency.
- Automatic reconnection: when RabbitMQ service disconnects, amqp-worker will automatically reconnect, ensuring uninterrupted consumption.
- Customizable consumption mode: freely decide to use multi-threading and coroutines in the consumption function.
- Configurable message acknowledgment mode: support automatic acknowledgment and manual acknowledgment modes, configure according to your consumption needs.
- Configurable exception handling: support global configuration of message exception consumption mode, re-enter queue, re-insert, consume message.

## Installation

You can use pip tool to install amqp-worker:

```
pip install amqp-workers
```

## Usage

First, you need to import the amqp_worker module in your Python code:

```python
from amqpworker.app import App
```

Then, you need to instantiate an App object, and the App object depends on the AMQPConnection object:

```python
from amqpworker.connections import AMQPConnection
amqp_conn = AMQPConnection(hostname='127.0.0.1', username='guest', password='guest', port=5672)

app = App(connections=[amqp_conn])
```



Next, you need to define the consumption function:

```python
@app.amqp.consume(
    ['test'],
    options=AMQPRouteOptions(bulk_size=1024 * 8, bulk_flush_interval=2)
)
def _handler(msgs: List[RabbitMQMessage]):
    print(f"Recv {len(msgs)} {datetime.now().isoformat()}")
```


In the above code we give the consumption function a decorator, giving the consumption queue, the number of consumption per batch, it is worth noting that the parameter type of the consumption function is `List[RabbitMQMessage]`

Finally, just call the `run` method to start consuming:

```python
app.run()
```

## Example code

Below is a simple example code that will consume messages from a queue named `test`:

```python
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

app.run()

```

## Contributors

- [@JimZhang](https://git.loom.run/zzl221000)

## License

amqp-worker uses MIT license. Please refer to LICENSE file for details.