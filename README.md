# 🐰amqp-worker

amqp-worker 是一个基于 Python 的多线程 RabbitMQ 消费框架。它可以让你在消费消息时更加高效和稳定。

## 功能特点

- 批量消费：按批处理消息，提高消费效率。
- 自动重连：当 RabbitMQ 服务断开连接时，amqp-worker 会自动重连，保证消费不中断。
- 自定义消费模式：消费函数中自由决定使用多线程和协程。
- 可配置的消息确认方式：支持自动确认和手动确认两种确认方式，根据你的消费需求进行配置。
- 可配置的异常处理：支持全局配置消息异常的消费模式，重入队列、重新插入、消费消息。

## 安装方式

你可以使用 pip 工具来安装 amqp-worker:

```
pip install amqp-worker
```

## 使用方法

首先，你需要在你的 Python 代码中引入 amqp_worker 模块：

```python
from amqpworker.app import App
```

然后，你需要实例化一个 App 对象，而App对象依赖AMQPConnection对象：

```python
from amqpworker.connections import AMQPConnection
amqp_conn = AMQPConnection(hostname='127.0.0.1', username='guest', password='guest', port=5672)

app = App(connections=[amqp_conn])
```



接下来，你需要定义消费函数:

```python
@app.amqp.consume(
    ['test'],
    options=AMQPRouteOptions(bulk_size=1024 * 8, bulk_flush_interval=2)
)
def _handler(msgs: List[RabbitMQMessage]):
    print(f"Recv {len(msgs)} {datetime.now().isoformat()}")
```


上面的代码中我们给消费函数一个装饰器，给出了消费的队列，每批消费的数量，值得注意的是，消费函数的参数类型为`List[RabbitMQMessage]`

最后，只需要调用 `run` 方法即可开始消费：

```python
app.run()
```

## 示例代码

下面是一个简单的示例代码，它会消费名为 `test` 的队列中的消息：

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

## 贡献者

- @JimZhang

## 许可证

amqp-worker 使用 MIT 许可证。详情请参阅 LICENSE 文件。