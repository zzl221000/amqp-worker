import logging
from asyncio import Task
from concurrent.futures import Future
from typing import TYPE_CHECKING, List

from loguru import logger

from amqpworker.connections import AMQPConnection
from amqpworker.consumer import Consumer
from amqpworker.options import RouteTypes
from amqpworker.signals.handlers.base import SignalHandler

if TYPE_CHECKING:  # pragma: no cover
    from amqpworker.app import App  # noqa: F401


class RabbitMQ(SignalHandler):
    def shutdown(self, app: "App"):
        logger.debug('shutdown rabbit consumers')
        if RouteTypes.AMQP_RABBITMQ in app:
            for consumer in app[RouteTypes.AMQP_RABBITMQ]["consumers"]:
                logger.debug(f'stop {consumer.host}')
                consumer.stop()
                logger.debug(f'stopped {consumer.host}')

    def startup(self, app: "App") -> List[Future]:
        tasks = []

        app[RouteTypes.AMQP_RABBITMQ]["consumers"] = []
        for route_info in app.routes_registry.amqp_routes:
            conn: AMQPConnection = app.get_connection_for_route(route_info)

            consumer = Consumer(
                route_info=route_info,
                host=conn.hostname,
                port=conn.port,
                username=conn.username,
                password=conn.password,
                prefetch_count=conn.prefetch,
            )
            app[RouteTypes.AMQP_RABBITMQ]["consumers"].append(consumer)
            conn.register(consumer.queue)
            task = app.loop.submit(consumer.start)

            tasks.append(task)

        return tasks
