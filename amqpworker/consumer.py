import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures.thread import _shutdown
from typing import Dict, Type, Union

from amqpstorm import AMQPError
from loguru import logger

from amqpworker import conf
from amqpworker.easyqueue.message import AMQPMessage
from amqpworker.easyqueue.queue import JsonQueue, QueueConsumerDelegate
from amqpworker.options import Events, Options
from amqpworker.routes import AMQPRoute
from .bucket import Bucket
from .rabbitmq import RabbitMQMessage
from .scheduled import ScheduledThreadPoolExecutor
from .utils import call


class Consumer(QueueConsumerDelegate):
    def __init__(
            self,
            route_info: Union[Dict, AMQPRoute],
            host: str,
            port: int,
            username: str,
            password: str,
            prefetch_count: int = 128,
            bucket_class: Type[Bucket] = Bucket[RabbitMQMessage],
    ) -> None:
        self.route = route_info
        self._handler = route_info["handler"]
        self._queue_name = route_info["routes"]
        self._route_options = route_info["options"]
        self.host = host
        self.vhost = route_info.get("vhost", "/")
        self.bucket = bucket_class(
            size=min(self._route_options["bulk_size"], prefetch_count)
        )
        self.running = False
        self.queue: JsonQueue = JsonQueue(
            host,
            username,
            password,
            port,
            virtual_host=self.vhost,
            delegate=self,
            prefetch_count=prefetch_count,
            logger=conf.logger,
            connection_fail_callback=self._route_options.get(
                Options.CONNECTION_FAIL_CALLBACK, None
            ),
        )

        self.pool = ThreadPoolExecutor(thread_name_prefix=f'submit-{"-".join(self._queue_name)}-',
                                       max_workers=self._route_options.get('max_workers', 8))
        self.multiple_queue_pool = ThreadPoolExecutor(thread_name_prefix='consumer-queue-',
                                                      max_workers=len(self._queue_name))
        self.clock = ScheduledThreadPoolExecutor(max_workers=1, name='flush-rabbit-')
        self.clock_task = None

    @property
    def queue_name(self) -> str:
        return self._queue_name

    def on_before_start_consumption(
            self, queue_name: str, queue: "JsonQueue"
    ):
        """
        Coroutine called before queue consumption starts. May be overwritten to
        implement further custom initialization.

        :param queue_name: Queue name that will be consumed
        :type queue_name: str
        :param queue: AsynQueue instanced
        :type queue: JsonQueue
        """
        pass

    def on_consumption_start(self, consumer_tag: str, queue: "JsonQueue"):
        """
        Coroutine called once consumption started.
        """
        pass

    def on_queue_message(self, msg: AMQPMessage):
        """
        Callback called every time that a new, valid and deserialized message
        is ready to be handled.
        """
        if not self.bucket.is_full():
            message = RabbitMQMessage(
                delivery_tag=msg.delivery_tag,
                amqp_message=msg,
                on_success=self._route_options[Events.ON_SUCCESS],
                on_exception=self._route_options[Events.ON_EXCEPTION],
            )
            self.bucket.put(message)

        if self.bucket.is_full():
            return self._flush_bucket_if_needed()

    def _flush_clocked(self, *args, **kwargs):
        try:
            self._flush_bucket_if_needed()
        except Exception as e:
            conf.logger.error(
                {
                    "type": "flush-bucket-failed",
                    "dest": self.host,
                    "retry": True,
                    "exc_traceback": traceback.format_exc(),
                }
            )

    def _flush_bucket_if_needed(self):
        try:
            if not self.bucket.is_empty():
                all_messages = self.bucket.pop_all()
                conf.logger.debug(
                    {
                        "event": "bucket-flush",
                        "bucket-size": len(all_messages),
                        "handler": self._handler.__name__,
                    }
                )
                if self.running:
                    rv = self._handler(all_messages)
                    for fu in self.pool.map(call, [m.process_success for m in all_messages]):
                        pass
                    return rv
                else:
                    for m in all_messages:
                        m.process_exception()
        except AMQPError:
            raise
        except Exception as e:
            traceback.print_exc()
            if self.running:
                for fu in self.pool.map(call, [m.process_exception for m in all_messages]):
                    pass
            else:
                for m in all_messages:
                    m.process_exception()
            raise e

    def on_queue_error(self, body, delivery_tag, error, queue):
        """
        Callback called every time that an error occurred during the validation
        or deserialization stage.

        :param body: unparsed, raw message content
        :type body: Any
        :param delivery_tag: delivery_tag of the consumed message
        :type delivery_tag: int
        :param error: THe error that caused the callback to be called
        :type error: MessageError
        :type queue: JsonQueue
        """
        conf.logger.error(
            {
                "parse-error": True,
                "exception": "Error: not a JSON",
                "original_msg": body,
            }
        )
        try:
            queue.ack(delivery_tag=delivery_tag)
        except AMQPError as e:
            self._log_exception(e)

    def on_message_handle_error(self, handler_error: Exception, **kwargs):
        """
        Callback called when an uncaught exception was raised during message
        handling stage.

        :param handler_error: The exception that triggered
        :param kwargs: arguments used to call the coroutine that handled
        the message
        """
        self._log_exception(handler_error)

    def on_connection_error(self, exception: Exception):
        """
        Called when the connection fails
        """
        self._log_exception(exception)

    def _log_exception(self, exception):
        current_exception = {
            "exc_message": str(exception),
            "exc_traceback": traceback.format_exc(),
        }
        conf.logger.error(current_exception)

    def consume_all_queues(self, queue: JsonQueue):

        # for r in self.multiple_queue_pool.map(queue.consume, self._queue_name, repeat(self)):
        #     pass
        for queue_name in self._queue_name:
            # Por enquanto não estamos guardando a consumer_tag retornada
            # se precisar, podemos passar a guardar.
            conf.logger.debug(
                {"queue": queue_name, "event": "start-consume"}
            )
            queue.consume(queue_name=queue_name, pool=self.multiple_queue_pool, delegate=self)

    def keep_runnig(self):
        return self.running

    def stop(self):
        logger.debug('stop consumer')
        self.running = False
        self.queue.connection.close()
        self.pool.shutdown(False)
        self.multiple_queue_pool.shutdown(False)
        self.clock.stop(None)

    def start(self):
        self.running = True
        if not self.clock_task:
            seconds = self._route_options.get(
                Options.BULK_FLUSH_INTERVAL, conf.settings.FLUSH_TIMEOUT
            )
            self.clock.schedule_at_fixed_rate(self._flush_clocked, seconds, seconds)
            self.clock.start(None)
            self.clock_task = self._flush_clocked
        while self.keep_runnig():
            if not self.queue.connection.has_channel_ready():
                try:
                    self.consume_all_queues(self.queue)
                except RuntimeError as e:
                    traceback.print_exc()
                    if self.multiple_queue_pool._shutdown or _shutdown:
                        conf.logger.info('app shutdown')
                    # if 'interpreter shutdown' in str(e):
                    #     return
                    else:
                        raise
                except KeyboardInterrupt:
                    self.stop()
                except Exception as e:

                    conf.logger.error(
                        {
                            "type": "connection-failed",
                            "dest": self.host,
                            "retry": True,
                            "exc_traceback": traceback.format_exc(),
                        }
                    )
            time.sleep(1)
