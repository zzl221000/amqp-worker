from amqpworker.easyqueue.message import AMQPMessage
from amqpworker.options import Actions


class RabbitMQMessage:
    def __init__(
            self,
            delivery_tag: int,
            amqp_message: AMQPMessage,
            on_success: Actions = Actions.ACK,
            on_exception: Actions = Actions.REQUEUE,
    ) -> None:
        self._delivery_tag = delivery_tag
        self._on_success_action = on_success
        self._on_exception_action = on_exception
        self._final_action = None
        self._amqp_message = amqp_message

    @property
    def body(self):
        return self._amqp_message.deserialized_data

    @property
    def serialized_data(self):
        return self._amqp_message.serialized_data

    def reject(self, requeue=True):
        """
        Marca essa mensagem para ser rejeitada. O parametro ``requeue`` indica se a mensagem será recolocada na fila original (``requeue=True``) ou será descartada (``requeue=False``).
        """
        self._final_action = Actions.REQUEUE if requeue else Actions.REJECT

    def requeue(self):
        self._final_action = Actions.REQUEUE_TAIL

    def accept(self):
        """
        Marca essa mensagem para ser confirmada (``ACK``) ao fim da execução do handler.
        """
        self._final_action = Actions.ACK

    def _process_action(self, action: Actions):
        if action == Actions.REJECT:
            self._amqp_message.reject(requeue=False)
        elif action == Actions.REQUEUE:
            self._amqp_message.reject(requeue=True)
        elif action == Actions.REQUEUE_TAIL:
            self._amqp_message.requeue()
        elif action == Actions.ACK:
            self._amqp_message.ack()

    def process_success(self):
        action = self._final_action or self._on_success_action
        return self._process_action(action)

    def process_exception(self):
        action = self._final_action or self._on_exception_action
        return self._process_action(action)
