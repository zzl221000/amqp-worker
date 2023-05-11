import pytest
from amqpworker.easyqueue.exceptions import UndecodableMessageException
from amqpworker.easyqueue.message import AMQPMessage


def test_lazy_deserialization_raises_an_error_if_deserialization_fails(mocker):
    Mock = mocker.Mock
    data = b"Xablau"
    deserializer = Mock(side_effect=ValueError)
    msg = AMQPMessage(
        connection=Mock(),
        channel=Mock(),
        queue_name=Mock(),
        serialized_data=data,
        delivery_tag=Mock(),
        properties=Mock(),
        deserialization_method=deserializer,
        queue=Mock(),
    )

    with pytest.raises(UndecodableMessageException):
        _ = msg.deserialized_data

    deserializer.assert_called_once_with(data)


def test_successful_deserialization(mocker):
    Mock = mocker.Mock
    data = b'["Xablau"]'
    deserializer = Mock(return_value=["Xablau"])
    msg = AMQPMessage(
        connection=Mock(),
        channel=Mock(),
        queue_name=Mock(),
        serialized_data=data,
        delivery_tag=Mock(),
        properties=Mock(),
        deserialization_method=deserializer,
        queue=Mock(),
    )
    assert msg.deserialized_data == ["Xablau"]


def test_deserialization_is_only_called_once(mocker):
    Mock = mocker.Mock
    data = b'["Xablau"]'
    deserializer = Mock(return_value=["Xablau"])

    msg = AMQPMessage(
        queue=Mock(),
        connection=Mock(),
        channel=Mock(),
        queue_name=Mock(),
        serialized_data=data,
        delivery_tag=Mock(),
        properties=Mock(),
        deserialization_method=deserializer,
    )

    _ = [msg.deserialized_data for _ in range(10)]

    deserializer.assert_called_once_with(data)


def test_equal_messages(mocker):
    Mock = mocker.Mock
    msg1 = AMQPMessage(
        connection=Mock(),
        channel=Mock(),
        queue_name=Mock(),
        serialized_data=Mock(),
        delivery_tag=Mock(),
        properties=Mock(),
        deserialization_method=Mock(),
        queue=Mock(),
    )
    msg2 = AMQPMessage(
        connection=msg1.connection,
        channel=msg1.channel,
        queue_name=msg1.queue_name,
        serialized_data=msg1.serialized_data,
        delivery_tag=msg1.delivery_tag,
        properties=msg1._properties,
        deserialization_method=msg1._deserialization_method,
        queue=msg1._queue,
    )
    assert msg1 == msg2


def test_not_equal_messages(mocker):
    Mock = mocker.Mock
    msg1 = AMQPMessage(
        connection=Mock(),
        channel=Mock(),
        queue_name=Mock(),
        serialized_data=Mock(),
        delivery_tag=Mock(),
        properties=Mock(),
        deserialization_method=Mock(),
        queue=Mock(),
    )

    msg2 = AMQPMessage(
        connection=msg1.connection,
        channel=Mock(),
        queue_name=msg1.queue_name,
        serialized_data=msg1.serialized_data,
        delivery_tag=msg1.delivery_tag,
        properties=msg1._properties,
        deserialization_method=msg1._deserialization_method,
        queue=Mock(),
    )
    assert msg1 != msg2


def test_it_acks_messages(mocker):
    Mock = mocker.Mock
    msg = AMQPMessage(
        connection=Mock(),
        channel=Mock(basic=Mock(ack=Mock())),
        queue_name=Mock(),
        serialized_data=Mock(),
        delivery_tag=Mock(),
        properties=Mock(),
        deserialization_method=Mock(),
        queue=Mock(),
    )
    msg.ack()

    msg.channel.basic.ack.assert_called_once_with(msg.delivery_tag)


def test_it_rejects_messages_without_requeue(mocker):
    Mock = mocker.Mock
    msg = AMQPMessage(
        connection=Mock(),
        channel=Mock(return_value=Mock(basic=Mock(reject=Mock()))),
        queue_name=Mock(),
        serialized_data=Mock(),
        delivery_tag=Mock(),
        properties=Mock(),
        deserialization_method=Mock(),
        queue=Mock(),
    )

    msg.reject()

    msg.channel.basic.reject.assert_called_once_with(
        delivery_tag=msg.delivery_tag, requeue=False
    )


def test_it_rejects_messages_with_requeue(mocker):
    Mock = mocker.Mock
    msg = AMQPMessage(
        connection=Mock(),
        channel=Mock(return_value=Mock(basic=Mock(reject=Mock()))),
        queue_name=Mock(),
        serialized_data=Mock(),
        delivery_tag=Mock(),
        properties=Mock(),
        deserialization_method=Mock(),
        queue=Mock(),
    )

    msg.reject(requeue=True)
    msg.channel.basic.reject.assert_called_once_with(
        delivery_tag=msg.delivery_tag, requeue=True
    )
