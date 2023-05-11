import json
import logging

import amqpstorm
import pytest
from unittest.mock import Mock as Mk
from amqpworker.easyqueue.message import AMQPMessage
from amqpworker.easyqueue.queue import (
    ConnType,
    JsonQueue,
    QueueConsumerDelegate,
    _ConsumptionHandler,
    _ensure_conn_is_ready,
)


def test_it_raises_an_error_if_its_initialized_with_both_delegate_and_delegate_class(mocker):
    with pytest.raises(ValueError) as e:
        JsonQueue(
            host="127.0.0.1",
            username="guest",
            password="guest",
            delegate=mocker.Mock(),
            delegate_class=mocker.Mock(),
        )


def test_its_possible_to_initialize_without_a_delegate():
    queue = JsonQueue(
        host="127.0.0.1",
        username="guest",
        password="guest",
    )

    assert isinstance(queue, JsonQueue)


def test_it_initializes_a_delegate_if_delegate_class_is_provided(mocker):
    Mock = mocker.Mock
    delegate_class = Mock()
    JsonQueue(Mock(), Mock(), Mock(), delegate_class=delegate_class)
    delegate_class.assert_called_once_with()


class SettableMock(Mk):
    def __setitem__(self, key, value):
        pass


class SubscriptableMock(Mk):
    def __getitem__(self, item):
        if item == "consumer_tag":
            return 'consumer_666'
        raise NotImplementedError

    # basic = Mk(
    #
    #     publish=Mk(),
    #     qos=Mk(),
    #     consume=Mk(
    #         return_value='consumer_666'
    #     ),
    #
    # )


@pytest.fixture
def connected_queue(mocker):
    mocked_connection = mocker.Mock(
        return_value=mocker.Mock(channel=SubscriptableMock(return_value=mocker.Mock(basic=mocker.Mock(
            publish=mocker.Mock(), qos=mocker.Mock(), consume=mocker.Mock(return_value='consumer_666')
        )))))
    mocker.patch.object(amqpstorm.Connection, '__new__', mocked_connection)
    return JsonQueue(host="127.0.0.1",
                     username="guest",
                     password="guest", delegate=mocker.Mock())


@pytest.fixture
def write_conn(connected_queue):
    return connected_queue.conn_for(ConnType.WRITE)


def test_it_dont_call_consumer_handler_methods(mocker, connected_queue):
    assert not connected_queue.delegate.on_queue_message.called


def test_it_puts_messages_into_queue_as_json_if_message_is_a_json_serializeable(mocker, connected_queue, write_conn):
    Mock = mocker.Mock
    message = {
        "artist": "Great White",
        "song": "Once Bitten Twice Shy",
        "album": "Twice Shy",
    }

    exchange = Mock()
    routing_key = Mock()
    properties = SettableMock()
    mandatory = Mock()
    immediate = Mock()
    connected_queue.put(
        data=message,
        exchange=exchange,
        routing_key=routing_key,
        properties=properties,
        mandatory=mandatory,
        immediate=immediate,
    )
    expected = mocker.call(body=json.dumps(message).encode(),
                           exchange=exchange,
                           routing_key=routing_key,
                           properties=properties,
                           mandatory=mandatory,
                           immediate=immediate, )
    assert [expected] == write_conn.channel.basic.publish.call_args_list


def test_it_raises_an_error_if_both_data_and_json_are_passed_to_put_message(
        mocker, connected_queue, write_conn
):
    Mock = mocker.Mock
    message = {
        "artist": "Great White",
        "song": "Once Bitten Twice Shy",
        "album": "Twice Shy",
    }
    exchange = Mock()
    routing_key = Mock()
    properties = SettableMock()
    mandatory = Mock()
    immediate = Mock()
    with pytest.raises(ValueError):
        connected_queue.put(
            serialized_data=json.dumps(message),
            data=message,
            exchange=exchange,
            routing_key=routing_key,
            properties=properties,
            mandatory=mandatory,
            immediate=immediate,
        )
    expected = mocker.call(
        body=json.dumps(message).encode(),
        routing_key=routing_key,
        exchange_name=exchange,
        properties=properties,
        mandatory=mandatory,
        immediate=immediate,
    )
    write_conn.channel.basic.publish.assert_not_called()


def test_it_encodes_payload_into_bytes_if_payload_is_str(mocker, connected_queue, write_conn):
    Mock = mocker.Mock
    payload = json.dumps({"dog": "Xablau"})
    exchange = Mock()
    routing_key = Mock()
    properties = SettableMock()
    mandatory = Mock()
    immediate = Mock()
    connected_queue.put(
        serialized_data=payload,
        exchange=exchange,
        routing_key=routing_key,
        properties=properties,
        mandatory=mandatory,
        immediate=immediate,
    )

    write_conn.channel.basic.publish.assert_called_once_with(
        body=payload.encode(),
        routing_key=routing_key,
        exchange=exchange,
        properties=properties,
        mandatory=mandatory,
        immediate=immediate,
    )


def test_it_doesnt_encodes_payload_into_bytes_if_payload_is_already_bytes(
        mocker, connected_queue, write_conn
):
    Mock = mocker.Mock
    payload = json.dumps({"dog": "Xablau"}).encode()
    exchange = Mock()
    routing_key = Mock()
    properties = SettableMock()
    mandatory = Mock()
    immediate = Mock()
    connected_queue.put(
        serialized_data=payload,
        exchange=exchange,
        routing_key=routing_key,
        properties=properties,
        mandatory=mandatory,
        immediate=immediate,
    )

    write_conn.channel.basic.publish.assert_called_once_with(
        body=payload,
        routing_key=routing_key,
        exchange=exchange,
        properties=properties,
        mandatory=mandatory,
        immediate=immediate,
    )


def test_connect_gets_if_put_is_called_before_connect(mocker, connected_queue, write_conn):
    message = {
        "artist": "Great White",
        "song": "Once Bitten Twice Shy",
        "album": "Twice Shy",
    }
    Mock = mocker.Mock
    connect = mocker.patch.object(write_conn, "_connect")
    mocker.patch.object(
        write_conn,
        "channel",
        Mock(is_open=False, return_value={'basic': Mock(
            publish=Mock()
        )}),
    )
    connected_queue.put(data=message, routing_key="Xablau")
    connect.assert_called_once()


def test_it_raises_and_error_if_put_message_isnt_json_serializeable(
        mocker, connected_queue, write_conn
):
    Mock = mocker.Mock
    message = Mock()
    exchange = Mock()
    routing_key = Mock()
    with pytest.raises(TypeError):
        connected_queue.put(message, exchange=exchange, routing_key=routing_key)
    write_conn.channel.basic.publish.assert_not_called()


@pytest.fixture
def consume_conn(connected_queue):
    return connected_queue.conn_for(ConnType.CONSUME)


class ConsumeException(Exception):
    pass


def test_it_calls_on_before_start_consumption_before_queue_consume(
        mocker, connected_queue, consume_conn
):
    Mock = mocker.Mock
    connected_queue.connection._connect()
    mocker.patch.object(connected_queue.connection.channel.basic, 'consume', side_effect=ConsumeException())
    delegate = mocker.Mock(on_before_start_consumption=mocker.Mock())
    queue_name = mocker.Mock()
    with pytest.raises(ConsumeException):
        connected_queue.consume(queue_name, Mock(), delegate)
    delegate.on_before_start_consumption.assert_called_once_with(
        queue_name=queue_name, queue=connected_queue
    )


def test_connect_gets_called_if_consume_is_called_before_connect(
        mocker, connected_queue
):
    Mock = mocker.Mock
    channel = Mock(
        is_open=False,
        return_value={
            'basic': Mock(qoc=Mock(), consume=Mock())
        }
    )
    connect = mocker.patch.object(
        connected_queue.connection, "_connect"
    )
    mocker.patch.object(connected_queue.connection, "channel", channel)
    queue_name = Mock()
    connected_queue.consume(
        queue_name, Mock(), delegate=Mock(spec=QueueConsumerDelegate)
    )
    connect.assert_called_once()


def test_calling_consume_starts_message_consumption(mocker, connected_queue):
    Mock = mocker.Mock
    connected_queue.connection._connect()
    connected_queue.consume(queue_name=Mock(), pool=Mock(), delegate=Mock(spec=QueueConsumerDelegate))
    assert connected_queue.connection.channel.basic.consume.call_count == 1


def test_calling_consume_binds_handler_method(mocker, connected_queue):
    Mock = mocker.Mock
    connected_queue.connection._connect()
    channel = connected_queue.connection.channel
    queue_name = Mock()
    consumer_name = Mock()
    expected_prefetch_count = 666
    connected_queue.prefetch_count = expected_prefetch_count
    Handler = mocker.patch(
        "amqpworker.easyqueue.queue._ConsumptionHandler",
        return_value=Mock(spec=_ConsumptionHandler),
    )
    delegate = Mock(spec=QueueConsumerDelegate)
    pool = Mock()
    connected_queue.consume(
        queue_name=queue_name,
        pool=pool,
        consumer_name=consumer_name,
        delegate=delegate,
    )
    expected = mocker.call(
        callback=mocker.ANY, queue=queue_name, consumer_tag=consumer_name
    )
    assert connected_queue.connection.channel.basic.consume.call_args_list == [expected]
    _, kwargs = channel.basic.consume.call_args_list[0]
    callback = kwargs["callback"]
    message = Mock()
    callback(
        message=message
    )
    Handler.assert_called_once_with(
        delegate=delegate, queue=connected_queue, queue_name=queue_name
    )
    Handler.return_value.handle_message.assert_called_once_with(
        message=message
    )


def test_calling_consume_sets_a_prefetch_qos(mocker, connected_queue):
    Mock = mocker.Mock
    connected_queue.connection._connect()
    expected_prefetch_count = 666
    connected_queue.prefetch_count = expected_prefetch_count
    connected_queue.consume(
        queue_name=Mock(), pool=Mock(), delegate=Mock(spec=QueueConsumerDelegate)
    )
    expected = mocker.call(
        global_=mocker.ANY,
        prefetch_count=expected_prefetch_count,
        prefetch_size=0,
    )
    assert connected_queue.connection.channel.basic.qos.call_args_list == [expected]


def test_calling_consume_starts_a_connection(mocker, connected_queue):
    Mock = mocker.Mock
    mocked_connection = mocker.Mock(return_value=mocker.Mock())
    _connect = mocker.patch.object(amqpstorm.Connection, '__new__', mocked_connection)
    consumer = Mock(spec=QueueConsumerDelegate)
    assert not _connect.called
    connected_queue.consume(
        queue_name="test_queue", pool=Mock(), delegate=consumer
    )
    assert _connect.called


def test_calling_consume_notifies_delegate(mocker, connected_queue):
    Mock = mocker.Mock
    expected_prefetch_count = 666
    connected_queue.prefetch_count = expected_prefetch_count
    delegate = Mock(spec=QueueConsumerDelegate)
    connected_queue.consume(
        queue_name="test_queue", pool=Mock(), delegate=delegate
    )

    delegate.on_before_start_consumption.assert_called_once_with(
        queue_name="test_queue", queue=connected_queue
    )
    delegate.on_consumption_start.assert_called_once_with(
        consumer_tag="consumer_666", queue=connected_queue
    )


@pytest.fixture
def handler_method(mocker, connected_queue):
    properties = SettableMock(name="Properties")
    delegate = mocker.Mock(spec=QueueConsumerDelegate)
    consumer_tag = connected_queue.consume(
        queue_name="test_queue",
        pool=mocker.Mock(),
        delegate=delegate,
        consumer_name='fixture',
    )

    handler = _ConsumptionHandler(
        delegate=delegate,
        queue=connected_queue,
        queue_name="test_queue",
    )
    return properties, delegate, handler, mocker.Mock(name="method", consumer_tag=consumer_tag, delivery_tag='1')


def test_it_calls_on_queue_message_with_the_message_body_wrapped_as_a_AMQPMessage_instance(mocker, connected_queue,
                                                                                           handler_method):
    content = {
        "artist": "Caetano Veloso",
        "song": "Não enche",
        "album": "Livro",
    }
    body = json.dumps(content).encode("utf-8")
    properties, delegate, handler, method = handler_method
    _handle_callback = mocker.patch.object(handler, "_handle_callback", mocker.Mock())
    message = mocker.Mock(channel=connected_queue.connection.channel, body=body, method=method, properties=properties,
                          delivery_tag='1')
    handler.handle_message(
        message=message
    )
    amqp_message = AMQPMessage(
        connection=connected_queue.connection,
        channel=connected_queue.connection.channel,
        queue=connected_queue,
        properties=properties,
        delivery_tag=method.delivery_tag,
        deserialization_method=connected_queue.deserialize,
        queue_name="test_queue",
        serialized_data=body,
    )
    _handle_callback.assert_called_once_with(
        handler.delegate.on_queue_message,
        msg=amqp_message,
    )


def test_it_calls_on_message_handle_error_if_message_handler_raises_an_error(
        mocker, connected_queue, handler_method
):
    content = {
        "artist": "Caetano Veloso",
        "song": "Não enche",
        "album": "Livro",
    }
    properties, delegate, handler, method = handler_method
    error = handler.delegate.on_queue_message.side_effect = KeyError()
    kwargs = dict(
        callback=handler.delegate.on_queue_message,
        channel=connected_queue.connection.channel,
        body=json.dumps(content),
        properties=properties,
    )
    handler._handle_callback(**kwargs)
    del kwargs["callback"]
    handler.delegate.on_message_handle_error.assert_called_once_with(
        handler_error=error, **kwargs
    )


@pytest.fixture
def ensure_queue(mocker):
    Mock = mocker.Mock
    return JsonQueue(
        "127.0.0.1",
        "guest",
        "guest",
        seconds_between_conn_retry=666,
        logger=Mock(spec=logging.Logger),
        connection_fail_callback=Mock(),
    )


def test_it_waits_before_trying_to_reconnect_if_connect_fails(mocker, ensure_queue):
    Mock = mocker.Mock
    coro = Mock()
    sleep = mocker.patch("amqpworker.easyqueue.queue.time.sleep")
    mocker.patch.object(ensure_queue.connection, '_connect', Mock(side_effect=[ConnectionError, True]))
    wrapped = _ensure_conn_is_ready(ConnType.CONSUME)(coro)
    wrapped(ensure_queue, 1, dog="Xablau")
    sleep.assert_called_once_with(666)
    ensure_queue.connection._connect.assert_has_calls([mocker.call(), mocker.call()])
    coro.assert_called_once_with(ensure_queue, 1, dog="Xablau")


def test_it_logs_connection_retries_if_a_logger_istance_is_available(mocker, ensure_queue):
    Mock = mocker.Mock
    coro = Mock()
    mocker.patch("amqpworker.easyqueue.queue.time.sleep")
    mocker.patch.object(ensure_queue.connection, '_connect', Mock(side_effect=[ConnectionError, True]))
    wrapped = _ensure_conn_is_ready(ConnType.CONSUME)(coro)
    wrapped(ensure_queue, 1, dog="Xablau")
    ensure_queue.logger.error.assert_called_once()


def test_it_calls_connection_fail_callback_if_connect_fails(mocker, ensure_queue):
    error = ConnectionError()
    Mock = mocker.Mock
    coro = Mock()
    mocker.patch("amqpworker.easyqueue.queue.time.sleep")
    mocker.patch.object(ensure_queue.connection, '_connect', Mock(side_effect=[error, True]))
    wrapped = _ensure_conn_is_ready(ConnType.CONSUME)(coro)
    wrapped(ensure_queue, 1, dog="Xablau")
    ensure_queue.connection_fail_callback.assert_called_once_with(
        error, 1
    )
