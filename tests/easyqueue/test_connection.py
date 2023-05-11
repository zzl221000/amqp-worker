import amqpstorm
import pytest
from amqpworker.easyqueue.connection import AMQPConnection
from tests.easyqueue.test_queue import SubscriptableMock


@pytest.fixture
def connection(mocker):
    mocked_connection = mocker.Mock(
        return_value=mocker.Mock(channel=SubscriptableMock(return_value=mocker.Mock(basic=mocker.Mock(
            publish=mocker.Mock(), qos=mocker.Mock(), consume=mocker.Mock(return_value='consumer_666')
        )))))
    mocker.patch.object(amqpstorm.Connection, '__new__', mocked_connection)
    return mocked_connection, AMQPConnection(**dict(
        host="money.que.é.good",
        username="nós",
        password="não",
        virtual_host="have",
        heartbeat=5,
    ))


from amqpstorm import Connection


def test_connection_lock_ensures_amqp_connect_is_only_called_once(
        mocker, connection
):
    Mock = mocker.Mock
    protocol = Mock(channel=Mock(is_open=True))
    connect = mocker.patch.object(Connection, "__new__",
                                  return_value=protocol
                                  )
    [connection[1]._connect() for _ in range(100)]
    assert connect.call_count == 1


def test_connects_with_correct_args(mocker, connection):
    connection[1]._connect()
    conn_params = dict(
        host="money.que.é.good",
        username="nós",
        password="não",
        virtual_host="have",
        heartbeat=5,
    )
    assert connection[0].call_args_list == [
        mocker.call(
            amqpstorm.Connection,
            hostname=conn_params["host"],
            port=5672,
            username=conn_params["username"],
            password=conn_params["password"],
            virtual_host=conn_params["virtual_host"],
            heartbeat=conn_params["heartbeat"],
        )
    ]
