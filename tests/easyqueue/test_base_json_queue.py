import pytest

from amqpworker.easyqueue.queue import BaseJsonQueue


@pytest.fixture
def base_json_queue(mocker):
    return BaseJsonQueue(mocker.ANY, mocker.ANY, mocker.ANY)


def test_serialize(base_json_queue):
    body = {"teste": "aãç"}
    result = base_json_queue.serialize(body)
    assert result == '{"teste": "a\\u00e3\\u00e7"}'


def test_serialize_with_ensure_ascii_false(base_json_queue):
    body = {"teste": "aãç"}
    result = base_json_queue.serialize(body, ensure_ascii=False)
    assert '{"teste": "aãç"}' == result


def test_deserialize(base_json_queue):
    body = '{"teste": "aãç"}'.encode("utf-8")
    result = base_json_queue.deserialize(body)
    assert {"teste": "aãç"} == result
