import json
from threading import Timer

import fakeredis
from pytest import fixture, raises

import abstractqueue.Queue as queue_module
from abstractqueue.exceptions import EmptyQueueException

queue_module.StrictRedis = fakeredis.FakeStrictRedis


@fixture
def queue():
    return queue_module.RedisQueue("Queue", "localhost")


def test_init():
    queue = queue_module.RedisQueue("Queue1", "localhost")
    queue.name == "Queue1"


def test_add(queue):
    def serialize(element):
        return bytes(json.dumps(element), "utf-8")

    input_element = {"hello": "world"}
    queue.put(input_element, serializer=serialize)
    assert len(queue) == 1


def test_get(queue):
    def serialize(element):
        return bytes(json.dumps(element), "utf-8")

    def deserialize(element):
        element = element.decode("utf-8")
        return json.loads(element)

    input_element = {"hello": "world"}
    queue.put(input_element, serializer=serialize)
    assert len(queue) == 1
    element = queue.get(deserializer=deserialize)
    assert input_element == element
    assert len(queue) == 0


def test_get_wait(queue):
    t = Timer(2, lambda: queue.put(bytes("hello world", "utf-8")))
    t.start()
    try:
        queue.get()
        assert True
    except EmptyQueueException:
        assert False
    t.join()


def test_add_too_late(queue):
    t = Timer(7, lambda: queue.put(bytes("hello world", "utf-8")))
    t.start()
    try:
        queue.get()
        assert False
    except EmptyQueueException:
        assert True
    t.join()


def test_get_empty(queue):
    try:
        queue.get()
        assert False
    except EmptyQueueException:
        assert True


def test_serializer(queue):
    def serialize(element):
        return bytes(json.dumps(element), "utf-8")

    input_element = {"hello": "world"}
    queue.put(input_element, serializer=serialize)
    element = queue.get()
    assert json.loads(element) == input_element


def test_deserialize(queue):
    def serialize(element):
        return bytes(json.dumps(element), "utf-8")

    def deserialize(element):
        element = element.decode("utf-8")
        return json.loads(element)

    input_element = {"hello": "world"}
    queue.put(input_element, serializer=serialize)
    element = queue.get(deserializer=deserialize)
    assert element == input_element


def test_put_invalid_type(queue):
    with raises(ValueError):
        queue.put("not bytes")
