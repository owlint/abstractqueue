import json
from threading import Timer

from pytest import fixture

from abstractqueue.exceptions import EmptyQueueException
from abstractqueue.Queue import InMemoryQueue


@fixture
def queue():
    return InMemoryQueue("Queue")


def test_init():
    queue = InMemoryQueue("Queue1")
    queue.name == "Queue1"


def test_add(queue):
    input_element = {"hello": "world"}
    queue.put(input_element)
    assert len(queue) == 1


def test_get(queue):
    input_element = {"hello": "world"}
    queue.put(input_element)
    assert len(queue) == 1
    element = queue.get()
    assert input_element == element
    assert len(queue) == 0


def test_get_wait(queue):
    t = Timer(2, lambda: queue.put("hello world"))
    t.start()
    try:
        queue.get()
        assert True
    except EmptyQueueException:
        assert False
    t.join()


def test_add_too_late(queue):
    t = Timer(7, lambda: queue.put("hello world"))
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
        return json.dumps(element)

    input_element = {"hello": "world"}
    queue.put(input_element, serializer=serialize)
    element = queue.get()
    assert json.loads(element) == input_element


def test_deserialize(queue):
    def serialize(element):
        return json.dumps(element)

    def deserialize(element):
        return json.loads(element)

    input_element = {"hello": "world"}
    queue.put(input_element, serializer=serialize)
    element = queue.get(deserializer=deserialize)
    assert element == input_element
