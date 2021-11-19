import abc
from threading import Thread
import time
from typing import Any
from redis import StrictRedis
import pika
from uuid import uuid4

from .exceptions import EmptyQueueException


class Queue(metaclass=abc.ABCMeta):
    def __init__(self, name: str):
        """Make this be a new queue.

        Args:
            name: the name of the queue

        Requires:
            name is not None

        """
        self.__name = name

    @property
    def name(self):
        """Name of the queue.

        Returns:
            The name of the queue

        """
        return self.__name

    def __len__(self):  # noqa: D105
        raise NotImplementedError()

    def put(self, element: Any, serializer: callable = None) -> None:
        """Put an element in the queue.

        Args:
            element: the element to be added
            serializer: an optionnal serializer for the element


        Requires:
            element is not None
            if given serializer is a callable with one parameter

        Effects:
            Serialize the element and put it in the queue

        """
        raise NotImplementedError()

    def get(self, deserializer: callable = None, timeout=5) -> Any:
        """Get an element from the queue.

        Args:
            deserializer: an optionnal deserializer for the element
            timeout: a timeout for the element to be given

        Requires:
            if given, deserializer is a callable with one parameter

        Raise:
            EmptyQueueException is the queue is empty after the timeout

        Returns:
            the deserialized element

        """
        raise NotImplementedError()


class InMemoryQueue(Queue):
    def __init__(self, name):  # noqa: D107
        super().__init__(name)
        self.__queue = []

    def __len__(self):  # noqa: D105
        return len(self.__queue)

    def put(
        self, element: Any, serializer: callable = None
    ) -> None:  # noqa: D102
        if serializer is not None:
            element = serializer(element)
        self.__queue.insert(0, element)

    def get(
        self, deserializer: callable = None, timeout=5
    ) -> Any:  # noqa: D102
        for _ in range(timeout):
            try:
                element = self.__queue.pop()
                if deserializer is not None:
                    element = deserializer(element)
                return element
            except IndexError:
                time.sleep(1)
        raise EmptyQueueException()


class RedisQueue(Queue):
    def __init__(self, name, address, db=0, password=None):  # noqa: D107
        super().__init__(name)
        self.__consumer_id = str(uuid4())
        self.__redis = StrictRedis(address, db=db, password=password)

    def __len__(self):  # noqa: D105
        return self.__redis.llen(self.name)

    def put(
        self, element: Any, serializer: callable = None
    ) -> None:  # noqa: D102
        if serializer is not None:
            element = serializer(element)

        if not isinstance(element, bytes):
            element = bytes(element, "utf-8")

        self.__redis.lpush(self.name, element)

    def get(
        self, deserializer: callable = None, timeout=5
    ) -> Any:  # noqa: D102
        for _ in range(timeout):
            if (
                self.__redis.rpoplpush(self.name, self.__consumer_id)
                is not None
            ):
                element = self.__redis.lpop(self.__consumer_id).decode("utf8")
                if deserializer is None:
                    return element
                else:
                    return deserializer(element)
            else:
                time.sleep(1)
        raise EmptyQueueException()


class RabbitMQueue(Queue):
    def __init__(
        self, name, address, username=None, password=None
    ):  # noqa: D107
        super().__init__(name)
        credentials = None
        if username is not None and password is not None:
            credentials = pika.PlainCredentials(username, password)
            self.__parameters = pika.ConnectionParameters(
                host=address, credentials=credentials
            )
        else:
            self.__parameters = pika.ConnectionParameters(host=address)

    def put(
        self, element: Any, serializer: callable = None
    ) -> None:  # noqa: D102
        if serializer is not None:
            element = serializer(element)
        assert isinstance(element, str)

        connection = pika.BlockingConnection(self.__parameters)
        channel = connection.channel()

        channel.queue_declare(queue=self.name, durable=True)

        message = element
        channel.basic_publish(
            exchange="",
            routing_key=self.name,
            body=message,
            properties=pika.BasicProperties(
                delivery_mode=2  # make message persistent
            ),
        )
        connection.close()

    def get(
        self, deserializer: callable = None, timeout=5
    ) -> Any:  # noqa: D102
        raise NotImplementedError(
            "Get methode is not implemented forRabbitMQueue. "
            "Use consumer or start_consuming method instead"
        )

    def start_consuming(
        self, callback: callable, deserializer: callable = None
    ):
        assert callable(callback)

        def _callback(channel, method, properties, body):
            try:
                message = deserializer(body.decode("utf-8"))
                callback(message)
            except Exception as e:
                raise e
            finally:
                channel.basic_ack(delivery_tag=method.delivery_tag)

        connection = pika.BlockingConnection(self.__parameters)
        channel = connection.channel()
        channel.queue_declare(queue=self.name, durable=True)

        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue=self.name, on_message_callback=_callback)
        channel.start_consuming()

    def consumer(self, callback: callable, deserializer: callable = None):
        assert callable(callback)

        def _callback(channel, method, properties, body):
            try:
                message = deserializer(body.decode("utf-8"))
                callback(message)
            except Exception as e:
                raise e
            finally:
                channel.basic_ack(delivery_tag=method.delivery_tag)

        def _consume():
            connection = pika.BlockingConnection(self.__parameters)
            channel = connection.channel()
            channel.queue_declare(queue=self.name, durable=True)

            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(
                queue=self.name, on_message_callback=_callback
            )
            channel.start_consuming()

        thread = Thread(target=_consume)
        thread.setDaemon(True)
        return thread
