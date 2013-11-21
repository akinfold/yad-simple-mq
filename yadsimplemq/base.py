# -*- coding: utf-8 -*-
DEFAULT_ROUTING_KEY = 'yadsimplemq.tasks'


class BaseTask(object):
    pass


class BaseMessageQueue(object):
    def __init__(self, broker_url, exchange, *args, **kwargs):
        """Create new message queue.

        :param broker_url: URI used to establish connection with backend.
        :param exchange: Message exchange.
        :param args: Sequential arguments which will be passed to backend on connect.
        :param kwargs: Keyword arguments which will be passed to backend on connect.
        """
        self.broker_url = broker_url
        self.exchange = exchange
        self.broker_args = args
        self.broker_kwqrga = kwargs

    def connect(self):
        """Open connection with message queue backend if it's doesn't.
        """
        raise NotImplementedError('Must be implemented in subclasses.')

    def close(self):
        """Close connection with message queue backend.
        """
        pass

    def publish(self, message, routing_key=DEFAULT_ROUTING_KEY, *args, **kwargs):
        """Publish new message to queue.

        :param message: Message to publish.
        :param routing_key: Routing key allowing to control which workers will execute task.
        """
        raise NotImplementedError('Must be implemented in subclasses.')

    def fetch(self, routing_key=DEFAULT_ROUTING_KEY):
        """Fetch message from queue.

        :param routing_key: Routing key allowing to control which workers will execute task.
        """
        raise NotImplementedError('Must be implemented in subclasses.')

    def consume(self, routing_key=DEFAULT_ROUTING_KEY):
        """Return generator fetching messages from queue.

        :param routing_key: Routing key allowing to control which workers will execute task.
        """
        return (message for message in self.fetch(routing_key))
