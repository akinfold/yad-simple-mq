# -*- coding: utf-8 -*-
from datetime import datetime
from pymongo import MongoClient, ASCENDING
from yadsimplemq.base import BaseMessageQueue, DEFAULT_ROUTING_KEY, Message


class MongoMessageQueue(BaseMessageQueue):
    _field_prefix = '_mongo_message_queue_'

    def __init__(self, broker_url, exchange, *args, **kwargs):
        """Create new message queue.

        :param broker_url: URI used to establish connection with backend.
        :param exchange: Message exchange.
        :param args: Sequential arguments which will be passed to backend on connect.
        :param kwargs: Keyword arguments which will be passed to backend on connect.
        """
        super(MongoMessageQueue, self).__init__(broker_url, exchange, *args, **kwargs)
        self._client = None
        self._db = None
        self._collection = None

    def connect(self):
        """Open connection with message queue backend if it's doesn't.
        """
        is_new_client = is_new_db = False
        if not self._client or not self._client.alive():
            self._client = MongoClient(self.broker_url, *self.broker_args, **self.broker_kwargs)
            is_new_client = True
        if not self._db or is_new_client:
            self._db = self._client.get_default_database()
            is_new_db = True
        if not self._collection or is_new_db:
            self._collection = self._db[self.exchange]

    def close(self):
        """Close connection with message queue backend.
        """
        self._collection = None
        self._db = None
        if self._client and self._client.alive():
            self._client.close()

    def _get_field_name(self, name):
        return '%s%s' % (self._field_prefix, name)

    def publish(self, message, routing_key=DEFAULT_ROUTING_KEY):
        """Publish new message to queue.

        :param message: Message to publish.
        :param routing_key: Routing key allowing to control which workers will execute task.
        :return: Published Message instance with updated id.
        """
        self.connect()
        _ = self._get_field_name
        raw_msg = message.body.copy()
        raw_msg.update({
            _('routing_key'): routing_key,
            _('is_fetched'): False,
            _('is_fetched_since'): None,
            _('is_acknowledged'): False,
            _('is_acknowledged_since'): None,
        })
        message.id = self._collection.insert(raw_msg)
        return message

    def acknowledge(self, message):
        """Acknowledge message.

        :param message: Instance of Message or message id.
        """
        self.connect()
        message_id = message.id if isinstance(message, Message) else message
        _ = self._get_field_name
        self._collection.update({'_id': message_id},
                                {'$set': {_('is_acknowledged'): False, _('is_acknowledged_since'): datetime.utcnow()}})

    def _fetch_raw_message(self, routing_key=DEFAULT_ROUTING_KEY):
        """Fetch raw message from queue.

        :param routing_key: Routing key allowing to control which workers will execute task.
        """
        _ = self._get_field_name
        return self._collection.find_and_modify(
            query={
                _('routing_key'): routing_key,
                _('is_fetched'): False,
                _('is_acknowledged'): False,
            },
            update={'$set': {_('is_fetched'): True, _('is_fetched_since'): datetime.utcnow()}},
            sort=[('_id', ASCENDING)]
        )

    def _build_message(self, raw_message):
        msg_id = raw_message.pop('_id')
        body = {k: v for k, v in raw_message.iteritems() if not k.startswith(self._field_prefix)}
        return Message(msg_id, body)

    def _fetch_message(self, routing_key=DEFAULT_ROUTING_KEY):
        """Fetch Message instance from queue.

        :param routing_key: Routing key allowing to control which workers will execute task.
        """
        msg = self._fetch_raw_message(routing_key)
        if not msg:
            return None
        else:
            return self._build_message(msg)

    def consume(self, routing_key=DEFAULT_ROUTING_KEY):
        """Generator fetching Messages from queue.

        :param routing_key: Routing key allowing to control which workers will execute task.
        """
        self.connect()
        msg = self._fetch_message(routing_key)
        while msg:
            yield msg
            msg = self._fetch_message(routing_key)
