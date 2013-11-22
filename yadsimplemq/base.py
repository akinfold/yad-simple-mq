# -*- coding: utf-8 -*-
DEFAULT_ROUTING_KEY = 'yadsimplemq.tasks'


class Message(object):
    def __init__(self, id='', body=None):
        self.id = id
        self.body = body or {}


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
        self.broker_kwargs = kwargs

    def connect(self):
        """Open connection with message queue backend if it's doesn't.
        """
        raise NotImplementedError('Must be implemented in subclasses.')

    def close(self):
        """Close connection with message queue backend.
        """
        pass

    def publish(self, message, routing_key=DEFAULT_ROUTING_KEY):
        """Publish new message to queue.

        :param message: Instance of Message to publish.
        :param routing_key: Routing key allowing to control which workers will execute task.
        :return: Published Message instance with updated id.
        """
        raise NotImplementedError('Must be implemented in subclasses.')

    def acknowledge(self, message):
        """Acknowledge message.

        :param message: Instance of Message or message id.
        """
        raise NotImplementedError('Must be implemented in subclasses.')

    def consume(self, routing_key=DEFAULT_ROUTING_KEY):
        """Generator fetching Messages from queue.

        :param routing_key: Routing key allowing to control which workers will execute task.
        """
        raise NotImplementedError('Must be implemented in subclasses.')


class BaseTask(object):
    def __init__(self, *arsg, **kwargs):
        """Create new instance of task.

        :param arsg: Sequential arguments for task. Will be passed to run() method.
        :param kwargs: Keyword arguments for task. Will be passed to run() method.
        """
        self.args = arsg
        self.kwargs = kwargs
        self.id = ''

    @classmethod
    def get_fqname(cls):
        """Return fully qualified class name of task.
        """
        return '%s.%s' % (cls.__module__, cls.__name__)

    def run(self, *args, **kwargs):
        """Execute exact work. Should be overridden in subclasses.
        WARNING: Do not call this method directly, instead use apply() method.

        :param arsg: Sequential arguments for task.
        :param kwargs: Keyword arguments for task.
        """
        raise NotImplementedError('Must be implemented in subclasses.')

    def apply(self, *args, **kwargs):
        """Run task for execution and return its result.

        :param arsg: Sequential arguments for task. Will override default arguments passed to __init__ if specified.
        :param kwargs: Keyword arguments for task. Will override default arguments passed to __init__ if specified.
        """
        if args or kwargs:
            return self.run(*args, **kwargs)
        else:
            return self.run(*self.args, **self.kwargs)


class TaskQueue(object):
    def __init__(self, backend):
        """Create new instance of task queue.

        :param backend: Instance of subclass of BaseMessageQueue.
        """
        self.backend = backend
        self._task_cache = {}

    def add_task_by_name(self, name, *args, **kwargs):
        """Add new task to queue by its name and arguments.

        :param name: Task name.
        :param routing_key: Routing key allowing to control which workers will execute task.
        :param args: Sequential arguments for task.
        :param kwargs: Keyword arguments for task.
        """
        routing_key = kwargs.pop('routing_key', DEFAULT_ROUTING_KEY)
        message = Message(body={
            'name': name,
            'args': args,
            'kwargs': kwargs,
        })
        return self.backend.publish(message, routing_key).id

    def add_task(self, task, routing_key=DEFAULT_ROUTING_KEY):
        """Add new task to queue.

        :param task: Fully initialized and ready to run instance of subclass of BaseTask.
        :param routing_key: Routing key allowing to control which workers will execute task.
        """
        task.id = self.add_task_by_name(task.get_fqname(), routing_key=routing_key, *task.args, **task.kwargs)
        return task

    def done(self, task):
        """Report that task successfully done.

        :param task: Instance of BaseTask subclass or task id.
        """
        task_id = task.id if isinstance(task, BaseTask) else task
        raise self.backend.acknowledge(task_id)

    @staticmethod
    def _load_cls(name):
        mod_name, cls_name = name.rsplit('.', 1)
        mod = __import__(mod_name, globals(), locals(), fromlist=[cls_name])
        return getattr(mod, cls_name)

    def _load_task(self, name):
        if name not in self._task_cache.keys():
            self._task_cache[name] = self._load_cls(name)
        return self._task_cache[name]

    def _task_from_message(self, message):
        cls = self._load_task(message.body['name'])
        task = cls(*message.body['args'], **message.body['kwargs'])
        task.id = message.id
        return task

    def consume(self, routing_key=DEFAULT_ROUTING_KEY):
        """Return generator fetching Tasks from queue.

        :param routing_key: Routing key allowing to control which workers will execute task.
        """
        return (self._task_from_message(m) for m in self.backend.consume(routing_key))
