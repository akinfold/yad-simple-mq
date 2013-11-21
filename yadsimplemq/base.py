# -*- coding: utf-8 -*-
DEFAULT_ROUTING_KEY = 'yadsimplemq.tasks'


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

    def publish(self, message, routing_key=DEFAULT_ROUTING_KEY):
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


class BaseTask(object):
    def __init__(self, *arsg, **kwargs):
        """Create new instance of task.

        :param arsg: Sequential arguments for task. Will be passed to run() method.
        :param kwargs: Keyword arguments for task. Will be passed to run() method.
        """
        self.args = arsg
        self.kwargs = kwargs

    @classmethod
    def get_fqname(cls):
        """Return fully qualified class name of task.
        """
        return '%s.%s' % (cls.__module__, cls.__name__)

    def _run(self, *args, **kwargs):
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
            return self._run(*args, **kwargs)
        else:
            return self._run(*self.args, **self.kwargs)


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
        message = {
            'name': name,
            'args': args,
            'kwargs': kwargs,
            'in_progress': False,
            'in_progress_since': None,
            'done': False,
            'done_since': None,
        }
        self.backend.publish(message, routing_key)

    def add_task(self, task, routing_key=DEFAULT_ROUTING_KEY):
        """Add new task to queue.

        :param task: Fully initialized and ready to run instance of subclass of BaseTask.
        :param routing_key: Routing key allowing to control which workers will execute task.
        """
        self.add_task_by_name(task.get_fqname(), routing_key, *task.args, **task.kwargs)

    def raw_fetch(self, routing_key=DEFAULT_ROUTING_KEY):
        """Fetch and return raw task from queue.

        :param routing_key: Routing key allowing to control which workers will execute task.
        """
        return self.backend.fetch(routing_key)

    @staticmethod
    def _load_cls(name):
        mod_name, cls_name = name.rsplit('.', 1)
        mod = __import__(mod_name, globals(), locals(), fromlist=[cls_name])
        return getattr(mod, cls_name)

    def _load_task(self, name):
        if name not in self._task_cache.keys():
            self._task_cache[name] = self._load_cls(name)
        return self._task_cache[name]

    def fetch_task(self, routing_key=DEFAULT_ROUTING_KEY):
        """Fetch task and return its initialized instance.
        """
        raw_task = self.raw_fetch(routing_key)
        cls = self._load_task(raw_task['name'])
        return cls(raw_task['args'], raw_task['kwargs'])
