# -*- coding: utf-8 -*-
from Queue import Empty, Full
import logging
import multiprocessing
import time
import signal
from yadsimplemq.base import DEFAULT_ROUTING_KEY


def _run_process(task_queue, free_procs, maxtasks=None):
    tasks_counter = 0
    proc = multiprocessing.current_process()
    task = task_queue.get(True)
    # task != signal.SIGTERM
    while task != signal.SIGTERM and (maxtasks is None or tasks_counter < maxtasks):
        logging.debug('Run task [%s] by process #%s' % (str(task.id), proc.name))
        try:
            task.apply()
        except Exception, e:
            logging.error('Task [%s] raised %s: %s' % (str(task.id), type(e).__name__, e.message))
        finally:
            tasks_counter += 1
            logging.debug('Task [%s] complete by process #%s (%d/%d)' % (str(task.id), proc.name, tasks_counter, maxtasks or -1))
            task_queue.task_done()
            free_procs.put(proc.name)
            logging.debug('Process #%s is free' % proc.name)
            task = task_queue.get(True)
    task_queue.task_done()


class Worker(object):
    def __init__(self, task_queue, max_procs=None, routing_key=None, maxtasksperchild=None, delay=0.05):
        """Create new worker.

        :param task_queue: TaskQueue to get tasks from.
        :param max_procs: Number of spawned processes, is not specified user cpu_count() method to get value.
        :param routing_key: Routing key allowing to control which workers will execute task.
        :param maxtasksperchild: Number of tasks per process, after which process will be replaced with new one.
        """
        self.task_queue = task_queue
        self.routing_key = routing_key or DEFAULT_ROUTING_KEY
        self.maxtasksperchild = maxtasksperchild
        self.delay = delay
        if max_procs:
            self.max_processes = max_procs
        else:
            try:
                self.max_processes = multiprocessing.cpu_count()
            except NotImplementedError:
                self.max_processes = 1
        logging.debug('Max processes: %d' % self.max_processes)

        self._manager = None
        self._task_queue = None
        self._free_procs = None
        self._procs = []
        self._is_active = False

    def _refill_procs(self):
        self._procs[:] = [p for p in self._procs if p.is_alive()]
        if len(self._procs) < self.max_processes:
            logging.debug('Respawn %d processes of %d' % (self.max_processes - len(self._procs), self.max_processes))
            while len(self._procs) < self.max_processes:
                proc = multiprocessing.Process(target=_run_process,
                                               args=(self._task_queue, self._free_procs, self.maxtasksperchild))
                self._procs.append(proc)
                proc.start()
                wait = True
                while wait:
                    try:
                        self._free_procs.put_nowait(proc.name)
                        wait = False
                    except Full:
                        time.sleep(self.delay)
            logging.debug('Respawn complete. Currently up %d/%d processes' % (len(self._procs), self.max_processes))

    def run(self):
        self._manager = multiprocessing.Manager()
        self._task_queue = self._manager.Queue(self.max_processes)
        self._free_procs = self._manager.Queue(self.max_processes)
        self._is_active = True

        while True:
            tasks = self.task_queue.consume(self.routing_key)
            self._refill_procs()
            proc_name = None
            try:
                while True:
                    try:
                        proc_name = self._free_procs.get_nowait()
                        task = tasks.next()
                        logging.debug('Got task %s [%s]' % (task.get_fqname(), str(task.id)))
                        wait = True
                        while wait:
                            try:
                                self._task_queue.put_nowait(task)
                                wait = False
                            except Full:
                                time.sleep(self.delay)
                    except Empty:
                        time.sleep(self.delay)
                        self._refill_procs()
            except StopIteration:
                self._free_procs.put(proc_name)
                time.sleep(self.delay)

        self._is_active = False
