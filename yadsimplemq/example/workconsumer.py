# -*- coding: utf-8 -*-
import os
import sys
sys.path.append(os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))

import logging
from yadsimplemq.base import TaskQueue
from yadsimplemq.example import settings
from yadsimplemq.mongo import MongoMessageQueue
from yadsimplemq.worker import Worker

logging.basicConfig(level=logging.DEBUG)

if __name__ == '__main__':
    mq = MongoMessageQueue(broker_url=settings.BROKER_URI, exchange=settings.EXCHANGE)
    tq = TaskQueue(mq)
    w = Worker(tq, max_procs=2, maxtasksperchild=10)
    w.run()
