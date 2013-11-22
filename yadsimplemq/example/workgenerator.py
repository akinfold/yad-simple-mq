# -*- coding: utf-8 -*-
import os
import sys
sys.path.append(os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))

from yadsimplemq.base import TaskQueue
from yadsimplemq.example import settings
from yadsimplemq.example.tasks import CountdownTask
from yadsimplemq.mongo import MongoMessageQueue

if __name__ == '__main__':
    mq = MongoMessageQueue(broker_url=settings.BROKER_URI, exchange=settings.EXCHANGE)
    mq.connect()
    # if we run example 2 or more times we want clean collection
    mq._db.drop_collection(settings.EXCHANGE)
    tq = TaskQueue(mq)
    for i in xrange(1000):
        print 'Added task #%d [%s]' % (i, tq.add_task(CountdownTask(1000000)).id)
