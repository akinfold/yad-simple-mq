# -*- coding: utf-8 -*-
import time
from yadsimplemq.base import BaseTask


class CountdownTask(BaseTask):
    def run(self, n):
        while n > 0:
            n -= 1


class SleepTask(BaseTask):
    def run(self):
        time.sleep(5)
