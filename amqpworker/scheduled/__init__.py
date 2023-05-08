"""The MIT License (MIT)

Copyright (c) 2021 Yogaraj.S

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE."""

import threading
import traceback
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Callable

from delayedqueue import DelayedQueue
from loguru import logger

"""Wrapper for the task submitted to ScheduledThreadPoolExecutor class"""

import time
from typing import Callable


class ScheduledTask:
    def __init__(self, runnable: Callable, initial_delay: int, period: int, *args, time_func=time.time, **kwargs):
        super().__init__()
        self.runnable = runnable
        self.initial_delay = initial_delay
        self.period = period
        self.__time_func = time_func
        self.args = args
        self.kwargs = kwargs
        self.__is_initial = True
        self.task_time = int(self.__time_func() * 1000) + (initial_delay * 1000)

    @property
    def is_initial_run(self) -> bool:
        return self.__is_initial

    @property
    def at_fixed_delay(self) -> bool:
        return self.kwargs.get('is_fixed_delay', False)

    @property
    def at_fixed_rate(self) -> bool:
        return self.kwargs.get('is_fixed_rate', False)

    @property
    def executor_ctx(self):
        return self.kwargs['executor_ctx']  # pragma: no cover

    @property
    def exception_callback(self):
        return self.kwargs.get('on_exception_callback')

    @property
    def time_func(self):
        return self.__time_func

    def __get_next_run(self) -> int:
        if not (self.at_fixed_rate or self.at_fixed_delay):
            raise TypeError("`get_next_run` invoked in a non-repeatable task")
        return int(self.__time_func() * 1000) + self.period * 1000

    def set_next_run(self, time_taken: float = 0) -> None:
        self.__is_initial = False
        self.task_time = self.__get_next_run() - time_taken

    def __lt__(self, other) -> bool:
        if not isinstance(other, ScheduledTask):
            raise TypeError(f"{other} is not of type ScheduledTask")
        return self.task_time < other.task_time

    def __repr__(self) -> str:
        return f"""(Task: {self.runnable.__name__}, Initial Delay: {self.initial_delay} second(s), Periodic: {self.period} second(s), Next run: {time.ctime(self.task_time / 1000)})"""

    def run(self, *args, **kwargs):
        st_time = time.time_ns()
        try:
            self.runnable(*self.args, **self.kwargs)
        except Exception as e:
            if self.exception_callback:
                self.exception_callback(e, *self.args, **self.kwargs)
            else:
                traceback.print_exc()
        finally:
            end_time = time.time_ns()
            time_taken = (end_time - st_time) / 1000000  # in milliseconds
            if self.at_fixed_rate:
                self.set_next_run(time_taken)
                next_delay = (self.period * 1000 - time_taken) / 1000
                if next_delay < 0 or self.task_time <= (self.__time_func() * 1000):
                    self.executor_ctx._put(self, 0)
                else:
                    self.executor_ctx._put(self, next_delay)
            elif self.at_fixed_delay:
                self.set_next_run()
                self.executor_ctx._put(self, self.period)


class ScheduledThreadPoolExecutor(ThreadPoolExecutor):
    def __init__(self, max_workers=10, name=''):
        super().__init__(max_workers=max_workers, thread_name_prefix=name)
        self._max_workers = max_workers
        self.queue = DelayedQueue()
        self.shutdown = False

    def schedule_at_fixed_rate(self, fn: Callable, initial_delay: int, period: int, *args, **kwargs) -> bool:
        if self.shutdown:
            raise RuntimeError(f"cannot schedule new task after shutdown!")
        task = ScheduledTask(fn, initial_delay, period, *args, is_fixed_rate=True, executor_ctx=self, **kwargs)
        return self._put(task, initial_delay)

    def schedule_at_fixed_delay(self, fn: Callable, initial_delay: int, period: int, *args, **kwargs) -> bool:
        if self.shutdown:
            raise RuntimeError(f"cannot schedule new task after shutdown!")
        task = ScheduledTask(fn, initial_delay, period, *args, is_fixed_delay=True, executor_ctx=self, **kwargs)
        return self._put(task, initial_delay)

    def schedule(self, fn, initial_delay, *args, **kwargs) -> bool:
        task = ScheduledTask(fn, initial_delay, 0, *args, executor_ctx=self, **kwargs)
        return self._put(task, initial_delay)

    def _put(self, task: ScheduledTask, delay: int) -> bool:
        # Don't use this explicitly. Use schedule/schedule_at_fixed_delay/schedule_at_fixed_rate. Additionally, to be
        # called by ScheduledTask only!
        if not isinstance(task, ScheduledTask):
            raise TypeError(f"Task `{task!r}` must be of type ScheduledTask")
        if delay < 0:
            raise ValueError(f"Delay `{delay}` must be a non-negative number")
        # logger.debug(f" enqueuing {task} with delay of {delay}")
        return self.queue.put(task, delay)

    def __run(self):
        while not self.shutdown:
            try:
                task: ScheduledTask = self.queue.get()
                super().submit(task.run, *task.args, **task.kwargs)
            except Exception as e:
                print(e)

    def stop(self, app, wait_for_completion: Optional[bool] = False):
        self.shutdown = True
        super().shutdown(wait_for_completion)

    def start(self, app):
        t = threading.Thread(target=self.__run)
        t.setDaemon(True)
        t.start()
