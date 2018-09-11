import os
import signal
from multiprocessing import Process, Value, cpu_count, SimpleQueue
from time import sleep


class ProcessPool:
    _process_list = []
    is_active = False
    process_limit = cpu_count() * 2

    class ProcessInfo:
        def __init__(self, id, process_object, queue, task_count):
            self.id = id
            self.process_object = process_object
            self.queue = queue
            self.task_count = task_count

    def _worker(current_queue, queue_count):
        """Spawns worker process."""
        while True:
            if queue_count.value == 0:
                sleep(0.00001)
                continue

            # get function and arguments from queue
            func, *args = current_queue.get()
            # execute the function
            if args:
                func(*args)
            else:
                func()

            with queue_count.get_lock():
                queue_count.value -= 1

    @staticmethod
    def initialize():
        """Initialize processes for task handling"""
        ProcessPool._process_list = []

        for i in range(ProcessPool.process_limit):
            current_queue = SimpleQueue()
            task_count = Value('i', 0)
            p = Process(target=ProcessPool._worker, args=(current_queue, task_count))
            info = ProcessPool.ProcessInfo(i, p, current_queue, task_count)
            ProcessPool._add_process(info)

        ProcessPool.start_processes()
        ProcessPool.is_active = True

    def _add_process(info):
        """Add new process to worker pool"""
        ProcessPool._process_list.append(info)

    @staticmethod
    def process_task(func, *args):
        """Gets the process with least tasks and assigns it a new task"""
        if not ProcessPool.is_active:
            ProcessPool.initialize()

        free_process = min(ProcessPool._process_list, key=lambda x: x.task_count.value)

        free_process.queue.put((func, *args))

        with free_process.task_count.get_lock():
            free_process.task_count.value += 1


    @staticmethod
    def wait_queues():
        """Waits until tasks for all processes finish"""
        for process in ProcessPool.get_process_list():
            while process.task_count.value != 0 or not process.queue.empty():
                sleep(0.00001)

    @staticmethod
    def kill_processes():
      for process in ProcessPool._process_list:
        pid = process.process_object.pid
        os.kill(pid, signal.SIGTERM)

    @staticmethod
    def get_process_list():
        return ProcessPool._process_list

    @staticmethod
    def kill_empty():
        ProcessPool.wait_queues()
        ProcessPool.kill_processes()

    @staticmethod
    def wait_all():
        for process in ProcessPool._process_list:
            process.process_object.join()

    @staticmethod
    def start_processes():
        for process in ProcessPool._process_list:
            process.process_object.start()
