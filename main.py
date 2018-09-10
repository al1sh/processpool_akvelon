from multiprocessing import Process, Manager, Lock, Value, cpu_count
import os
import signal
from time import sleep


class ProcessPool:
    _process_list = []
    _task_count = 0
    is_active = False
    process_limit = cpu_count() * 2

    @staticmethod
    def _worker(current_queue, lock, queue_count):
        """Spawns worker process."""
        while True:
            if not current_queue.empty():
                with lock:
                    # get function and arguments from queue
                    func, *args = current_queue.get()
                    queue_count.value -= 1
                # execute the function
                if args:
                    func(*args)
                else:
                    func()

    @staticmethod
    def process_task(func, *args):
        """Gets the process with least tasks and assigns it a new task"""
        free_process = min(ProcessPool._process_list, key=lambda x: x['task_count'].value)
        with free_process['lock']:
            free_process['queue'].put((func, *args))
            free_process['task_count'].value += 1

    @staticmethod
    def kill_if_empty():
        """Waits until task count for all processes is 0 and kills them"""
        while True:
            is_all_empty = True
            for process in ProcessPool._process_list:
                if process['task_count'].value == 0:
                    continue
                else:
                    is_all_empty = False
                    sleep(0.01)
                    break

            if is_all_empty:
                break

        for process in ProcessPool._process_list:
            pid = process['process_object'].pid
            os.kill(pid, signal.SIGTERM)

    @staticmethod
    def _add_process(info):
        """Add new process to worker pool"""
        ProcessPool._process_list.append(info)

    @staticmethod
    def initialize():
        """Initialize processes for task handling"""
        ProcessPool._process_list = []

        for i in range(ProcessPool.process_limit):
            current_queue = Manager().Queue()
            lock = Lock()
            task_count = Value('i', 0)
            p = Process(target=ProcessPool._worker, args=(current_queue, lock, task_count))
            info = {"process_object": p, "queue": current_queue, 'lock': lock, 'task_count': task_count}
            ProcessPool._add_process(info)

        ProcessPool.start_processes()
        ProcessPool.is_active = True

    @staticmethod
    def wait_all():
        for process in ProcessPool._process_list:
            process['process_object'].join()

    def start_processes():
        for process in ProcessPool._process_list:
            process['process_object'].start()





