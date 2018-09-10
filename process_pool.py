import os
import signal
from multiprocessing import Process, Manager, Lock, Value, cpu_count


class ProcessPool:
    _process_list = []
    is_active = False
    process_limit = cpu_count() * 2

    class ProcessInfo:
        def __init__(self, process_object, queue, lock, task_count):
            self.process_object = process_object
            self.queue = queue
            self.lock = lock
            self.task_count = task_count

    @staticmethod
    def initialize():
        """Initialize processes for task handling"""
        ProcessPool._process_list = []

        for i in range(ProcessPool.process_limit):
            current_queue = Manager().Queue()
            lock = Lock()
            task_count = Value('i', 0)
            p = Process(target=ProcessPool._worker, args=(current_queue, lock, task_count))
            info = ProcessPool.ProcessInfo(p, current_queue, lock, task_count)
            ProcessPool._add_process(info)

        ProcessPool._start_processes()
        ProcessPool.is_active = True

    @staticmethod
    def process_task(func, *args):
        """Gets the process with least tasks and assigns it a new task"""
        if not ProcessPool.is_active:
            ProcessPool.initialize()

        # returns process with minimum task count
        free_process = min(ProcessPool._process_list, key=lambda x: x.task_count.value)

        with free_process.lock:
            free_process.queue.put((func, *args))
            free_process.task_count.value += 1

    @staticmethod
    def kill_empty():
        """Waits until tasks for all processes finish and then terminates them"""
        for process in ProcessPool.get_process_list():
            process.queue.join()

        for process in ProcessPool._process_list:
            pid = process.process_object.pid
            os.kill(pid, signal.SIGTERM)

    @staticmethod
    def wait_all():
        for process in ProcessPool._process_list:
            process.process_object.join()

    @staticmethod
    def get_process_list():
        return ProcessPool._process_list

    @staticmethod
    def _start_processes():
        for process in ProcessPool._process_list:
            process.process_object.start()

    @staticmethod
    def _add_process(info):
        """Add new process to worker pool"""
        ProcessPool._process_list.append(info)

    @staticmethod
    def _worker(current_queue, lock, queue_count):
        """Spawns worker process."""
        while True:
            if not current_queue.empty():
                with lock:
                    # get function and arguments from queue
                    func, *args = current_queue.get()
                # execute the function
                if args:
                    func(*args)
                else:
                    func()

                current_queue.task_done()
                queue_count.value -= 1
