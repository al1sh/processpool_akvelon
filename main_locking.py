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
    def _worker(current_queue, lock):
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

    @staticmethod
    def process_task(func, *args):
        ProcessPool._task_count += 1
        # get the least busy process. acquire locks to check queue for each process
        for process in ProcessPool._process_list:
            process['lock'].acquire()

        free_process = min(ProcessPool._process_list, key=lambda x: x['queue'].qsize())
        free_process['queue'].put((func, *args))

        for process in ProcessPool._process_list:
            process['lock'].release()
        ProcessPool._task_count -= 1

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
        ProcessPool._process_list.append(info)

    @staticmethod
    def initialize():
        ProcessPool._process_list = []

        for i in range(ProcessPool.process_limit):
            current_queue = Manager().Queue()
            lock = Lock()
            task_count = Value('i', 0)
            p = Process(target=ProcessPool._worker, args=(current_queue, lock))
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


if __name__ == "__main__":
    from time import sleep

    # function for working processes
    def sleep_task(sec):
        print("task is loading")
        sleep(sec)

    run_count = 24
    for i in range(run_count):
        ProcessPool.process_task(sleep_task, 5)

    ProcessPool.kill_if_empty()

    print("main process exited")



