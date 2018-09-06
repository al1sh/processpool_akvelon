from multiprocessing import Process, Manager, Lock, Value, cpu_count


class ProcessPool:
    process_list = []
    is_active = False
    process_limit = cpu_count() * 2

    class ProcessWithCount(Process):
        def __init__(self, **kwargs):
            Process.__init__(self, **kwargs)
            self.task_count = Value('i', 0)


    @staticmethod
    def worker(current_queue, lock):
        import os

        while True:
            if current_queue.empty():
                print("pid " + str(os.getpid()) + " sleeping, waiting for tasks")
                sleep(5)
            else:
                with lock:
                    print("pid " + str(os.getpid()) + " received new task")
                    # get function and arguments from queue
                    func, *args = current_queue.get()

                # execute the function
                if args:
                    func(*args)
                else:
                    func()


    @staticmethod
    def process_task(func, *args):
        if not ProcessPool.is_active:
            ProcessPool.initialize()

        # get the least busy process. acquire locks to check queue for each process
        free_process = min(ProcessPool.process_list, key=lambda x: x['process_object'].task_count.value)
        free_process['queue'].put((func, *args))
        free_process['process_object'].task_count.value += 1

        # for process in ProcessPool.process_list:
        #     process['lock'].acquire()
        #
        # free_process = min(ProcessPool.process_list, key=lambda x: x['queue'].qsize())
        #
        # for process in ProcessPool.process_list:
        #     process['lock'].release()
        #
        # free_process_lock = free_process['lock']
        # free_process_lock.acquire()
        # free_process['queue'].put((func, *args))
        # free_process_lock.release()

    def add_process(info):
        ProcessPool.process_list.append(info)

    @staticmethod
    def initialize():
        if not ProcessPool.is_active:

            for i in range(ProcessPool.process_limit):
                current_queue = Manager().Queue()
                lock = Lock()
                p = ProcessPool.ProcessWithCount(target=ProcessPool.worker, args=(current_queue, lock))
                info = {"process_object": p, "queue": current_queue, 'lock': lock}
                ProcessPool.add_process(info)

            ProcessPool.start_processes()
            ProcessPool.is_active = True

    @staticmethod
    def wait_all():
        for process in ProcessPool.process_list:
            process['process_object'].join()

    def start_processes():
        for process in ProcessPool.process_list:
            process['process_object'].start()



if __name__ == "__main__":
    from time import sleep

    # function for working processes
    def sleep_task(sec):
        sleep(sec)

    run_count = 24
    for i in range(run_count):
        ProcessPool.process_task(sleep_task, 5)

    ProcessPool.wait_all()

    print("main process exited")




