from multiprocessing import Process, Manager, Lock, cpu_count


class ProcessPool:

    process_list = []
    is_active = False

    @staticmethod
    def worker(current_queue, lock):
        import os

        while True:
            if current_queue.empty():
                print("pid " + str(os.getpid()) + " sleeping, waiting for tasks")
                sleep(5)
            else:
                print("pid " + str(os.getpid()) + " received new task")
                lock.acquire()
                # get function and arguments from queue
                func, *args = current_queue.get()
                lock.release()
                # execute the function
                if args:
                    func(*args)
                else:
                    func()


    @staticmethod
    def process_task(func, *args):
        # initialize working processes
        if not ProcessPool.is_active:
            process_limit = cpu_count() * 2

            for i in range(process_limit):
                current_queue = Manager().Queue()
                lock = Lock()
                p = Process(target=ProcessPool.worker, args=(current_queue, lock))
                ProcessPool.process_list.append({"process_object": p, "queue": current_queue, 'lock': lock})

            for process in ProcessPool.process_list:
                process['process_object'].start()

            ProcessPool.is_active = True

        # get the least busy process. acquire locks to check queue for each process
        for process in ProcessPool.process_list:
            process['lock'].acquire()

        free_process = min(ProcessPool.process_list, key=lambda x: x['queue'].qsize())

        for process in ProcessPool.process_list:
            process['lock'].release()

        free_process_lock = free_process['lock']
        free_process_lock.acquire()
        free_process['queue'].put((func, *args))
        free_process_lock.release()


if __name__ == "__main__":
    from time import sleep

    # function for working processes
    def sleep_task(sec):
        sleep(sec)

    run_count = 24
    for i in range(run_count):
        ProcessPool.process_task(sleep_task, 5)

    for process in ProcessPool.process_list:
        process['process_object'].join()

    print("main process exited")




