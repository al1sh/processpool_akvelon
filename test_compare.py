from process_pool import ProcessPool
from multiprocessing import Pool, cpu_count
import time


def test_function(x):
    time.sleep(x)
    return x**10


def main():
    iterations = 10, 100, 1000
    wait_times = [0.01, 0.001, 0.0001]
    for iteration in iterations:
        print("\nRunning with {} iterations".format(str(iteration)))
        for wait_time in wait_times:
            print("\n\tRunning with {}s wait time".format(str(wait_time)))
            # *** Consecutive execution
            start = time.time()
            for i in range(iteration):
                test_function(wait_time)
            end = time.time() - start
            print("\t\tConsecutive: ", str(end))

            # *** multiprocessing.Pool ***
            start = time.time()
            pool = Pool(processes=cpu_count() * 2)
            powers = pool.map(test_function, [wait_time]*iteration)
            end = time.time() - start
            print("\t\tmultiprocessing.Pool: ", str(end))

            # *** ProcessPool implementation***
            ProcessPool.initialize()
            start = time.time()
            for i in range(iteration):
                ProcessPool.process_task(test_function, wait_time)
            ProcessPool.wait_queues()
            end = time.time() - start
            ProcessPool.kill_processes()
            print("\t\tProcessPool implementation: ", str(end), '\n')


if __name__ == "__main__":
    main()
