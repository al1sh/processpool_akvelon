from main import ProcessPool
from multiprocessing import Pool, cpu_count
import time


def test_function(x):
    time.sleep(x)
    return x**10


iterations = 1, 10, 100
wait_times = [0.001, 0.005, 0.01, 0.05]
for iteration in iterations:
    print("\nRunning with {} iterations".format(str(iteration)))
    for wait_time in wait_times:
        print("\n\tRunning with {}s wait time".format(str(wait_time)))
        # *** Consecutive execution
        start = time.time()
        for i in range(iteration):
            test_function(wait_time)
        end = time.time() - start
        print("\t Consecutive: ", str(end))


        # *** multiprocessing.Pool ***
        start = time.time()
        pool = Pool(processes=cpu_count() * 2)
        powers = pool.map(test_function, [wait_time]*iteration)
        end = time.time() - start
        print("\t multiprocessing.Pool: ", str(end))


        # *** ProcessPool implementation***
        start = time.time()
        ProcessPool.initialize()
        for i in range(iteration):
            ProcessPool.process_task(test_function, wait_time)
        ProcessPool.kill_if_empty()
        end = time.time() - start
        print("\t ProcessPool implementation: ", str(end), '\n')



