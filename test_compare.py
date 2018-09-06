from main import ProcessPool
from multiprocessing import Pool, cpu_count
import time

def test_function(x, n=5):
    return x**n


start = time.time()
pool = Pool(processes=cpu_count() * 2)
powers = pool.map(test_function, range(100))
print(powers)
end = time.time() - start
print("multiprocessing.Pool: ", str(end))

start = time.time()
for i in range(100):
    ProcessPool.process_task(test_function, i)
end = time.time() - start
ProcessPool.wait_all()
print("ProcessPool: ", str(end))

