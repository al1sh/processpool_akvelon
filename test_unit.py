from process_pool import ProcessPool
import unittest
import multiprocessing
import psutil
import time


def test_function(x):
    time.sleep(x)


class ProcessPoolTests(unittest.TestCase):
    def test_correct_process_count(self):
        ProcessPool.initialize()
        self.assertEqual(len(ProcessPool.get_process_list()), multiprocessing.cpu_count()*2)
        ProcessPool.kill_empty()

    def test_correct_child_process_creation(self):
        ProcessPool.initialize()
        children_list = psutil.Process().children()
        children_pids = [children.pid for children in children_list]

        process_list = ProcessPool.get_process_list()
        pid_list = [process.process_object.pid for process in process_list]
        for pid in pid_list:
            self.assertTrue(pid in children_pids)
        ProcessPool.kill_empty()

    def test_all_children_terminate(self):
        ProcessPool.initialize()
        process_list = ProcessPool.get_process_list()
        pid_list = [process.process_object.pid for process in process_list]

        ProcessPool.kill_empty()
        time.sleep(1)

        children_list = psutil.Process().children()

        for pid in pid_list:
            for children_process in children_list:
                if pid == children_process.pid:
                    self.assertTrue(children_process.status() == 'zombie')

    def test_queue_is_filled_correctly(self):

        ProcessPool.initialize()
        process_list = ProcessPool.get_process_list()

        for process in process_list:
            self.assertEqual(process.task_count.value, process.queue.qsize(), 0)

        sleep_sec = 0.1
        for i in range(multiprocessing.cpu_count() * 4):
            ProcessPool.process_task(test_function, sleep_sec)

        for process in process_list:
            self.assertEqual(process.task_count.value, 2)

        time.sleep(sleep_sec+sleep_sec/2)

        for process in process_list:
            self.assertEqual(process.task_count.value, 1)

        time.sleep(sleep_sec+sleep_sec/2)

        for process in process_list:
            self.assertEqual(process.task_count.value, 0)

        ProcessPool.kill_empty()


if __name__ == '__main__':
    unittest.main()