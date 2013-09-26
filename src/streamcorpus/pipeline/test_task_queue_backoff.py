from _task_queues import ZookeeperTaskQueue

class TestableZookeeperTaskQueue(ZookeeperTaskQueue):

    """ Undefine everything we don't intend to test and log all the time spent sleeping"""

    def __init__(self):
        self.sleeping = []
        self.tasks = [ (0, None) for x in xrange(0,8) ] + [(1, "Test Done")]
        self.data = {}
        self._continue_running = True
        pass

    def _register(self):
        pass

    def commit(self):
        pass

    def _random_available_task(self):
        return "1"

    def _read_mode(self):
        return "TEST"

    def _backoff(self, time):
        self.sleeping.append(time)

    def _win_task(self, key):
        return self.tasks.pop(0)


def test_iter_backoff():
    tq = TestableZookeeperTaskQueue()
    it = iter(tq)
    end_count, i_str, data = it.next()
    assert i_str == "Test Done"
    # Specification is to sleep for upto 128 seconds with exponential backoff starting at 2 seconds
    assert tq.sleeping == [2, 4, 8, 16, 32, 64, 128, 128]
     

    
