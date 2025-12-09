# Copyright (c) 2010-2026 OpenStack Foundation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest

import threading
from swift.common.concurrency import Pool, tpool, SwiftPool, SwiftPile
from time import sleep


class TestPool(unittest.TestCase):

    def _make_pool(self, max_size=2, free_items=None):
        class TestPool(Pool):
            created = 0

            def create(self):
                TestPool.created += 1
                return TestPool.created

        pool = TestPool(max_size=max_size)
        if free_items:
            for item in free_items:
                pool.free_items.append(item)
        return pool

    def test_get_creates_item(self):
        pool = self._make_pool(max_size=2)
        item = pool.get()
        self.assertEqual(item, 1)

    def test_get_returns_item(self):
        pool = self._make_pool(max_size=2, free_items=['cached'])
        item = pool.get()
        self.assertEqual(item, 'cached')

    def test_put_then_get(self):
        pool = self._make_pool(max_size=2)
        pool.put('returned')
        item = pool.get()
        self.assertEqual(item, 'returned')

    def test_concurrent_get_put(self):
        pool = self._make_pool(max_size=2)
        results = []
        errors = []

        def worker():
            try:
                item = pool.get()
                results.append(item)
                pool.put(item)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5)

        self.assertFalse(errors)
        self.assertEqual(len(results), 4)

    def test_put_notifies_waiting_get(self):
        pool = self._make_pool(max_size=1)
        first = pool.get()  # exhaust pool
        result = []
        e = threading.Event()

        def getter():
            e.set()
            result.append(pool.get())

        t = threading.Thread(target=getter)
        t.start()
        e.wait()
        self.assertTrue(t.is_alive())
        self.assertEqual(result, [])
        pool.put(first)  # return item, unblock getter
        t.join(timeout=5)
        self.assertEqual(result, [first])


class TestTpool(unittest.TestCase):
    def test_with_args(self):
        f = lambda x, y: x * y
        result = tpool.execute(f, 6, 7)
        self.assertEqual(result, 42)

    def test_with_kwargs(self):
        def dummy(a, b=1):
            return (a, b)

        result = tpool.execute(dummy, 0, b=2)
        self.assertEqual(result, (0, 2))

    def test_exception(self):
        class DummyException(Exception):
            pass

        def fail():
            raise DummyException('reason')

        with self.assertRaises(DummyException):
            tpool.execute(fail)


class TestSwiftPool(unittest.TestCase):
    def test_waitall(self):
        results = []

        def append_val(val):
            results.append(val)

        pool = SwiftPool(size=4)
        for i in range(5):
            pool.spawn_n(append_val, i)
        pool.waitall()
        self.assertEqual(sorted(results), [0, 1, 2, 3, 4])
        self.assertEqual(pool.futures, [])
        pool.shutdown(wait=True)

    def test_free_and_running(self):
        pool = SwiftPool(size=4)
        # Before any tasks, free should equal size
        self.assertEqual(pool.free(), 4)
        self.assertEqual(pool.running(), 0)
        pool.shutdown(wait=True)

    def test_imap_returns_results_in_order(self):
        def double(x):
            return x * 2

        pool = SwiftPool(size=4)
        results = list(pool.imap(double, [1, 2, 3, 4, 5]))
        self.assertEqual(results, [2, 4, 6, 8, 10])
        pool.shutdown(wait=True)

    def test_starmap_returns_results_in_order(self):
        def multiply(a, b):
            return a * b

        pool = SwiftPool(size=4)
        results = list(pool.starmap(multiply, [(2, 3), (4, 5), (6, 7)]))
        self.assertEqual(results, [6, 20, 42])
        pool.shutdown(wait=True)

    def test_runs_in_separate_thread(self):
        main_thread_id = threading.current_thread().ident
        pool = SwiftPool(size=2)
        future = pool.spawn(lambda: threading.current_thread().ident)
        worker_thread_id = future.result()
        self.assertNotEqual(main_thread_id, worker_thread_id)
        pool.shutdown(wait=True)


class TestSwiftPile(unittest.TestCase):
    def test_results_in_order(self):
        # Test that a slow func result is still returned in order
        pile = SwiftPile(4)

        def slow():
            sleep(0.1)
            return 0
        pile.spawn(slow)

        for i in range(1, 5):
            pile.spawn(lambda x=i: x)

        self.assertEqual(list(pile), [0, 1, 2, 3, 4])

    def test_runs_in_separate_thread(self):
        main_thread_id = threading.current_thread().ident
        pile = SwiftPile(2)
        pile.spawn(lambda: threading.current_thread().ident)
        worker_thread_id = list(pile)[0]
        self.assertNotEqual(main_thread_id, worker_thread_id)
