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
from swift.common.concurrency import Pool


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
