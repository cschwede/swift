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
from swift.common.concurrency import spawn


class TestSpawn(unittest.TestCase):
    def test_with_args(self):
        f = lambda x, y: x * y
        result = spawn(f, 6, 7)
        self.assertEqual(result.wait(), 42)

    def test_with_kwargs(self):
        def dummy(a, b=1):
            return (a, b)

        result = spawn(dummy, 0, b=2)
        self.assertEqual(result.wait(), (0, 2))

    def test_exception(self):
        def fail():
            raise Exception('reason')

        result = spawn(fail)
        with self.assertRaises(Exception) as ctx:
            result.wait()
        self.assertEqual(str(ctx.exception), 'reason')


if __name__ == '__main__':
    unittest.main()
