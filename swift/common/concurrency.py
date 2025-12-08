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

"""Concurrency primitives for Swift.

All modules that need eventlet functionality should import from here
rather than importing directly from eventlet.
"""

import collections
from concurrent.futures import ThreadPoolExecutor, wait
import importlib.util
import os
import threading
import time
from contextlib import contextmanager
from socket import timeout as socket_timeout

# Used when reading config values
FALSE_VALUES = {'false', '0', 'no', 'off', 'f', 'n'}


def config_false_value(value):
    return value is False or \
        (isinstance(value, str) and value.lower() in FALSE_VALUES)


# Use eventlet by default if it is installed
USE_EVENTLET = importlib.util.find_spec('eventlet') is not None

# Check if eventlet is manually disabled even if installed
if USE_EVENTLET:
    if config_false_value(os.environ.get('USE_EVENTLET')):
        USE_EVENTLET = False

import eventlet  # noqa: E402
import eventlet.debug
import eventlet.greenio
import eventlet.greenthread
import eventlet.hubs
import eventlet.patcher
import eventlet.queue
import eventlet.semaphore
import eventlet.wsgi

from eventlet import GreenPile
from eventlet import greenio, greenpool, hubs, patcher, queue, wsgi
from eventlet import debug, listen, timeout, websocket
from eventlet import greenthread

from eventlet.event import Event
from eventlet.green import socket, ssl, subprocess
from eventlet.green import os as green_os
from eventlet.green import threading as green_threading
from eventlet.green.http import client as green_http_client
from eventlet.green.http.client import CONTINUE, HTTPConnection, \
    HTTPResponse, HTTPSConnection, ImproperConnectionState, _UNKNOWN
from eventlet.green.urllib import request as urllib_request
from eventlet.greenthread import getcurrent, spawn as greenthread_spawn
from eventlet.hubs import trampoline
from eventlet.queue import Empty, LightQueue, Queue
from eventlet.semaphore import Semaphore
from eventlet.support.greenlets import GreenletExit
import eventlet.green.profile as eprofile

hub_exceptions = eventlet.debug.hub_exceptions
hub_prevent_multiple_readers = eventlet.debug.hub_prevent_multiple_readers
monkey_patch = eventlet.patcher.monkey_patch
shutdown_safe = eventlet.greenio.shutdown_safe
ChunkReadError = eventlet.wsgi.ChunkReadError


if USE_EVENTLET:
    from eventlet import Timeout as _Timeout
    from eventlet import tpool
    from eventlet import GreenPool as SwiftPool
    from eventlet.pools import Pool

    class Timeout(_Timeout):
        def __init__(self, *args, **kwargs):
            # Timeout might be used with a socket keyword, which does not
            # exist in eventlet. Remove this from the list of keywords
            new_kwargs = {k: v for k, v in kwargs.items() if k != "socket"}
            super(Timeout, self).__init__(*args, **new_kwargs)

        def check_time(self):
            # Only needed without eventlet
            pass

    from eventlet import sleep

    # Helper functions to replace eventlet spawn with a threading equivalent
    class EventletResult(object):
        """Wrapper to support timeout arg when using eventlet """
        def __init__(self, gt):
            self._gt = gt

        @property
        def dead(self):
            return self._gt.dead

        def wait(self, timeout=None):
            if timeout is not None:
                with Timeout(timeout):
                    return self._gt.wait()
            return self._gt.wait()

        def kill(self):
            self._gt.kill()

    def spawn(func, *args, **kwargs):
        return EventletResult(eventlet.spawn(func, *args, **kwargs))

    # spawn_n is not used with a kwarg, just use the unwrapped function
    spawn_n = eventlet.spawn_n

else:
    class Timeout(BaseException):
        def __init__(self, seconds=None, socket=None, exception=None):
            # exception is unused, kept to be compatible with eventlet and
            # test/unit/obj/test_ssync.py::TestSsyncECReconstructorSyncJob
            self.seconds = seconds
            self.socket = socket
            self.old_timeout = None
            self.deadline = None

        def __enter__(self):
            if self.seconds is not None:
                if self.seconds > 0:
                    self.deadline = time.monotonic() + self.seconds
            if self.seconds is not None and self.socket is not None:
                self.old_timeout = self.socket.gettimeout()
                self.socket.settimeout(self.seconds)
            return self

        def check_time(self):
            if self.deadline is not None and time.monotonic() > self.deadline:
                raise self

        def restore_timeout(self):
            if self.old_timeout is not None and self.socket is not None:
                try:
                    self.socket.settimeout(self.old_timeout)
                except OSError:
                    pass
                self.old_timeout = None

        def __exit__(self, exc_type, exc_value, exc_traceback):
            self.restore_timeout()
            if exc_type is socket_timeout:
                raise self
            return False

        def __str__(self):
            if self.seconds is not None:
                if self.seconds == 1:
                    suffix = ''
                else:
                    suffix = 's'
                return '%s second%s' % (self.seconds, suffix)
            return ''

        # Only used in tests, but just in case restore timeouts
        def cancel(self):
            self.restore_timeout()

    def sleep(seconds=0):
        if seconds:
            time.sleep(seconds)

    # Helper functions to replace eventlet spawn with a threading equivalent
    class ThreadResult(object):
        def __init__(self, func, args, kwargs):
            self.result = None
            self.exc = None
            self.thread = threading.Thread(
                target=self.run, args=(func, args, kwargs))
            self.thread.daemon = True
            self.thread.start()

        def run(self, func, args, kwargs):
            try:
                self.result = func(*args, **kwargs)
            except BaseException as e:
                self.exc = e

        def wait(self, timeout=None):
            self.thread.join(timeout=timeout)
            if self.thread.is_alive():
                raise Timeout(timeout)
            if self.exc:
                raise self.exc
            return self.result

        @property
        def dead(self):
            return not self.thread.is_alive()

        def kill(self):
            pass

    def spawn(func, *args, **kwargs):
        return ThreadResult(func, args, kwargs)

    # spawn_n in eventlet is the same as spawn, but without return value or
    # exceptions. Just using the same spawn without eventlet here
    spawn_n = spawn

    # Class to replaceme eventlet.pools.Pool

    class Pool(object):
        """
        Thread-safe connection pool replacement for eventlet.pools.Pool.

        This code is very similar to eventlet/eventlet/pools.py, but uses
        threading.Condition to maintain thread-safety.
        """
        def __init__(self, min_size=0, max_size=4, create=None):
            self.min_size = min_size
            self.max_size = max_size
            self.current_size = 0
            self.free_items = collections.deque()
            self.available = threading.Condition()

            if create is not None:
                self.create = create

            for x in range(min_size):
                self.current_size += 1
                self.free_items.append(self.create())

        def get(self):
            with self.available:  # acquire the lock
                if self.free_items:
                    return self.free_items.popleft()

                if self.current_size < self.max_size:
                    self.current_size += 1
                    try:
                        created = self.create()
                    except BaseException:
                        self.current_size -= 1
                        raise
                    return created

                while not self.free_items:
                    # Wait until notified by put
                    self.available.wait()

                self.current_size -= 1
                return self.free_items.popleft()

        def put(self, item):
            with self.available:  # acquires the lock
                if self.current_size > self.max_size:
                    # This should never happen
                    raise RuntimeError

                self.free_items.append(item)

                # Notify self.available.wait() in get() to re-acquire lock
                self.available.notify()

        def create(self):
            raise NotImplementedError()

        # dispersion_populate and dispersion_report require this
        @contextmanager
        def item(self):
            item = self.get()
            try:
                yield item
            finally:
                self.put(item)

    # Replacement for eventlet.tpool
    class Executor:
        """Drop-in replacement for eventlet.tpool running in the current
        thread.

        All calls to execute will run in the current thread and not in a
        separate thread pool. Eventlet uses a threadpool to be able to yield
        to other coros and not block the current one, but without eventlet
        this is not needed - it is already running in a thread.
        """
        # No-op to be compatible with eventlet call
        def set_num_threads(self, *args, **kwargs):
            pass

        @staticmethod
        def execute(func, *args, **kwargs):
            return func(*args, **kwargs)

    # No need for a threadpool when already running in threads.
    tpool = Executor()

    class SwiftPool(ThreadPoolExecutor):
        """SwiftPool-compatible pool backed by ThreadPoolExecutor.

        Provides the same API as eventlet.SwiftPool so callers don't need
        per-method ``if USE_EVENTLET`` branches.
        """

        def __init__(self, size=1024):
            super(SwiftPool, self).__init__(max_workers=size)
            self.size = size
            self.futures = []

        def spawn(self, func, *args, **kwargs):
            future = self.submit(func, *args, **kwargs)
            self.futures.append(future)
            return future

        def spawn_n(self, func, *args, **kwargs):
            future = self.submit(func, *args, **kwargs)
            self.futures.append(future)
            return future

        def waitall(self):
            wait(self.futures)
            self.futures = []

        def running(self):
            return len([f for f in self.futures if f.running()])

        def free(self):
            return self.size - self.running()

        def starmap(self, func, iterable):
            return self.map(lambda args: func(*args), iterable)

        def imap(self, func, *iterables):
            return self.map(func, *iterables)


# flake8 raises a F401 without this
__all__ = [
    'USE_EVENTLET',
    'FALSE_VALUES',
    'config_false_value',
    'debug',
    'greenio',
    'greenthread',
    'hubs',
    'patcher',
    'queue',
    'wsgi',
    'GreenPile',
    'SwiftPool',
    'Timeout',
    'greenio',
    'greenpool',
    'hubs',
    'patcher',
    'queue',
    'tpool',
    'wsgi',
    'debug',
    'listen',
    'sleep',
    'spawn',
    'timeout',
    'websocket',
    'greenthread',
    'Event',
    'socket',
    'ssl',
    'subprocess',
    'green_os',
    'green_threading',
    'green_http_client',
    'CONTINUE',
    'HTTPConnection',
    'HTTPResponse',
    'HTTPSConnection',
    'ImproperConnectionState',
    '_UNKNOWN',
    'urllib_request',
    'getcurrent',
    'greenthread_spawn',
    'trampoline',
    'Pool',
    'Empty',
    'LightQueue',
    'Queue',
    'Semaphore',
    'GreenletExit',
    'eprofile',
    'hub_exceptions',
    'hub_prevent_multiple_readers',
    'monkey_patch',
    'shutdown_safe',
    'spawn_n',
    'ChunkReadError',
]
