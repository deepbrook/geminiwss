# Import Built-Ins
import logging
import time
from threading import Thread
from collections import defaultdict
from queue import Queue
# Import Third-Party

# Import Homebrew

# Init Logging Facilities
log = logging.getLogger(__name__)


from websocket import create_connection, WebSocketTimeoutException


class GeminiWss:
    def __init__(self, endpoints=None):

        self.endpoints = endpoints if endpoints else []
        self.endpoint_threads = {}
        self.running = {}
        self.endpoint_qs = defaultdict(Queue)
        self.restarter_thread = None
        self.restart_q = Queue()
        self.addr = 'wss://api.gemini.com/v1/'

    def _restart_thread(self):
        """
        Restarts subscription threads if their connection drops.
        :return:
        """
        while self.running['restart_thread']:
            try:
                endpoint = self.restart_q.get(timeout=.5)
            except TimeoutError:
                continue
            log.info("_restart_thread(): Restarting Thread for endpoint %s",
                     endpoint)
            self.unsubscribe(endpoint)
            self.subscribe(endpoint)

    def _subscription_thread(self, endpoint):
        """
        Thread Method, running the connection for each endpoint.
        :param endpoint:
        :return:
        """
        try:
            conn = create_connection(self.addr + endpoint, timeout=1)
        except WebSocketTimeoutException:
            self.restart_q.put(endpoint)
            return

        while self.running[endpoint]:
            try:
                msg = conn.recv()
            except WebSocketTimeoutException:
                self.restart_q.put(endpoint)

            self.endpoint_qs[endpoint].put(msg)

    def start(self):
        self.restarter_thread = Thread(target=self._restart_thread,
                                       name='Restarter Thread')
        self.restarter_thread.daemon = True
        self.running['restart_thread'] = True
        self.restarter_thread.start()
        for endpoint in self.endpoints:
            self.subscribe(endpoint)

    def stop(self):
        self.running['restart_thread'] = False
        self.restarter_thread.join()
        for endpoint in self.endpoints:
            self.unsubscribe(endpoint)
        self.garbage_collector()

    def restart(self):
        self.stop()
        self.start()

    def subscribe(self, endpoint):
        self.running[endpoint] = True
        t = Thread(target=self._subscription_thread,
                   args=(endpoint,), name=endpoint)
        t.daemon = True
        t.start()
        self.endpoint_threads[endpoint] = t

    def unsubscribe(self, endpoint):
        self.running[endpoint] = False
        self.endpoint_threads[endpoint].join()

        self.garbage_collector()

    def garbage_collector(self):
        for endpoint in self.endpoints:
            if self.endpoint_threads[endpoint].isalive():
                continue
            else:
                self.endpoint_threads.pop(endpoint)









