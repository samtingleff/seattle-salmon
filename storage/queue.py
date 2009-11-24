import logging
import Queue
import time
from threading import RLock

class QueueFullException(Exception):
    pass

class BoundedList(object):
    def __init__(self, limit=100):
        self.queue = Queue.Queue(limit)
        self.limit = limit

    def push(self, value):
        try:
            self.queue.put(value)
        except Queue.Full: raise QueueFullException

    def pop(self):
        try: return self.queue.get(False)
        except Queue.Empty: return None

class QueuedStorage(object):
    def __init__(self, backend, limit=1000):
        self.backend = backend
        self.queue = BoundedList(limit=limit)
        self.limit = limit

    def get(self, key):
        return self.backend.get(key)

    def set(self, key, value):
        self.queue.push( (self.backend.set_txn, key, value) )

    def set_bulk(self, dict):
        for key in dict.keys(): self.set(key, dict[key])

    def delete(self, key):
        self.queue.push( (self.backend.delete_txn, key, None) )

    def delete_bulk(self, keys):
        for key in keys: self.delete(key)

    def flush(self):
        start = time.time()
        txn = self.backend.create_txn()
        flushed = 0
        try:
            tuple = self.queue.pop()
            while (tuple is not None) and flushed < self.limit:
                op, key, val = tuple
                op(key, val, txn)
                flushed += 1
                tuple = self.queue.pop()
            self.backend.commit_txn(txn)
        except Exception, e:
            logging.exception(e)
            self.backend.abort_txn(txn)
        self.backend.flush()
        duration = int(time.time() - start)
        logging.info("flushed %d items from queue in %s seconds" % (flushed, duration))

    def sync(self): self.backend.sync()

