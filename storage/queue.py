import logging
import time
from threading import RLock

class QueueFullException(Exception):
    pass

class BoundedSynchronizedDictionary(dict):
    def __init__(self, limit=100):
        self.lock = RLock()
        self.limit = limit

    def __getitem__(self, key):
        return dict.__getitem__(self, key)

    def __setitem__(self, key, value):
        if len(dict(self)) >= self.limit:
            logging.error("memory queue size reached max of %s items. Rejecting set()" % self.limit)
            raise QueueFullException
        self.lock.acquire()
        try: dict.__setitem__(self, key, value)
        finally: self.lock.release()

    def __delitem__(self, key):
        self.lock.acquire()
        try: dict.__delitem__(self, key)
        finally: self.lock.release()

    def copy_and_clear(self):
        self.lock.acquire()
        try:
            copy = dict.copy(self)
            dict.clear(self)
            return copy
        finally: self.lock.release()

class QueuedStorage(object):
    def __init__(self, backend, limit=1000):
        self.backend = backend
        self.queue = BoundedSynchronizedDictionary(limit=limit)
        self.to_flush = {}

    def get(self, key):
        try:
            return self.queue[key][1]
        except KeyError:
            try: return self.to_flush[key][1]
            except KeyError: return self.backend.get(key)

    def set(self, key, value):
        self.queue[key] = (self.backend.set_txn, value)

    def set_bulk(self, dict):
        for key in dict.keys(): self.set(key, dict[key])

    def delete(self, key):
        try: del self.queue[key]
        except KeyError: pass
        self.queue[key] = (self.backend.delete_txn, None)

    def delete_bulk(self, keys):
        self.delete(key)

    def flush(self):
        start = time.time()
        self.to_flush = self.queue.copy_and_clear()
        txn = self.backend.create_txn()
        try:
            for key in self.to_flush.keys():
                op, val = self.to_flush[key]
                op(key, val, txn)
            self.backend.commit_txn(txn)
        except Exception, e:
            logging.exception(e)
            self.backend.abort_txn(txn)
        self.backend.flush()
        duration = int(time.time() - start)
        logging.info("flushed %d items in %s seconds" % (len(self.to_flush), duration))
        self.to_flush.clear()

    def sync(self): self.backend.sync()

