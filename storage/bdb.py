import struct
import logging
import time
from memcache import binary, constants

try:
    from bsddb3 import db
except ImportError:
    from bsddb import db

class BTree(object):
    default_flags, default_expires, default_cas = 0, 0, 0
    def __init__(self, datadir, logdir, cache_gbytes=1, cache_bytes=0):
        self.dbenv = db.DBEnv()
        self.dbenv.set_cachesize(cache_gbytes, cache_bytes)
        self.dbenv.open(logdir, db.DB_INIT_LOCK | db.DB_INIT_LOG | db.DB_INIT_MPOOL | db.DB_INIT_TXN | db.DB_RECOVER | db.DB_USE_ENVIRON | db.DB_USE_ENVIRON_ROOT | db.DB_CREATE | db.DB_REGISTER | db.DB_THREAD | db.DB_READ_COMMITTED | db.DB_TXN_NOWAIT | db.DB_TXN_NOSYNC)
        txn = None
        try:
            txn = self.dbenv.txn_begin()
            self.db = db.DB(dbEnv=self.dbenv)
            self.db.open("%s/data-1.db" % datadir, None, dbtype=db.DB_BTREE, flags=db.DB_CREATE | db.DB_READ_UNCOMMITTED | db.DB_THREAD | db.DB_TXN_NOSYNC, txn=txn)
            txn.commit()
        except Exception, e:
            logging.exception(e)
            txn.abort()

    def doGet(self, req, data):
        try:
            val = self.db.get(req.key)
            if val:
                return binary.GetResponse(req, self.default_flags, self.default_cas, data=val)
            else: raise binary.MemcachedNotFound()
        except KeyError:
            raise binary.MemcachedNotFound()

    def doSet(self, req, data):
        flags, exp = struct.unpack(constants.SET_PKT_FMT, req.extra)
        txn = None
        try:
            txn = self.dbenv.txn_begin()
            self.db.put(req.key, data, txn=txn)
            txn.commit()
        except Exception:
            logging.exception(e)
            txn.abort()
            raise binary.MemcachedError

    def doDelete(self, req, data):
        txn = None
        try:
            txn = self.dbenv.txn_begin()
            self.db.delete(req.key, txn=txn)
            txn.commit()
            self.db.sync()
        except db.DBNotFoundError:
            txn.abort()
            raise binary.MemcachedNotFound()
        except Exception, e:
            logging.exception(e)
            txn.abort()
            raise binary.MemcachedError

    def doQuit(self, *a):
        raise binary.MemcachedDisconnect()

    def sync(self):
        try:
            start = time.time()
            self.db.sync()
            duration = int(time.time() - start)
            logging.info("sync() completed in %s" % duration)
        except Exception, e:
            logging.exception(e)

