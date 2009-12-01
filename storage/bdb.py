import hashlib
import logging
import time

from bsddb3 import db

class BTree(object):
    default_flags, default_expires, default_cas = 0, 0, 0
    def __init__(self, datadir, homedir, splits=1, cache_gbytes=1, cache_bytes=0, lk_max_locks=1000, lk_max_lockers=1000, lk_max_objects=1000):
        self.datadir = datadir
        self.homedir = homedir
        self.dbs = []
        self.dbenv = db.DBEnv()
        self.dbenv.set_cachesize(cache_gbytes, cache_bytes)
        self.dbenv.set_lk_max_locks(lk_max_locks)
        self.dbenv.set_lk_max_lockers(lk_max_lockers)
        self.dbenv.set_lk_max_objects(lk_max_objects)
        self.dbenv.open(homedir, db.DB_INIT_LOCK | db.DB_INIT_LOG | db.DB_INIT_MPOOL | db.DB_INIT_TXN | db.DB_RECOVER | db.DB_USE_ENVIRON | db.DB_USE_ENVIRON_ROOT | db.DB_CREATE | db.DB_REGISTER | db.DB_THREAD | db.DB_READ_COMMITTED | db.DB_TXN_NOWAIT | db.DB_TXN_NOSYNC)
        txn = None
        try:
            txn = self.dbenv.txn_begin()
            for i in range(splits):
                d = db.DB(dbEnv=self.dbenv)
                d.open("%s/data-%d.db" % (datadir, i), None, dbtype=db.DB_BTREE, flags=db.DB_CREATE | db.DB_READ_UNCOMMITTED | db.DB_THREAD | db.DB_TXN_NOSYNC, txn=txn)
                self.dbs += [d]
            txn.commit()
        except Exception, e:
            logging.exception(e)
            txn.abort()

    def get(self, key):
        return self._get_db(key).get(key)

    def set(self, key, val):
        txn = None
        try:
            txn = self.dbenv.txn_begin()
            self.set_txn(key, val, txn)
            txn.commit()
        except Exception, e:
            logging.exception(e)
            txn.abort()
            raise e

    def set_txn(self, key, val, txn):
        self._get_db(key).put(key, val, txn=txn)

    def set_bulk(self, dict):
        txn = None
        try:
            txn = self.dbenv.txn_begin()
            for key in dict.keys(): self._get_db(key).put(key, dict[key], txn=txn)
            txn.commit()
        except Exception, e:
            logging.exception(e)
            txn.abort()
            raise e

    def delete(self, key):
        txn = None
        try:
            txn = self.dbenv.txn_begin()
            self.delete_txn(key, None, txn)
            txn.commit()
        except db.DBNotFoundError, e:
            txn.abort()
            raise KeyError()
        except Exception, e:
            logging.exception(e)
            txn.abort()
            raise e

    def delete_txn(self, key, val, txn):
        try: self._get_db(key).delete(key, txn=txn)
        except db.DBNotFoundError: pass

    def delete_bulk(self, keys):
        txn = None
        try:
            txn = self.dbenv.txn_begin()
            for key in keys: self.delete_txn(key, txn)
            txn.commit()
        except db.DBNotFoundError, e:
            txn.abort()
            raise KeyError()
        except Exception, e:
            logging.exception(e)
            txn.abort()
            raise e

    def create_txn(self): return self.dbenv.txn_begin()

    def commit_txn(self, txn): txn.commit()

    def abort_txn(self, txn): txn.abort()

    def flush(self):
        try:
            start = time.time()
            self.dbenv.log_flush()
            duration = int(time.time() - start)
            logging.info("flush() completed in %s seconds" % duration)
        except Exception, e:
            logging.exception(e)

    def sync(self):
        try:
            for i, d in enumerate(self.dbs):
                sync_start = time.time()
                d.sync()
                logging.info("sync() on db #%d completed in %s seconds" % (i, int(time.time() - sync_start)))
            txn_checkpoint_start = time.time()
            self.dbenv.txn_checkpoint()
            logging.info("txn_checkpoint() completed in %s seconds" % int(time.time() - txn_checkpoint_start))
        except Exception, e:
            logging.exception(e)

    def _get_db(self, key):
        if len(self.dbs) == 1: return self.dbs[0]
        else:
            return self.dbs[int(hashlib.md5(key).hexdigest(), 16) % len(self.dbs)]
