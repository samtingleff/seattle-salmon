import logging
import md5
import os
import struct
import time

from bsddb3 import db

class BTreeIterator(object):
    def __init__(self, dbenv, dbs):
        self.dbenv = dbenv
        self.dbs = dbs
        self.index = 0
        self._next_cursor()

    def __iter__(self):
        return self

    def next(self):
        if not self.next:
            raise StopIteration

        obj = self.next

        self.next = self.cursor.next()
        if not self.next:
            self.cursor.close()
            self.index += 1
            self._next_cursor()

        return obj

    def _next_cursor(self):
        self.cursor = self.dbs[self.index].cursor()
        self.next = self.cursor.first()
        if not self.next:
            self.cursor.close()
            if self.index < len(self.dbs) - 1:
                self.index += 1
                return self._next_cursor()

class BTree(object):
    default_flags, default_expires, default_cas = 0, 0, 0
    def __init__(self, daemon):
        self.datadir = daemon.get_option('bdb', 'data-dir', default='tmp/data')
        self.homedir = daemon.get_option('bdb', 'log-dir', default='tmp/logs')
        self.dbs = []
        self.dbenv = db.DBEnv()
        self.dbenv.set_cachesize(
                daemon.get_int_option('bdb', 'cache-gbytes', default=1),
                daemon.get_int_option('bdb', 'cache-bytes', default=0))
        self.dbenv.set_lk_max_locks(daemon.get_int_option('bdb', 'lk-max-locks', default=1000))
        self.dbenv.set_lk_max_lockers(daemon.get_int_option('bdb', 'lk-max-lockers', default=1000))
        self.dbenv.set_lk_max_objects(daemon.get_int_option('bdb', 'lk-max-objects', default=1000))
        self.dbenv.open(self.homedir, db.DB_INIT_LOCK | db.DB_INIT_LOG | db.DB_INIT_MPOOL | db.DB_INIT_TXN | db.DB_RECOVER | db.DB_USE_ENVIRON | db.DB_USE_ENVIRON_ROOT | db.DB_CREATE | db.DB_REGISTER | db.DB_THREAD | db.DB_READ_COMMITTED | db.DB_TXN_NOWAIT | db.DB_TXN_NOSYNC)
        txn = None
        try:
            txn = self.dbenv.txn_begin()
            for i in range(daemon.get_int_option('bdb', 'splits', default=1)):
                d = db.DB(dbEnv=self.dbenv)
                d.open("%s/data-%d.db" % (self.datadir, i), None, dbtype=db.DB_BTREE, flags=db.DB_CREATE | db.DB_READ_UNCOMMITTED | db.DB_THREAD | db.DB_TXN_NOSYNC, txn=txn)
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

    def iterator(self):
        return BTreeIterator(self.dbenv, self.dbs)

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

    def cleanup(self):
        try:
            logs = self.dbenv.log_archive()
            for log in logs:
                os.remove("%s/%s" % (self.homedir, log))
        except Exception, e:
            logging.exception(e)

    def close(self):
        try:
            for i, d in enumerate(self.dbs):
                d.close()
            self.dbenv.close()
            logging.info("closed all databases")
        except Exception, e:
            logging.exception(e)
            raise e

    def _get_db(self, key):
        if len(self.dbs) == 1: return self.dbs[0]
        return self.dbs[struct.unpack('Q', md5.new(key).digest()[:8])[0] % len(self.dbs)]
