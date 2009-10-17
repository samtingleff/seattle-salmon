import struct
from memcache import binary, constants

try:
    from bsddb3 import db
except ImportError:
    from bsddb import db

class BTree(object):
    default_flags, default_expires, default_cas = 0, 0, 0
    def __init__(self, homedir):
        self.dbenv = db.DBEnv()
        self.dbenv.open(homedir, db.DB_CREATE | db.DB_THREAD | db.DB_INIT_LOCK | db.DB_INIT_MPOOL | db.DB_TXN_SYNC)
        self.dbm = db.DB(dbEnv=self.dbenv)
        self.dbm.open("testfile.db", None, dbtype=db.DB_BTREE, flags=db.DB_CREATE)

    def doGet(self, req, data):
        try:
            val = self.dbm.get(req.key)
            if val:
                return binary.GetResponse(req, self.default_flags, self.default_cas, data=val)
            else: raise binary.MemcachedNotFound()
        except KeyError:
            raise binary.MemcachedNotFound()

    def doSet(self, req, data):
        flags, exp = struct.unpack(constants.SET_PKT_FMT, req.extra)
        self.dbm.put(req.key, data)
        self.dbm.sync()

    def doDelete(self, req, data):
        try:
            self.dbm.delete(req.key)
            self.dbm.sync()
        except db.DBNotFoundError:
            raise binary.MemcachedNotFound()

    def doQuit(self, *a):
        raise binary.MemcachedDisconnect()
