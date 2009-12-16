
class IKeyValueIterator(object):
    def configure(self, storage): pass

    def item(self, key, value): pass

    def close(self): pass
