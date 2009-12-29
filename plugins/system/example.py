from plugins.api import IStoragePlugin, PluginManager

class ExamplePlugin(IStoragePlugin):
    def __init__(self):
        PluginManager().register("storage.get", self.get)
        PluginManager().register("storage.set", self.set)
        PluginManager().register("storage.iterator", self.iterator)

    def get(self, func, key): print "get() called with key %s" % key

    def set(self, func, key, value): print "set() called with key %s and value %s" % (key, value)

    def iterator(self, func): print "iterator() callled"

ExamplePlugin()
