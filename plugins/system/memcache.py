from plugins.api import IStoragePlugin, PluginManager

from cache import memcache

class MemcachePlugin(IStoragePlugin):
    def __init__(self):
        self._connect()
        PluginManager().register("storage.get", self.get)
        PluginManager().register("storage.set", self.set)
        PluginManager().register("storage.delete", self.delete)

    def _connect(self):
        self.mc = memcache.Client(['localhost:11211'], debug=0)

    def get(self, func, key):
        val = self.mc.get(key)
        if val: return lambda k: val

    def set(self, func, key, value):
        self.mc.set(key, value)

    def delete(self, func, key):
        self.mc.delete(key)

MemcachePlugin()

