import logging

from plugins.api import IStoragePlugin, PluginManager

class AccessLogPlugin(IStoragePlugin):
    def __init__(self):
        PluginManager().register("storage.get", self.get)
        PluginManager().register("storage.set", self.set)
        PluginManager().register("storage.delete", self.delete)

    def get(self, func, key):
        logging.info("get: %s" % key)

    def set(self, func, key, value):
        logging.info("set: %s" % key)

    def delete(self, func, key):
        logging.info("delete: %s" % key)

AccessLogPlugin()
