#!/usr/bin/env python
import os
import unittest
import sys

sys.path.append("..")
sys.path.append(os.path.join(sys.path[0], '..'))

from config import TestConfig
from plugins.api import IStoragePlugin, PluginManager

class TestPlugin(IStoragePlugin):
    def __init__(self):
        mgr = PluginManager()
        mgr.load(TestConfig().get_option('daemon', 'plugin-file', None))
        mgr.register("test_event", self.plugin_function)

    def plugin_function(self, fn, *args, **kargs):
        self.plugin_function_called = 1
        return self.plugin_substitute

    def original_function(self, k, v, z=1): self.original_called, self.substitute_called = 1, 0

    def plugin_substitute(self, k, v, z=2):
        self.original_called, self.substitute_called = 0, 1
        return k * z

class PluginManagerTestCase(unittest.TestCase):

    def setUp(self): pass

    def tearDown(self): pass

    def testPluginManager(self):
        mgr = PluginManager()
        plugin = TestPlugin()
        r = mgr.execute("test_event", plugin.original_function, 10, 12, z=123)
        self.assertEquals(plugin.plugin_function_called, 1)
        self.assertEquals(plugin.substitute_called, 1)
        self.assertEquals(plugin.original_called, 0)
        self.assertEquals(r, 10*123)
        mgr.execute("unregistered_test_event", plugin.original_function, 10, 12, z=123)

if __name__ == '__main__':
    unittest.main()

