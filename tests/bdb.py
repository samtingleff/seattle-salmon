#!/usr/bin/env python
import os
import sys
import unittest

sys.path.append("..")
sys.path.append(os.path.join(sys.path[0], '..'))

from config import TestConfig
from storage.bdb import BTree

class BTreeTestCase(unittest.TestCase):

    def setUp(self):
        self.storage = BTree(TestConfig())
        self.values = {'a':'1', 'b':'2', 'c':'3', 'd':'4', 'e':'5'}

    def tearDown(self):
        self.storage.close()

    def testSetGetDelete(self):
        for key in self.values.keys(): self.storage.set(key, self.values[key])
        for key in self.values.keys(): self.assertEquals(self.values[key], self.storage.get(key))
        for key in self.values.keys(): self.storage.delete(key)
        for key in self.values.keys(): self.assertFalse(self.storage.get(key))

    def testIterator(self):
        for key in self.values.keys(): self.storage.set(key, self.values[key])
        iter = self.storage.iterator()
        found = {}
        while iter.has_next():
            key, value = iter.next_item()
            found[key] = self.storage.get(key)
        self.assertEquals(len(self.values), len(found))
        self.assertEquals(self.values, found)
        for key in self.values.keys(): self.storage.delete(key)
        iter.close()

if __name__ == '__main__':
    unittest.main()

