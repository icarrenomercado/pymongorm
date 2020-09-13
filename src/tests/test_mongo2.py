import unittest
import copy
import datetime as dt
import decimal
import mongomock
import pymongo
from bson.binary import Binary
from bson.decimal128 import Decimal128
from bson.objectid import ObjectId
from bson.int64 import Int64
from bson.son import SON
from bson.timestamp import Timestamp
from orm.mongorm import *


class Temp(MongoCollectionBase):
    def __init__(self):
        super().__init__()
        self._test = 1
    @int_field()
    def test(self):
        return self._test
   
    @test.setter
    def test(self, value):
        self._test = value

class TestMongoORM2(unittest.TestCase):
    def __init__(self, methodName):
        super().__init__(methodName)

         
    def test_aaaaa(self):
        temp = Temp()
        temp.test = temp.test.value + 9
        self.assertEqual(temp.test.value, 10)
  

if __name__ == '__main__':
    unittest.main()