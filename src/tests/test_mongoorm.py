import unittest
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

class TestCustomType(MongoFieldBase):
    def __init__(self, value, field_name, *argv, **kwargs):
        super().__init__(value, field_name, MongoType.UNDEFINED)

        self._test_param_str = kwargs['test_param_str']
        self._test_param_int = kwargs['test_param_int']

    def to_mongo(self):
        return self.value + self._test_param_str + str(self._test_param_int)

class TestPersonModel(MongoCollectionBase):
    def __init__(self):
        super().__init__()
        self._id = ObjectId('5f2234f0a36b8cfba16e3f67')
        self._name = 'John'
        self._age = 28
        self._insurance_number = 1234567890213434
        self._height = 1.82
        self._address = TestAddress()
        self._attributes = ['male', 'married', 'unemployed']
        self._hourly_rate = decimal.Decimal('99.99')
        self._date_created = dt.datetime(2020, 7, 26, 23, 49)
        self._last_viewed = dt.datetime(2020, 7, 26, 00, 41)
        self._photo = bytearray('R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7', 'utf-8')
        self._account_enabled = True
        self._some_regex = 'ab*'
        self._custom_field = 'Hello'

    @object_id_field(primary_key=True)
    def id(self):
        return self._id

    @id.setter
    def id(self, value):
        self._id = value

    @string_field()
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        self._name = value

    @int_field()
    def age(self):
        return self._age

    @age.setter
    def age(self, value):
        self._age = value

    @long_field()
    def insurance_number(self):
        return self._insurance_number

    @insurance_number.setter
    def insurance_number(self, value):
        self._insurance_number = value

    @float_field()
    def height(self):
        return self._height

    @height.setter
    def height(self, value):
        self._height = value

    @embedded_document_field()
    def address(self):
        return self._address

    @address.setter
    def address(self, value):
        self.address = value

    @list_field()
    def attributes(self):
        return self._attributes

    @decimal_field()
    def hourly_rate(self):
        return self._hourly_rate

    @hourly_rate.setter
    def hourly_rate(self, value):
        self.hourly_rate = value

    @datetime_field()
    def date_created(self):
        return self._date_created

    @date_created.setter
    def date_created(self, value):
        self._date_created = value

    @timestamp_field()
    def last_modified(self):
        pass

    @timestamp_field()
    def last_viewed(self):
        return self._last_viewed

    @binary_field(BinaryField.Subtypes.DEFAULT_BINARY)
    def photo(self):
        return self._photo

    @boolean_field()
    def account_enabled(self):
        return self._account_enabled

    @account_enabled.setter
    def account_enabled(self, value):
        self._account_enabled = value

    @regex_field()
    def some_regex(self):
        return self._some_regex

    @custom_field(TestCustomType, test_param_str='World', test_param_int=2020)
    def custom_field(self):
        return self._custom_field

    @custom_field.setter
    def custom_field(self, value):
        self._custom_field = value
    
class TestAddress(MongoCollectionBase):
    def __init__(self):
        super().__init__()

        self._address = None

    @string_field()
    def address(self):
        return self._address

class TestMongoORM(unittest.TestCase):
    def __init__(self, methodName):
        super().__init__(methodName)

        self._test_model = None

    def setUp(self):
        Defaults.field_naming_convention = Defaults.NamingConventions.PASCAL_CASE
        Defaults.name_separator = '_'
        self._test_model = TestPersonModel()

    def test_text_with_space_should_return_snake_case(self):
        text = 'SNAKE Case'
        self.assertEqual(Common.snake_case(text), 'snake_case')

    def test_text_without_space_should_return_snake_case(self):
        text = 'TestCase'
        self.assertEqual(Common.snake_case(text), 'test_case')

    def test_text_with_double_spaces_should_return_snake_case(self):
        text = 'SNAKE    Case with  SPACES'
        self.assertEqual(Common.snake_case(text), 'snake_case_with_spaces')

    def test_text_with_separators_should_return_camel_case(self):
        text = 'TEXT_wiTH -separators'
        self.assertEqual(Common.camel_case(text), 'textWithSeparators')

    def test_text_with_one_word_should_return_camel_case(self):
        text = 'TeXT'
        self.assertEqual(Common.camel_case(text), 'text')

    def test_text_with_separators_should_return_pascal_case(self):
        text = 'tEXT_wiTH -separators'
        self.assertEqual(Common.pascal_case(text), 'TextWithSeparators')

    def test_text_with_one_word_should_return_pascal_case(self):
        text = 'teXT'
        self.assertEqual(Common.pascal_case(text), 'Text')

    def test_convert_field_name_returns_selected_defaults(self):
        Defaults.field_naming_convention = Defaults.NamingConventions.SNAKE_CASE
        Defaults.name_separator = '-'
        text = 'ThisIsAFieldName'
        self.assertEqual(Common.convert_field_name(text), 'this-is-a-field-name')

    def test_collection_name_matches_class_name(self):
        self.assertEqual(self._test_model.get_collection_name(), 'test_person_model')

    def test_mongo_field_setter_matches_getter(self):
        self._test_model.name = 'Johnny'
        self.assertEqual(self._test_model.name.value, 'Johnny')

    def test_mongo_object_id_field_matches_property_type(self):
        self.assertEqual(self._test_model.id.mongo_type, MongoType.OBJECT_ID)

    def test_mongo_object_id_field_to_mongo_matches_mongo_type(self):
        self.assertTrue(isinstance(self._test_model.id.to_mongo(), ObjectId))    

    def test_mongo_string_field_matches_property_type(self):
        self.assertEqual(self._test_model.name.mongo_type, MongoType.STRING)

    def test_mongo_string_field_to_mongo_matches_mongo_type(self):
        self.assertTrue(isinstance(self._test_model.name.to_mongo(), str))

    def test_mongo_int_field_matches_property_type(self):
        self.assertEqual(self._test_model.age.mongo_type, MongoType.INT32)

    def test_mongo_int_field_to_mongo_matches_mongo_type(self):
        self.assertTrue(isinstance(self._test_model.age.to_mongo(), int))

    def test_mongo_long_field_matches_property_type(self):
        self.assertEqual(self._test_model.insurance_number.mongo_type, MongoType.INT64)

    def test_mongo_long_field_to_mongo_matches_mongo_type(self):
        self.assertTrue(isinstance(self._test_model.insurance_number.to_mongo(), Int64))

    def test_mongo_float_field_matches_property_type(self):
        self.assertEqual(self._test_model.height.mongo_type, MongoType.DOUBLE)

    def test_mongo_float_field_to_mongo_matches_mongo_type(self):
        self.assertTrue(isinstance(self._test_model.height.to_mongo(), float))

    def test_mongo_embedded_document_field_matches_property_type(self):
        self.assertEqual(self._test_model.address.mongo_type, MongoType.OBJECT)

    def test_mongo_embedded_document_field_to_mongo_matches_mongo_type(self):
        self.assertTrue(isinstance(self._test_model.address.to_mongo(), MongoCollectionBase))

    def test_mongo_list_field_matches_property_type(self):
        self.assertEqual(self._test_model.attributes.mongo_type, MongoType.ARRAY)

    def test_mongo_list_field_to_mongo_matches_mongo_type(self):
        self.assertTrue(isinstance(self._test_model.attributes.to_mongo(), list))

    def test_mongo_datetime_field_matches_property_type(self):
        self.assertEqual(self._test_model.date_created.mongo_type, MongoType.DATE)

    def test_mongo_datetime_field_to_mongo_matches_mongo_type(self):
        self.assertTrue(isinstance(self._test_model.date_created.to_mongo(), dt.datetime))

    def test_mongo_timestamp_field_matches_property_type(self):
        self.assertEqual(self._test_model.last_modified.mongo_type, MongoType.TIMESTAMP)
        self.assertEqual(self._test_model.last_viewed.mongo_type, MongoType.TIMESTAMP)

    def test_mongo_timestamp_field_to_mongo_matches_mongo_type(self):
        self.assertTrue(isinstance(self._test_model.last_modified.to_mongo(), Timestamp))
        self.assertTrue(isinstance(self._test_model.last_viewed.to_mongo(), Timestamp))

    def test_mongo_binary_field_matches_property_type(self):
        self.assertEqual(self._test_model.photo.mongo_type, MongoType.BINARY_DATA)

    def test_mongo_binary_field_to_mongo_matches_mongo_type(self):
        self.assertTrue(isinstance(self._test_model.photo.to_mongo(), Binary))
    
    def test_mongo_boolean_field_matches_property_type(self):
        self.assertEqual(self._test_model.account_enabled.mongo_type, MongoType.BOOLEAN)

    def test_mongo_boolean_field_to_mongo_matches_mongo_type(self):
        self.assertTrue(isinstance(self._test_model.account_enabled.to_mongo(), bool))

    def test_mongo_decimal_field_matches_property_type(self):
        self.assertEqual(self._test_model.hourly_rate.mongo_type, MongoType.DECIMAL128)

    def test_mongo_decimal_field_to_mongo_matches_mongo_type(self):
        self.assertTrue(isinstance(self._test_model.hourly_rate.to_mongo(), Decimal128))

    def test_mongo_regex_field_matches_property_type(self):
        self.assertEqual(self._test_model.some_regex.mongo_type, MongoType.REGEX)

    def test_mongo_regex_field_to_mongo_matches_mongo_type(self):
        self.assertTrue(isinstance(self._test_model.some_regex.to_mongo(), Regex))


    def test_mongo_custom_field_matches_property_type(self):
        self.assertEqual(self._test_model.custom_field.mongo_type, MongoType.UNDEFINED)

    def test_mongo_custom_field_to_mongo_matches_custom_parameterss(self):
        result = 'HelloWorld2020'
        self.assertEqual(self._test_model.custom_field.to_mongo(), result)

    def test_mongo_to_son_matches_bson(self):
        son = SON()
        son['_id'] = ObjectId('5f2234f0a36b8cfba16e3f67')
        son['name'] = 'John'
        son['age'] = 28
        son['insurance_number'] = 1234567890213434
        son['height'] = 1.82
        son['address'] = SON()
        son['address']['address'] = None
        son['attributes'] = ['male', 'married', 'unemployed']
        son['hourly_rate'] = Decimal128('99.99')
        son['date_created'] = dt.datetime(2020, 7, 26, 23, 49)
        son['last_modified'] = Timestamp(0, 0)
        son['last_viewed'] = Timestamp(1595724060, 0)
        son['photo'] = Binary(b'R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7', 0)  
        son['account_enabled'] = True
        son['some_regex'] = Regex('ab*', 0) 
        son['custom_field'] = 'HelloWorld2020'
        
        self.assertEqual(self._test_model.to_son(), son)

    def test_mongo_collection_equals_mongo_collection(self):
        other = TestPersonModel()
        self.assertEqual(self._test_model, other)
        other.age = other.age.value + 1
        self.assertNotEqual(self._test_model, other)

    def test_repository_generic_type_cannot_be_empty(self):
        self.assertRaises(TypeError, lambda: MongoRepository(None, None))

    def test_repository_generic_type_implements_collection_base(self):
        self.assertRaises(TypeError, lambda: MongoRepository[str](None, None))

    @mongomock.patch(servers=(('localhost', 27017),))
    def test_repository_generic_type_matches_concrete_type(self):
        mongo_repo = MongoRepository[TestAddress](mongomock.MongoClient('mongodb://localhost:27017/test_db'))
        self.assertEqual(TestAddress, mongo_repo._concrete_type)
    
    @mongomock.patch(servers=(('localhost', 27017),))
    def test_insert_one_should_find_one(self):
        mongo_repo = MongoRepository[TestPersonModel](mongomock.MongoClient('mongodb://localhost:27017/test_db'))
        mongo_repo.insert_one(self._test_model)
        result = mongo_repo.find_one(self._test_model._id)
        self.assertEqual(result, self._test_model)

    # @mongomock.patch(servers=(('localhost', 27017),))
    # def test_aaa(self):
    #     repository = MongoRepository(pymongo.MongoClient('mongodb://localhost:27017/test4'))
    #     repository.insert_one(self._test_model)

if __name__ == '__main__':
    unittest.main()