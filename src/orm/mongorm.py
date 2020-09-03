import datetime as dt
import re
import bson
import inspect
import typing
import sys
from inspect import stack
from bson.binary import Binary
from bson.son import SON
from bson.objectid import ObjectId
from bson.decimal128 import Decimal128
from bson.int64 import Int64
from bson.regex import Regex
from bson.timestamp import Timestamp
from abc import ABC, abstractmethod
from enum import Enum, IntEnum
from functools import wraps
from typing import TypeVar, Generic, Callable
from pymongo.mongo_client import MongoClient
from pymongo.cursor import Cursor


class Defaults:
    class NamingConventions(Enum):
        SNAKE_CASE = 1
        CAMEL_CASE = 2
        PASCAL_CASE = 3
        UNCHANGED = 4
    
    strip_separators=['_', '-']
    name_separator='_'
    collection_naming_convention=NamingConventions.SNAKE_CASE
    field_naming_convention=NamingConventions.CAMEL_CASE

class Common:
    @staticmethod
    def snake_case(text: str):
        text = re.sub('([A-Z][a-z]+)', r' \1', re.sub('([A-Z]+)', r' \1', text))
        return Defaults.name_separator.join([word.lower() for word in ' '.join(text.lower().split()).split(' ')])

    @staticmethod
    def camel_case(text: str):
        for sep in Defaults.strip_separators[1:]:
            text = text.replace(sep, Defaults.strip_separators[0])
        words = [i.strip().lower() for i in text.split(Defaults.strip_separators[0])]
        return '{}{}'.format(words[0], ''.join([word[0].upper() + word[1:] for word in words[1:]]))

    @staticmethod
    def pascal_case(text: str):
        for sep in Defaults.strip_separators[1:]:
            text = text.replace(sep, Defaults.strip_separators[0])
        words = [i.strip().lower() for i in text.split(Defaults.strip_separators[0])]
        return ''.join([word[0].upper() + word[1:] for word in words])

    @staticmethod
    def convert_collection_name(collection_name):
        if Defaults.collection_naming_convention == Defaults.NamingConventions.CAMEL_CASE:
            return Common.camel_case(collection_name)
        elif Defaults.collection_naming_convention == Defaults.NamingConventions.SNAKE_CASE:
            return Common.snake_case(collection_name)
        elif Defaults.collection_naming_convention == Defaults.NamingConventions.PASCAL_CASE:
            return Common.pascal_case(collection_name)
        else:
            return collection_name

    @staticmethod
    def convert_field_name(field_name):
        if Defaults.field_naming_convention == Defaults.NamingConventions.CAMEL_CASE:
            return Common.camel_case(field_name)
        elif Defaults.field_naming_convention == Defaults.NamingConventions.SNAKE_CASE:
            return Common.snake_case(field_name)
        elif Defaults.field_naming_convention == Defaults.NamingConventions.PASCAL_CASE:
            return Common.pascal_case(field_name)
        else:
            return field_name

class MongoType(Enum):
    DOUBLE = 1,
    STRING = 2,
    OBJECT = 3,
    ARRAY = 4,
    BINARY_DATA = 5,
    UNDEFINED = 6,
    OBJECT_ID = 7,
    BOOLEAN = 8,
    DATE = 9,
    NULL = 10,
    REGEX = 11,
    INT32 = 16,
    TIMESTAMP = 17,
    INT64 = 18,
    DECIMAL128 = 19


class MongoFieldBase(ABC):
        def __init__(self, value, field_name, skip_none: bool, mongo_type: MongoType):
            super().__init__()
            self._field_counter=0
            self._value = value
            self._field_name = field_name
            self._skip_none = skip_none
            self._mongo_type = mongo_type

        def __eq__(self, other):
            return self.value == other.value and self.field_name == other.field_name and self.mongo_type == other.mongo_type

        @property
        def value(self):
            return self._value

        @property
        def field_name(self):
            return self._field_name

        @property
        def skip_none(self):
            return self._skip_none

        @property
        def mongo_type(self) -> MongoType:
            return self._mongo_type

        @abstractmethod
        def to_mongo(self):
            pass

class BinaryField(MongoFieldBase):
    class Subtypes(IntEnum):
        DEFAULT_BINARY=0
        FUNCTION=1
        OLD_BINARY=2
        OLD_UUID=3
        UUID=4
        MD5=5
        USER_DEFINED=128

    def __init__(self, value, field_name, skip_none, subtype: Subtypes = Subtypes.DEFAULT_BINARY):
        super().__init__(value, field_name, skip_none, MongoType.BINARY_DATA)

        self._subtype=subtype

    def to_mongo(self):
        if (isinstance(self.value, Binary)):
            return self.value
        
        return Binary(self.value, int(self._subtype))

class BooleanField(MongoFieldBase):
    def __init__(self, value, field_name, skip_none):
        super().__init__(value, field_name, skip_none, MongoType.BOOLEAN)

    def to_mongo(self):
        if (isinstance(self.value, bool)):
            return self.value

        return bool(self.value) if self.value is not None else None

class DecimalField(MongoFieldBase):
    def __init__(self, value, field_name, skip_none):
        super().__init__(value, field_name, skip_none, MongoType.DECIMAL128)

    def to_mongo(self):
        if (isinstance(self.value, Decimal128)):
            return self.value

        return Decimal128(self.value)

class DateTimeField(MongoFieldBase):
    def __init__(self, value, field_name, skip_none):
        super().__init__(value, field_name, skip_none, MongoType.DATE)

    def to_mongo(self):
        if isinstance(self.value, dt.datetime):
            return self.value
        elif isinstance(self.value, dt.date):
            return dt.datetime(self.value.year, self.value.month, self.value.day)
        else:
            raise TypeError('value must be of type date or datetime')

        'TODO: Support parsing the date from a string format (add format string to class so users can change it)'

class EmbeddedDocumentField(MongoFieldBase):
    def __init__(self, value, field_name, skip_none):
        super().__init__(value, field_name, skip_none, MongoType.OBJECT)

    def to_mongo(self):
        'TODO: support a dict object and generate modelbase from it'
        if not isinstance(self.value, MongoCollectionBase):
            raise TypeError('value must be of type MongoCollectionBase')

        return self.value

class FloatField(MongoFieldBase):
    def __init__(self, value, field_name, skip_none):
        super().__init__(value, field_name, skip_none, MongoType.DOUBLE)

    def to_mongo(self):
        if isinstance(self.value, float):
            return self.value
        return float(self.value) if self.value is not None else None

class IntField(MongoFieldBase):
    def __init__(self, value, field_name, skip_none):
        super().__init__(value, field_name, skip_none, MongoType.INT32)

    def to_mongo(self):
        if isinstance(self.value, int):
            return self.value
        return int(self.value) if self.value is not None else None

class ObjectIdField(MongoFieldBase):
    def __init__(self, value, field_name, skip_none, primary_key=False):
        super().__init__(value, field_name, skip_none, MongoType.OBJECT_ID)

        self._primary_key = primary_key

        if self._primary_key:
            self._field_name = '_id'

    @property
    def primary_key(self):
        return self._primary_key

    def to_mongo(self):
        if isinstance(self.value, ObjectId):
            return self.value
        return ObjectId(self._value)

class RegExField(MongoFieldBase):
    def __init__(self, value, field_name, skip_none, flags=0):
        super().__init__(value, field_name, skip_none, MongoType.REGEX)
        self._flags = 0

    def to_mongo(self):
        if isinstance(self.value, Regex):
            return self.value

        return Regex(self._value, self._flags)

class StringField(MongoFieldBase):
    def __init__(self, value, field_name, skip_none):
        super().__init__(value, field_name, skip_none, MongoType.STRING)

    def to_mongo(self):
        if isinstance(self.value, str):
            return self.value
        return str(self.value) if self.value is not None else None


class LongField(MongoFieldBase):
    def __init__(self, value, field_name, skip_none):
        super().__init__(value, field_name, skip_none, MongoType.INT64)

    def to_mongo(self):
        if isinstance(self.value, Int64):
            return self.value
        return Int64(self.value) if self.value is not None else None

class ListField(MongoFieldBase):
    def __init__(self, value, field_name, skip_none):
        super().__init__(value, field_name, skip_none, MongoType.ARRAY)

    def to_mongo(self):
        if isinstance(self.value, list):
            return self.value
        return list(self.value) if self.value is not None else None

class TimestampField(MongoFieldBase):
    def __init__(self, value, field_name, skip_none):
        super().__init__(value, field_name, skip_none, MongoType.TIMESTAMP)

    def to_mongo(self):
        if isinstance(self.value, Timestamp):
            return self.value
        elif isinstance(self.value, dt.datetime):
            return Timestamp(self.value, 0)
        else:
            return Timestamp(0, 0)

class FieldWrapper(object):
    __global_field_counter=0

    def __init__(self, mongo_field_base_cls, field_name: None, skip_none=False, *args, **kwargs):
        FieldWrapper.__global_field_counter = FieldWrapper.__global_field_counter + 1
        self._mongo_field_base_cls = mongo_field_base_cls
        self._field_name = field_name
        self._skip_none = skip_none
        self._field_counter = FieldWrapper.__global_field_counter
        self._args = args
        self._kwargs = kwargs

    def __call__(self, func):
        @property
        @wraps(func)
        def wrapper(*fargs, **fkwargs):
            f_name = self._field_name
            if f_name is None:
                f_name = func.__name__
            mongo_field = self._mongo_field_base_cls(func(*fargs, **fkwargs), f_name, self._skip_none, *self._args, **self._kwargs)
            mongo_field._field_counter = self._field_counter
            return mongo_field
        return wrapper
        

def binary_field(subtype: BinaryField.Subtypes = BinaryField.Subtypes.DEFAULT_BINARY, field_name=None, skip_none=False):
    return FieldWrapper(BinaryField, field_name, skip_none, subtype)

def boolean_field(field_name=None, skip_none=False):
    return FieldWrapper(BooleanField, field_name, skip_none)

def custom_field(mongo_field_imp, field_name=None, skip_none=False, *argv, **kwargs):
    return FieldWrapper(mongo_field_imp, field_name, skip_none, *argv, **kwargs)

def decimal_field(field_name=None, skip_none=False):
    return FieldWrapper(DecimalField, field_name, skip_none)

def embedded_document_field(field_name=None, skip_none=False):
    return FieldWrapper(EmbeddedDocumentField, field_name, skip_none)

def datetime_field(field_name=None, skip_none=False):
    return FieldWrapper(DateTimeField, field_name, skip_none)

def float_field(field_name=None, skip_none=False):
    return FieldWrapper(FloatField, field_name, skip_none)

def int_field(field_name=None, skip_none=False):
    return FieldWrapper(IntField, field_name, skip_none)

def list_field(field_name=None, skip_none=False):
    return FieldWrapper(ListField, field_name, skip_none)

def long_field(field_name=None, skip_none=False):
    return FieldWrapper(LongField, field_name, skip_none)

def object_id_field(primary_key=False, field_name=None, skip_none=False):
    return FieldWrapper(ObjectIdField, field_name, skip_none, primary_key)

def regex_field(flags=0, field_name=None, skip_none=False):
    return FieldWrapper(RegExField, field_name, skip_none, flags)

def string_field(field_name=None, skip_none=False):
    return FieldWrapper(StringField, field_name, skip_none)

def timestamp_field(field_name=None, skip_none=False):
    return FieldWrapper(TimestampField, field_name, skip_none)


class MongoCollectionBase(ABC):
    def __init__(self, skip_none=False):
        super().__init__()
        
        self._skip_none = skip_none
        self._id = None

    def __eq__(self, other):
        for name, mongo_field in self._get_mongo_fields(self):
            if mongo_field != getattr(other, name):
                return False
        
        return True

    @classmethod  
    def get_collection_name(cls):
        if not hasattr(cls, '__collection_name') or cls.__collection_name is None:
            cls.__collection_name = Common.convert_collection_name(cls.__name__)
        return cls.__collection_name

    @property
    def id(self):
        return self._id

    @staticmethod
    def _get_mongo_fields(obj):
        mongo_fields = inspect.getmembers(obj, lambda m: isinstance(m, MongoFieldBase))
        mongo_fields.sort(key=lambda m: m[1]._field_counter)
        return mongo_fields

    def to_son(self):
        son = SON()
        for name, mongo_field in self._get_mongo_fields(self):

            if (self._skip_none or mongo_field.skip_none) and mongo_field.value is None:
                continue 
            elif isinstance(mongo_field, ObjectIdField) and mongo_field.primary_key:
                son['_id'] = mongo_field.value
            elif isinstance(mongo_field, EmbeddedDocumentField):
                son[mongo_field.field_name] = mongo_field.value.to_son()
            else:
                son[mongo_field.field_name] = mongo_field.to_mongo()
        
        return son

    @classmethod
    def from_dict(cls, document_dict):
        collection = cls()
        mongo_fields = dict()
        for name, mongo_field in cls._get_mongo_fields(collection):
            mongo_fields[mongo_field.field_name] = mongo_field

        for key, value in document_dict.items():
            mongo_field = mongo_fields[key]
            mongo_field._value = value

        return collection
    
def is_Generic(tp):
    try:
        return isinstance(tp, typing.GenericMeta)
    except AttributeError:
        try:
            return issubclass(tp, typing.Generic)
#             return isinstance(tp, typing._VariadicGenericAlias) and \
#                     tp.__origin__ is tuple
        except AttributeError:
            return False
        except TypeError:
            # Shall we accept _GenericAlias, i.e. Tuple, Union, etc?
            return isinstance(tp, typing._GenericAlias)
            #return False

def get_orig_class(obj, default_to__class__=False):
    """Robust way to access `obj.__orig_class__`. Compared to a direct access this has the
    following advantages:
    1) It works around https://github.com/python/typing/issues/658.
    2) It prevents infinite recursion when wrapping a method (`obj` is `self` or `cls`) and either
    - the object's class defines `__getattribute__`
    or
    - the object has no `__orig_class__` attribute and the object's class defines `__getattr__`.
    See discussion at https://github.com/Stewori/pytypes/pull/53.
    If `default_to__class__` is `True` it returns `obj.__class__` as final fallback.
    Otherwise, `AttributeError` is raised  in failure case (default behavior).
    """
    try:
        # See https://github.com/Stewori/pytypes/pull/53:
        # Returns  `obj.__orig_class__` protecting from infinite recursion in `__getattr[ibute]__`
        # wrapped in a `checker_tp`.
        # (See `checker_tp` in `typechecker._typeinspect_func for context)
        # Necessary if:
        # - we're wrapping a method (`obj` is `self`/`cls`) and either
        #     - the object's class defines __getattribute__
        # or
        #     - the object doesn't have an `__orig_class__` attribute
        #       and the object's class defines __getattr__.
        # In such a situation, `parent_class = obj.__orig_class__`
        # would call `__getattr[ibute]__`. But that method is wrapped in a `checker_tp` too,
        # so then we'd go into the wrapped `__getattr[ibute]__` and do
        # `parent_class = obj.__orig_class__`, which would call `__getattr[ibute]__`
        # again, and so on. So to bypass `__getattr[ibute]__` we do this:
        return object.__getattribute__(obj, '__orig_class__')
    except AttributeError:
        if sys.version_info.major >= 3:
            cls = object.__getattribute__(obj, '__class__')
        else:
            # Python 2 may return instance objects from object.__getattribute__.
            cls = obj.__class__
        if is_Generic(cls):
            # Workaround for https://github.com/python/typing/issues/658
            stck = stack()
            # Searching from index 2 is sufficient: At 0 is get_orig_class, at 1 is the caller.
            # We assume the caller is not typing._GenericAlias.__call__ which we are after.
            for line in stck[2:]:
                try:
                    res = line[0].f_locals['self']
                    if res.__origin__ is cls:
                        return res
                except (KeyError, AttributeError):
                    pass
        if default_to__class__:
            return cls # Fallback
        raise

def _checktype_class(func, T):
    def wrapper(self, *args, **kwargs):
        bound_type = T.__bound__
        if bound_type is not None:
            orig_type = get_orig_class(self, True)
            if not hasattr(orig_type, '__args__') or orig_type.__args__ is None:
                raise TypeError('A subclass of {} must be specified for {}'.format(bound_type, type(self)))
            else:
                concrete_type = orig_type.__args__[0]
                if not issubclass(concrete_type, bound_type):
                    raise TypeError('Type {} is not valid. A subclass of {} must be specified for {}'.format(concrete_type, bound_type, type(self)))
                else:
                    self._concrete_type = concrete_type
        return func(self, *args, **kwargs)
    return wrapper

def _checktype_func(func, generic_type):
    sig = inspect.signature(func)
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        concrete_type = self.__orig_class__.__args__[0]

        bound_args = sig.bind(self, *args, **kwargs)
        bound_args.apply_defaults()

        for key, value in typing.get_type_hints(func).items():
            if key != 'return' and value == generic_type:
                if not isinstance(bound_args.arguments[key], concrete_type):
                    raise TypeError('Expected argument of type {} but received argument of type {}.'.format(concrete_type, bound_args.arguments[key]))

        return func(self, *args, **kwargs)
    return wrapper

def check_mongo_collection_type(cls):
    cls.__init__ = _checktype_class(cls.__init__, TMongoCollection)

    generic_type = cls.__parameters__[0]
    for name, func in inspect.getmembers(cls, predicate=inspect.isfunction):
        # check if function has type hints
        if generic_type in typing.get_type_hints(func).values():
            setattr(cls, name, _checktype_func(func, generic_type))

    return cls

TMongoCollection = TypeVar('TMongoCollection', bound=MongoCollectionBase, covariant=False, contravariant=False)

@check_mongo_collection_type
class QueryResult(Generic[TMongoCollection]):
    def __init__(self, cursor: Cursor):
        self.__cursor = cursor
        # https://github.com/mongodb/mongo-python-driver/blob/7e2790cc446b5023410429e3fe4272a8ad532e73/pymongo/cursor.py
        # https://pymongo.readthedocs.io/en/stable/api/pymongo/cursor.html#pymongo.cursor.RawBatchCursor
   
    def __getitem__(self, index):
        return self._concrete_type.from_dict(self.__cursor.__getitem__(index))

    def __enter__(self):
        return self.__cursor.__enter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__cursor.__exit__(exc_type, exc_val, exc_tb)

    def next(self):
        return self.__cursor.next()

    __next__ = next

    @property
    def alive(self):
        return self.__cursor.alive()

    def comment(self, comment):
        return self.__cursor.comment(comment)
    
    def count(self):
        return self.__cursor.count_documents()

    def explain(self):
        return self.__cursos.explain()

    def hint(self, index):
        return self.__cursor.hint(index)

    def where(self, code):
        return self.__cursor.where(code)

@check_mongo_collection_type
class MongoRepository(Generic[TMongoCollection]):
    def __init__(self, mongo_client: MongoClient, db_name: str = None):
        self._mongo_client = mongo_client
        if db_name is not None:
            self._db = self._mongo_client.get_database(name=db_name)
        else:
            self._db = self._mongo_client.get_default_database()

    def _get_collection(self):
        return self._db.get_collection(self._concrete_type.get_collection_name())

    def find_one(self, filter, *args, **kwargs) -> TMongoCollection:
        result = self._get_collection().find_one(filter, *args, **kwargs)

        if result is None:
            return None

        return self._concrete_type.from_dict(result)

    def insert_one(self, document: TMongoCollection):
        result = self._get_collection().insert_one(document.to_son())
        document._id = ObjectId(result.inserted_id)
        return result

    def insert_many(self, documents: typing.List[MongoCollectionBase], ordered=True):
        son_documents = [document.to_son() for document in documents]
        result = self._get_collection().insert_many(son_documents, ordered)
        return result

    def replace_one(self, filter, document: TMongoCollection, upsert=False):
        filter_d = filter
        if isinstance(filter_d, MongoCollectionBase):
            filter_d = {'_id': filter.id.value}

        return self._get_collection().replace_one(filter_d, document.to_son(), upsert)

    def delete_one(self, filter, hint=None):
        filter_d = filter
        if isinstance(filter_d, MongoCollectionBase):
            filter_d = {'_id': filter.id.value}
        
        return self._get_collection().delete_one(filter_d, hint=hint)

    def delete_many(self, filter, hint=None):
        return self._get_collection().delete_many(filter, hint=hint)

    def find(self, filter):
        return QueryResult[self._concrete_type](self._get_collection().find(filter))