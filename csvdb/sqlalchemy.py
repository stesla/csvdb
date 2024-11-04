import datetime

from sqlalchemy import TypeDecorator
from sqlalchemy.dialects import registry
from sqlalchemy.engine import default
from sqlalchemy.sql import sqltypes


registry.register('csvdb', __name__, 'Dialect')


class Dialect(default.DefaultDialect):
    name = 'csvdb'
    driver = 'csvdb'
    
    supports_statement_cache = False

    @classmethod
    def import_dbapi(cls):
        import csvdb.dbapi
        return csvdb.dbapi


class ParsedType(TypeDecorator):
    impl = sqltypes.String


class Date(ParsedType):
    def process_result_value(self, value, dialect):
        return datetime.datetime.strptime(value, "%Y-%m-%d").date()


class Datetime(ParsedType):
    def process_result_value(self, value, dialect):
        return datetime.datetime.strptime(value, "%Y-%m-%d %H:%M:%S")

class Float(ParsedType):
    def process_result_value(self, value, dialect):
        return float(value)


class Integer(ParsedType):
    def process_result_value(self, value, dialect):
        return int(value)