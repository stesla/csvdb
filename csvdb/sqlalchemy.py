import datetime

from sqlalchemy import TypeDecorator
from sqlalchemy.dialects import registry
from sqlalchemy.engine import default
from sqlalchemy.engine.interfaces import ReflectedColumn
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

    def get_columns(self, connection, table_name, schema=None, **kw):
        columns = connection._dbapi_connection.get_columns(table_name)
        return [
            ReflectedColumn(
                name=c[0],
                type=sqltypes.String,
            )
            for c in columns
        ]

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        return []

    def get_indexes(self, connection, table_name, schema=None, **kw):
        return []

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        return []

    def get_table_names(self, connection, schema=None, **kw):
        return connection._dbapi_connection.get_table_names()

    def has_table(self, connection, table_name, schema=None, **kw):
        tables = self.get_table_names(connection, schema=schema, **kw)
        return any(name == table_name for name in tables)


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
