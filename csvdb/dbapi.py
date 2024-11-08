from csv import DictReader
from pathlib import Path
from typing import Iterable

from sqlglot.executor import execute


paramstyle = 'pyformat'


def connect(database):
    return Connection(database)


class Connection:
    def __init__(self, dir):
        self.__dir = dir
        self.__paths = tuple(Path(dir).glob("*.csv"))

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        pass

    def commit(self):
        pass

    def rollback(self):
        pass

    def cursor(self):
        tables = {
            path.stem: csv_table(path)
            for path in self.__paths
        }
        return Cursor(tables)

    def get_columns(self, table_name):
        path = Path(self.__dir).joinpath(table_name).with_suffix('.csv')
        with csv_table(path) as table:
            return [(name, STRING) for name in table.columns]

    def get_table_names(self):
        return [path.stem for path in self.__paths]


class Cursor:
    arraysize = 1
    __iter = None
    __result = None

    def __init__(self, tables):
        self.__tables = tables

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @property
    def description(self):
        if self.__result is None:
            return None
        return [(col, STRING) for col in self.__result.columns]

    def close(self):
        for table in self.__tables.values():
            table.close()

    def execute(self, operation, parameters=None):
        query = _bind_parameters(operation, parameters)
        self.__result = execute(
            query,
            tables=self.__tables
        )
        self.__iter = iter(self.__result)

    def executemany(self, operation, seq_of_parameters):
        raise NotSupportedError

    def fetchone(self):
        if self.__iter is None:
            raise ProgrammingError
        try:
            row = next(self.__iter)
            return tuple(row[col] for col in self.__result.columns)
        except StopIteration:
            self.__reset()
            return None

    def fetchmany(self, pagesize=None):
        raise NotSupportedError

    def fetchall(self):
        if self.__result is None:
            raise ProgrammingError
        try:
            return self.__result.rows[self.__iter.index+1:]
        finally:
            self.__reset()

    def __reset(self):
        self.__result = None
        self.__iter = None


def _bind_parameters(query, parameters):
    if not parameters:
        return query
    return query % {
        k: f"'{v}'" if isinstance(v, str) else v
        for k, v in parameters.items()
    }


class csv_table:
    __file: Iterable[str] = None
    __reader: DictReader = None

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __init__(self, path):
        self.__path = path

    def __iter__(self):
        return self.reader

    @property
    def columns(self):
        return self.reader.fieldnames

    @property
    def file(self):
        if not self.__file or self.__file.closed:
            self.__file = open(self.__path)
        return self.__file

    @property
    def reader(self):
        if not self.__reader:
            self.file.seek(0)
            self.__reader = DictReader(self.file)
        return self.__reader

    def close(self):
        if self.__file:
            self.__file.close()


class Error(Exception):
    pass


class Warning(Exception):
    pass


class InterfaceError(Error):
    pass


class DatabaseError(Error):
    pass


class InternalError(DatabaseError):
    pass


class OperationalError(DatabaseError):
    pass


class ProgrammingError(DatabaseError):
    pass


class IntegrityError(DatabaseError):
    pass


class DataError(DatabaseError):
    pass


class NotSupportedError(DatabaseError):
    pass


STRING = 'STRING'
