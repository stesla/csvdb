from csv import DictReader
from pathlib import Path

from sqlglot.executor import execute


def connect(dir):
    return Connection(dir)


class Connection:
    def __init__(self, dir):
        self.__paths = Path(dir).glob("*.csv")

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
        self.__result = execute(
            operation,
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


class csv_table:
    def __init__(self, path):
        self.__file = open(path)

    def __iter__(self):
        self.__file.seek(0)
        return DictReader(self.__file)

    def __del__(self):
        self.close()

    def close(self):
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
