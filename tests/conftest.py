import os

import pytest

import csvdb.dbapi as db


DBPATH = os.path.join(os.path.dirname(__file__), 'testdb')


@pytest.fixture
def cursor():
    with db.connect(DBPATH) as conn:
        with conn.cursor() as cursor:
            yield cursor
