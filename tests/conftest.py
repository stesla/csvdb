import os

import pytest

import csvdb


DBPATH = os.path.join(os.path.dirname(__file__), 'testdb')


@pytest.fixture
def cursor():
    with csvdb.connect(DBPATH) as conn:
        with conn.cursor() as cursor:
            yield cursor
