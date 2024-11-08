import os

import pytest

from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import Session

import csvdb.dbapi as db
import csvdb.sqlalchemy  # noqa: F401


DBPATH = os.path.join(os.path.dirname(__file__), 'testdb')


@pytest.fixture
def cursor():
    with db.connect(DBPATH) as conn:
        with conn.cursor() as cursor:
            yield cursor


@pytest.fixture
def inspector(session):
    return inspect(session.bind.engine)


@pytest.fixture
def session():
    engine = create_engine(f'csvdb:///{DBPATH}')
    with Session(engine) as session:
        yield session
