import datetime

from sqlalchemy import select, type_coerce, Table, Column, String
from sqlalchemy.orm import declarative_base

from csvdb.sqlalchemy import Date, Datetime, Float, Integer


Base = declarative_base()


class User(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    username = Column(String)
    first_name = Column(String)
    last_name = Column(String)


data_table = Table(
    'data',
    Base.metadata,
    Column('key', String),
    Column('value', String),
)


def test_simple_query(session):
    result = session.query(User).all()
    assert [u.first_name for u in result] == ['Alice', 'Bob', 'Eve']


def test_integer_column(session):
    result = session.query(User).all()
    assert [u.id for u in result] == [1, 2, 3]


def test_type_coerce(session):
    tests = [
        ('float', Float, 4.2),
        ('int', Integer, 42),
        ('str', String, 'foo'),
        ('date', Date, datetime.date(2024, 11, 2)),
        ('datetime', Datetime, datetime.datetime(2024, 11, 2, 10, 51, 42)),
    ]
    for key, cls, expected in tests:
        actual = session.execute(
            select(
                type_coerce(data_table.c.value, cls)
            ).where(
                data_table.c.key == key
            )
        ).one()
        assert actual.value == expected
