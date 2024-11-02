import pytest

from csvdb import dbapi


def test_description(cursor):
    cursor.execute("SELECT * FROM users")
    expected = [
        ('id', dbapi.STRING),
        ('username', dbapi.STRING),
        ('first_name', dbapi.STRING),
        ('last_name', dbapi.STRING),
    ]
    assert expected == cursor.description


def test_fetchall(cursor):
    cursor.execute("""SELECT last_name, first_name
                      FROM users ORDER BY last_name""")
    expected = [
        ('Spy', 'Eve'),
        ('User', 'Alice'),
        ('User', 'Bob'),
    ]
    assert cursor.description is not None
    assert expected == list(cursor.fetchall())


def test_fetchone(cursor):
    cursor.execute("SELECT id, username FROM users")
    assert ('1', 'auser') == cursor.fetchone()
    assert ('2', 'buser') == cursor.fetchone()
    assert ('3', 'espy') == cursor.fetchone()
    assert cursor.fetchone() is None
    with pytest.raises(dbapi.Error):
        cursor.fetchone()
    assert cursor.description is None


def test_fetchone_then_fetchall(cursor):
    cursor.execute("SELECT id, username FROM users")
    cursor.fetchone()
    expected = [
        ('2', 'buser'),
        ('3', 'espy'),
    ]
    assert expected == cursor.fetchall()


def test_fetchall_twice(cursor):
    cursor.execute("SELECT id, username FROM users")
    cursor.fetchall()
    with pytest.raises(dbapi.Error):
        cursor.fetchall()
