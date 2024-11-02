import os

import csvdb


DBPATH = os.path.join(os.path.dirname(__file__), 'testdb')


def test_query():
    conn = csvdb.connect(DBPATH)
    cur = conn.cursor()
    cur.execute("SELECT last_name, first_name FROM users ORDER BY last_name")
    expected = [
        ('Spy', 'Eve'),
        ('User', 'Alice'),
        ('User', 'Bob'),
    ]
    assert cur.description is not None
    assert expected == list(cur.fetchall())