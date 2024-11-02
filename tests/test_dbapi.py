def test_query(cursor):
    cursor.execute("""SELECT last_name, first_name
                      FROM users ORDER BY last_name""")
    expected = [
        ('Spy', 'Eve'),
        ('User', 'Alice'),
        ('User', 'Bob'),
    ]
    assert cursor.description is not None
    assert expected == list(cursor.fetchall())
