import sqlite3

con = sqlite3.connect(":memory:")

cur = con.cursor()

cur.execute("create table lang (name, first_appeared)")
cur.execute("insert into lang values (?,?)", ("c",1972))

lang_list=[
    ("Fortran", 1957),
    ("Python", 1991),
    ("Go", 2009)

]

cur.executemany("insert into lang values (?,?)", lang_list)


cur.execute("select * from lang").fetchall()

cur.execute("select * from lang where first_appeared=:year", {"year":1972})

print(cur.fetchall())

cur.close()
