import sqlite3

con = sqlite3.connect(r"C:\_dev\Python_INAP_Avanzado\TEMA_5\ejemplo.db")

cur= con.cursor()

##cur.execute('''CREATE TABLE stocks (date text, trans text, symbol text, qty real, price real)''')  //// SOLO LA PRIMERA VEZ

cur.execute("INSERT INTO stocks VALUES ('2006-01-05','BUY','RHAT',100,35.14)")
cur.execute("INSERT INTO stocks VALUES ('2006-09-05','SELL','AWTG',200,80.99)")

con.commit()
con.close()