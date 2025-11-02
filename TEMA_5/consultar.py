import sqlite3

con = sqlite3.connect(r"C:\_dev\Python_INAP_Avanzado\TEMA_5\ejemplo.db")

cur= con.cursor()

for row in cur.execute('SELECT * FROM stocks'):
    print(row)

##Aqui no va comit ya que solo consulta
con.close()