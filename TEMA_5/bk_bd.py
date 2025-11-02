import sqlite3

def progress(status, remaining, total):
    print(f'Copadas{total-remaining} de {total} páginas')

con = sqlite3.connect(r"C:\_dev\Python_INAP_Avanzado\TEMA_5\ejemplo.db")
bck = sqlite3.connect(r"C:\_dev\Python_INAP_Avanzado\TEMA_5\backup.db")


with bck:

    con.backup(bck,pages=1, progress=progress)

bck.close()
con.close()