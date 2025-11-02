import pprint

from pymongo import MongoClient

# Conectar a MongoDB (por defecto, localhost:27017)
client = MongoClient('mongodb://localhost:27017/')

# O si tu MongoDB está en otro lugar:
# client = MongoClient('mongodb://user:password@host:port/')

# Seleccionar una base de datos
db = client['local']

# Seleccionar una colección (equivalente a una tabla en bases de datos relacionales)

collection = db['mi_collection']
posts = db.posts
pprint.pprint(posts.find_one())

print("Conexión exitosa a MongoDB y selección de la colección.")


print("\nTodos los documentos en la colección:")
for doc in collection.find():
    print(doc)
    print("ddd")


print("\nDocumento con nombre 'Alice':")
alice_doc = collection.find_one({"nombre": "Alice"})
print(alice_doc)