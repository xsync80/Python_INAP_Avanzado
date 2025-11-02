import json
from pymongo import MongoClient
import logging

# Conexión a MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['bd']      
collection = db['mi_coleccion']      

# Carga el fichero JSON
with open("hogar.json", "r", encoding="utf-8") as file:
    data = json.load(file)

print("Datos cargados correctamente.")