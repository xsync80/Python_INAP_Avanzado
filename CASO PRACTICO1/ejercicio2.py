import json
from pymongo import MongoClient
import logging
from bson.decimal128 import Decimal128
from decimal import Decimal

# Conexión a MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['bd']      
collection = db['mi_coleccion']   


def calcular_media(db,colletion, camp):

