import json
from pymongo import MongoClient # type: ignore
import pprint


# Conexión a MongoDB
client = MongoClient('mongodb://localhost:27017/')


db = client.test_database 

posts = db.posts

pprint.pprint(posts.find_one())

