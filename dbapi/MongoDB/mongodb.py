import db
import collection
from flask_pymongo import PyMongo

class MongoDB(db.Database):
    def __init__(self, app):
        self.mongo = PyMongo(app)
    def close(self):
        self.mongo.close()
    def getCollection(self, id):
        pass


