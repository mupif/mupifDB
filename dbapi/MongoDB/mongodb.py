import db
import collection
from pymongo import MongoClient
# do not use flask_pymongo as module can be used outside flask


class MongoDB(db.Database):
    def __init__(self, app):
        self.client = MongoClient()
        self.db = client.MuPIF
        log.info("MongoDB connected")
    def __del__(self):
        self.db.close()
        self.db=None
    def close(self):
        if (self.db):
            self.db.close()
    def getCollection(self, id):
        match id:
            case collection.CollectionID.UseCaseCollection:
                return MongoCollection(self.db.UseCases)
            case collection.CollectionID.WorkflowCollection:
                return MongoCollection(self.db.Workflows)
            case collection.CollectionID.WorkflowExecutionCollection:
                return MongoCollection(collection.CollectionID.WorkflowExecutions)
            case _:
                raise ValueError('Unsupported collection id (%s)'%(id))
        pass

class MongoCollection(collection.Collection):
    def __init__(self, collection):
        self.collection = collection
        pass
    def findDocuments(self, searchCrit):
        """
            Returns array of Documents matching given search criteria
            :param dict searchCrit: Dictionary, where key correspond to record name and value to criteria to match the value.
        """
        return self.collection.find(searchCrit)
    def findDocument(self, searchCrit):
        """
            Returns single Document matching given search criteria
            :param dict searchCrit: Dictionary, where key correspond to record name and value to criteria to match the value.
        """
        return self.collection.find_one(searchCrit)

    def createDocument(self):
        """
            Creates and returns new Document in collection
        """
        rec = {}
        result = self.collection.insert_one(rec)
        return result.inserted_id

class MongoDocument(document.Document):
    """
        Representation of single document(record) in database
    """
    def __init__(self, collection, document):
        self.collection = collection
        self.document = document
    def toJson (self):
        """
            Returns document representation in json
        """ 
        return self.document
    def get (self, name):
        """
            Returns the value of attribute
        """
        return self.document[name]
    def set (self, name, value):
        """
            Sets attribute value
        """
        self.document[name]=value
    def update (self, data):
        """
            Updates the receiver according to given json data 
        """
        id = self.document['_id']
        self.collection.update_one({'_id': id}, {'$set': data}})
        