import collection

class Database():
    """ 
        Abstract class representing database connection.
    """ 
    def __int__(self):
        pass
    def close(self):
        pass
    def getCollection(self, id):
        """
            Returns the collection identified by id
            :param CollectionID id: collection id
        """
        pass
