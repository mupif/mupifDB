import enum

class CollectionID(enum.IntEnum):
    UseCaseCollection = 1
    WorkflowCollection = 2
    WorkflowExecutionCollection = 3

class Collection():
    def __init__(self, db):
        self.db = db
        pass
    def findDocuments(self, searchCrit):
        """
            Returns array of Documents matching given search criteria
            :param dict searchCrit: Dictionary, where key correspond to record name and value to criteria to match the value.
        """
        pass
    def findDocument(self, searchCrit):
        """
            Returns single Document matching given search criteria
            :param dict searchCrit: Dictionary, where key correspond to record name and value to criteria to match the value.
        """
        pass
    def createDocument(self):
        """
            Creates and returns new Document in collection
        """
        pass 