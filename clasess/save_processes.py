

class SaveProcesses:

    def __init__(self, db_handler, collection):
        self.db = db_handler
        self.collection = db_handler["exchange_rate"]

    def save(self, docs):
        response = self.collection.insert_many(docs)