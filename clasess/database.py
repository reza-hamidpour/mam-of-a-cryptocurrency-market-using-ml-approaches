import pymongo
from pymongo import MongoClient
from datetime import datetime
from datetime import timedelta

class Database:
    op_col = "operations"
    leg_col = "ledgers"
    db_name = "stellar"
    db = None

    def __init__(self):
        client = MongoClient()
        self.db = client[self.db_name]

    def get_db(self):
        return self.db

    def select_collection(self, collection):
        if collection in self.db.list_collection_names():
            return self.db[collection]
        else:
            return None

    # def select_new_collection(self, col_name):
    #     return self.db[]
    # def test(self):
    #     handler = self.select_collection("operations")
    #     elements = handler.find()
    #     # for element in elements:
    #     date = datetime.strptime(elements[0]["created_at"], "%Y-%m-%dT%H:%M:%S.%fZ")
    #     next_date = date + timedelta(days=1)
    #     print(elements[0]["created_at"])
    #     print(date)
    #     print(next_date)
