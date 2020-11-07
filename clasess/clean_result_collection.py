
class CleanCollection:
    working_records = None

    def __init__(self, working_collection, operation):
        self.working_collection = working_collection
        self.operation = operation

    def query_on_working_c(self):
        query = [
            {
                "$group": {
                    "_id": "$source_account"
                }
            }
        ]
        self.working_records = self.working_collection.aggregate(pipeline=query, allowDiskUse=True)

    def clean_operation(self):
        for user in self.working_records:
            self.delete_current_user(user["_id"])
            print("User ", user["_id"], " deleted.")

    def delete_current_user(self, source_account):
        query = {
            "source_account": source_account
        }
        self.operation.delete_many(query)