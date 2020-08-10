from datetime import timedelta
from datetime import datetime

class CumulativeNetInventory:
    users = None
    end_of_query_range = None
    last_cumulative = 0.0

    def __init__(self, bucket, db, cni_collection_name, opening_time, closing_time):
        self.bucket = bucket
        self.cumulative_net_inventory = db[cni_collection_name]
        self.opening_time = datetime.strptime(opening_time, "%Y-%m-%dT%H:%M:%SZ")
        self.closing_time = datetime.strptime(closing_time, "%Y-%m-%dT%H:%M:%SZ")

    def get_users(self):
        query = [{"$group": {
            "_id": "$source_account"
        }}]
        self.users = self.bucket.aggregate(pipeline=query, allowDiskUse=True)

    def handel_users(self):
        for user in self.users:
            self.compute_CNI(user["_id"])

    def compute_CNI(self, source_account):
        current_time = self.opening_time
        self.last_cumulative = 0.0
        while current_time <= self.closing_time:
            cumulative_net_inventory = list(self.query_range(source_account, current_time))
            current_time = current_time + timedelta(seconds=900)
            if len(cumulative_net_inventory) >= 1:
                self.last_cumulative += cumulative_net_inventory[0]["cumulative_net_inventory"]
            else:
                self.last_cumulative += 0
            obj = self.get_CNI_object(source_account, self.last_cumulative, self.end_of_query_range)
            self.save_CNI(obj)

    def query_range(self, current_user, current_time):
        self.end_of_query_range = current_time + timedelta(seconds=900)
        query = [
                {"$match": {
                    "source_account": current_user,
                    "$and": [{"start_time_window": {"$lte": self.end_of_query_range.strftime("%Y-%m-%dT%H:%M:%S.%fZ")}},
                             {"start_time_window": {"$gte": current_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")}}]
                }},
                {"$sort": {"start_time_window": 1}},
                {"$group": {
                    "_id": "source_account",
                    "cumulative_net_inventory": {"$sum": "change_in_inventory"}
                }}
                ]
        return self.bucket.aggregate(pipeline=query, allowDiskUse=True)

    def get_CNI_object(self, source_account, CNI, end_of_period):
        return{
            "source_account": source_account,
            "cumulative_net_inventory": CNI,
            "end_of_time_period": end_of_period
        }

    def save_CNI(self, current_CNI):
        self.cumulative_net_inventory.insert(current_CNI)
        print("Time Window: ", current_CNI["end_of_time_period"], " ,inserted.")