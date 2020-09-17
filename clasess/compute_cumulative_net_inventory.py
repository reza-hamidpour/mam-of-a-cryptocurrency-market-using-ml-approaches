from datetime import timedelta
from datetime import datetime
import asyncio
import math

class CumulativeNetInventory:
    users = None
    end_of_query_range = None
    last_cumulative = 0.0
    queue_size = 10

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
            self.handle_tasks(user["_id"])

    async def handle_tasks(self, source_account):
        current_time  = self.opening_time
        task_number = 0
        loop = asyncio.get_event_loop()
        queue = asyncio.Queue(self.queue_size)
        difference_between_o_and_c = (self.closing_time - self.opening_time).total_seconds()
        number_of_TW = int(math.ceil(difference_between_o_and_c / 900.0))
        tasks = [loop.create_task(self.compute_CNI(queue)) for _ in range(number_of_TW)]
        while current_time <= self.closing_time:
            end_time_window = current_time + timedelta(seconds=900)
            task_number += 1
            await queue.put({
                "source_account": source_account,
                "end_time_window": end_time_window,
                "task_number": task_number
            })
            if task_number > 9:
                print("Waiting for tasks... .")
                await queue.join()
                task_number = 0
            current_time = end_time_window
        print("Waiting for tasks... .")
        await queue.join()

    async def compute_CNI(self, queue):
        data = queue.get()
        self.last_cumulative = 0.0
        cumulative_net_inventory = list(await self.query_range(data["source_account"], data["end_time_window"]))
        if len(cumulative_net_inventory) >= 1:
            self.last_cumulative += cumulative_net_inventory[0]["cumulative_net_inventory"]
        else:
            self.last_cumulative += 0
        obj = await self.get_CNI_object(data["source_account"], self.last_cumulative, self.end_of_query_range)
        await self.save_CNI(obj)
        print("task ", data["task_number"], " finished successfully.")
        await queue.task_done()

    async def query_range(self, current_user, current_time):
        query = [
                {"$match": {
                    "source_account": current_user,
                    "$and": [{"start_time_window": {"$lte": current_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")}},
                             {"start_time_window": {"$gte": self.opening_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")}}]
                }},
                {"$sort": {"start_time_window": 1}},
                {"$group": {
                    "_id": "source_account",
                    "cumulative_net_inventory": {"$sum": "change_in_inventory"}
                }}
                ]
        return self.bucket.aggregate(pipeline=query, allowDiskUse=True)

    async def get_CNI_object(self, source_account, CNI, end_of_period):
        return{
            "source_account": source_account,
            "cumulative_net_inventory": CNI,
            "end_of_time_period": end_of_period
        }

    async def save_CNI(self, current_CNI):
        self.cumulative_net_inventory.insert(current_CNI)
        # print("Time Window: ", current_CNI["end_of_time_period"], " ,inserted.")