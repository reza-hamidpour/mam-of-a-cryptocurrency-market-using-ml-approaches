from datetime import datetime
from datetime import timedelta
import asyncio
import math

class ComputeChangeInInventory:
    transactions = None
    short_positions_amount = 0.0
    long_positions_amount = 0.0
    number_of_short_positions = 0
    number_of_long_positions = 0
    users = None
    queue_size = 20

    def __init__(self, operations, db, working_collection, opening_time, closing_time, active_asset):
        self.operations = operations
        self.working_collection = db[working_collection]
        self.opening_time = datetime.strptime(opening_time, "%Y-%m-%dT%H:%M:%SZ")
        self.closing_time =datetime.strptime(closing_time, "%Y-%m-%dT%H:%M:%SZ")
        self.active_asset = active_asset

    def get_users(self):
        query = [
            {
                "$sort": {"created_at": 1}
            },
            {
                "$group": {
                    "_id": "$source_account"
                }
            }
        ]
        self.users = self.operations.aggregate(pipeline=query, allowDiskUse=True)

    def users_handler(self):
        for user in self.users:
            self.compute_cumulative_net_inventory(user["_id"])

    async def tasks_handler_for_net_inventory(self, source_account):
        number_of_tasks = 0
        loop = asyncio.get_event_loop()
        queue = asyncio.Queue(self.queue_size)
        difference_between_o_and_c = (self.closing_time - self.opening_time).total_seconds()
        number_of_TW = int(math.ceil(difference_between_o_and_c / 900.0))
        tasks = [loop.create_task(self.compute_cumulative_net_inventory(queue)) for _ in range(number_of_TW)]
        current_time = self.opening_time
        while current_time <= self.closing_time:
            number_of_tasks += 1
            await queue.put({
                "source_account": source_account,
                "start_time_window": current_time,
                "task_number": number_of_tasks
            })
            current_time = current_time + timedelta(seconds=900)
            if number_of_tasks > 19:
                print("Waiting for tasks... .")
                await queue.join()
                number_of_tasks = 0
        print("Waiting for tasks... .")
        await queue.join()

    async def compute_cumulative_net_inventory(self, queue):
        data = queue.get()
        end_of_time_window = data["start_time_window"] + timedelta(seconds=900)
        await self.query_on_transactions(data["source_account"], data["start_time_window"], end_of_time_window)
        await self.long_or_short_position()
        change_in_inventory = self.short_positions_amount - self.long_positions_amount
        obj = {
            "source_account": data["source_account"],
            "asset": self.active_asset,
            "time_window": data["start_time_window"],
            "change_in_inventory": change_in_inventory,
            "short_positions_amount": self.short_positions_amount,
            "number_of_short_positions": self.number_of_short_positions,
            "long_positions_amount": self.long_positions_amount,
            "number_of_long_positions": self.number_of_long_positions
        }
        await self.save_short_and_long_positions(obj)
        self.short_positions_amount = 0.0
        self.number_of_short_positions = 0
        self.long_positions_amount = 0.0
        self.number_of_long_positions = 0
        change_in_inventory = None
        print("Task ", data["task_number"], " finished successfully.")
        await queue.task_done()

    async def query_on_transactions(self, source_account, start_time, end_time):
        query = [
            { "$match": {
                "source_account": source_account,
                "and": [{"created_at": {"$gte": end_time}}, {"created_at": {"$lte": start_time}}]
            }
            },
            {"$sort": {"created_at": 1}}
        ]
        self.transactions = self.operations.aggregate(pipeline=query, allowDiskUse=True)

    async def long_or_short_position(self):
        for transaction in self.transactions:
            if (hasattr(transaction, "selling_asset_type") and
                transaction["selling_asset_type"] == self.active_asset) or \
                    (hasattr(transaction, "selling_asset_code") and
                     transaction["selling_asset_code"] == self.active_asset):
                self.short_positions_amount += float(transaction["amount"])
                self.number_of_short_positions += 1
            elif (hasattr(transaction, "buying_asset_type") and
                  transaction["buying_asset_type"] == self.active_asset) or \
                    (hasattr(transaction, "buying_asset_code") and
                     transaction["buying_asset_code"] == self.active_asset):
                self.long_positions_amount += float(transaction["amount"])
                self.number_of_long_positions += 1

    async def save_short_and_long_positions(self, obj):
        self.working_collection.insert(obj)
        # print("Positions at time " + obj["time_window"] + " saved.")