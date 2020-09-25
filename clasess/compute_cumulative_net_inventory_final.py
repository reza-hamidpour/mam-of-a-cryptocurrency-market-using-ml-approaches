from datetime import timedelta
from datetime import datetime
import asyncio
import math


class ComputeCNT:
    users = None
    processed_users = None
    queue_size = 10
    number_of_tasks = 0

    def __init__(self, operations, db, working_collection, opening_time, closing_time, asset):
        self.bucket = operations
        self.cumulative_net_inventory = db[working_collection]
        self.opening_time = datetime.strptime(opening_time, "%Y-%m-%dT%H:%M:%SZ")
        self.closing_time = datetime.strptime(closing_time, "%Y-%m-%dT%H:%M:%SZ")

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
        self.users = self.bucket.aggregate(pipeline=query, allowDiskUse=True)

    async def get_users_number(self):
        query = [
            {
                "$group": {
                    "_id": "$source_account"
                }
            },
            {
                "$group": {
                    "_id": None,
                    "total": {"$sum": 1}
                }
            }
        ]
        return self.bucket.aggregate(pipeline=query)

    async def handle_users(self):
        loop = asyncio.get_event_loop()
        queue = asyncio.Queue(self.queue_size)
        number_of_users = await list(await self.get_users_number())[0]["total"]
        tasks = [loop.create_task(self.compute_CNI_per_user(queue)) for _ in range(number_of_users)]
        for user in self.users:
            transactions = await self.get_user_change_in_inventory_records(user["_id"])
            await queue.put({
                "source_account": user["_id"],
                "transactions": transactions
            })
            self.number_of_tasks += 1
            if self.number_of_tasks > 10:
                await queue.join()
                self.number_of_tasks = 0
        await queue.join()
        self.number_of_tasks = 0

    async def get_user_change_in_inventory_records(self, source_account):
        query = [
            {"$match": {
                "source_account": source_account
            }
            },
            {"$sort": {"start_time_window": 1}},
        ]
        return self.bucket.aggregate(pipeline=query, allowDiskUse=True)

    async def compute_CNI_per_user(self, queue):
        data = queue.get()
        current_time = self.opening_time
        end_time_window = current_time + timedelta(seconds=900)
        last_cumulative = 0.0
        while current_time >= end_time_window:
            time_window_transactions = await self.get_time_window_CNI(data["source_account"], end_time_window)
            cumulative_net_inventory = await self.get_comulative_change_inventory_for_period(time_window_transactions)
            if cumulative_net_inventory > 0:
                last_cumulative += cumulative_net_inventory
            else:
                last_cumulative += 0
            obj = await self.get_CNI_object(data["source_account"], last_cumulative, end_time_window)
            await self.save_CNI(obj)
            print("task ", data["task_number"], " finished successfully.")
            end_time_window = current_time + timedelta(seconds=900)

        await queue.task_done()

    async def get_comulative_change_inventory_for_period(self, transactions):
        cumulative_net_inventory = 0.0
        for transaction in transactions:
            cumulative_net_inventory += transaction["change_in_inventory"]
        return cumulative_net_inventory

    async def get_time_window_CNI(self, transactions, end_of_tw):
        TW_transactions = []
        for transaction in transactions:
            tmp = datetime.strptime(transaction["time_window"], "%Y-%m-%dT%H:%M:%S.%fZ")
            if tmp <= end_of_tw:
                TW_transactions.append(transaction)
            else:
                continue
        return TW_transactions

    async def get_CNI_object(self, source_account, CNI, end_of_period):
        return {
            "source_account": source_account,
            "cumulative_net_inventory": CNI,
            "end_of_time_period": end_of_period
        }

    async def save_CNI(self, current_CNI):
        self.cumulative_net_inventory.insert(current_CNI)
