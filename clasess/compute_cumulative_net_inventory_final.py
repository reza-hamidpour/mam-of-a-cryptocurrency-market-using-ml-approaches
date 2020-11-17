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
        self.asset = asset

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
        counter = 0
        loop = asyncio.get_event_loop()
        queue = asyncio.Queue(self.queue_size)
        number_of_users = list(await self.get_users_number())[0]["total"]
        tasks = [loop.create_task(self.compute_CNI_per_user(queue)) for _ in range(number_of_users)]
        for user in self.users:
            check_user = await self.check_user_exists(user["_id"])
            if check_user == False:
                transactions = await self.get_user_change_in_inventory_records(user["_id"])
                await queue.put({
                    "source_account": user["_id"],
                    "transactions": transactions
                })
                # print("Task number ", self.number_of_tasks, " pushed.")
                self.number_of_tasks += 1
                if self.number_of_tasks > 3:
                    await queue.join()
                    self.number_of_tasks = 0
            else:
                print(counter, "- leave this user.")
                counter += 1
        await queue.join()
        print("Finish.")
        self.number_of_tasks = 0

    async def get_user_change_in_inventory_records(self, source_account):
        query = [
            {"$match": {
                "source_account": source_account
            }
            },
            {"$sort": {"time_window": 1}},
        ]
        return self.bucket.aggregate(pipeline=query, allowDiskUse=True)

    async def compute_CNI_per_user(self, queue):
        data = await queue.get()
        current_time = self.opening_time
        end_time_window = current_time + timedelta(seconds=900)
        last_cumulative = 0.0
        print("User ", data["source_account"], " started.")
        while current_time <= self.closing_time:
            current_time = end_time_window
            time_window_transactions = await self.get_time_window_CNI(data["transactions"], end_time_window)
            cumulative_net_inventory = await self.get_comulative_change_inventory_for_period(time_window_transactions)
            if cumulative_net_inventory > 0:
                last_cumulative += cumulative_net_inventory
            else:
                last_cumulative += 0
            obj = await self.get_CNI_object(data["source_account"], last_cumulative, end_time_window)
            await self.save_CNI(obj)
            # print("Time Window ", end_time_window,  " inserted.")
            end_time_window = current_time + timedelta(seconds=900)
        try:
            await queue.task_done()
        except Exception as e:
            pass

    async def get_comulative_change_inventory_for_period(self, transactions):
        cumulative_net_inventory = 0.0
        for transaction in transactions:
            cumulative_net_inventory += float(transaction["change_in_inventory"])
        return cumulative_net_inventory

    async def get_time_window_CNI(self, transactions, end_of_tw):
        TW_transactions = []
        break_after_2 = 2
        for transaction in transactions:
            if transaction["time_window"] <= end_of_tw:
                TW_transactions.append({"change_in_inventory": transaction["change_in_inventory"]})
            else:
                break_after_2 -= 1
                if break_after_2 == 0:
                    break
                continue
        return TW_transactions

    async def get_CNI_object(self, source_account, CNI, end_of_period):
        return {
            "source_account": source_account,
            "asset": self.asset,
            "cumulative_net_inventory": CNI,
            "end_of_time_period": end_of_period
        }

    async def save_CNI(self, current_CNI):
        self.cumulative_net_inventory.insert(current_CNI)

    async def check_user_exists(self, source_account):
        query = {
            "source_account": source_account
        }
        processed_users = self.cumulative_net_inventory.find_one(query)
        check = False
        if processed_users != None and len(list(processed_users)) > 0:
            check = True
        return check