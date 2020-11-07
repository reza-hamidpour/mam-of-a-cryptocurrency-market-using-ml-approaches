from datetime import datetime
from datetime import timedelta
import asyncio
import math
import time
from multiprocessing import Process


class userAmountIn15Minuets:
    users = None
    user_transcations = None
    cumulative_trading_volume = 0.0
    cumulative_number_of_trades = 0
    number_of_iterate = 0
    start_time = None
    end_time = None
    TASKs_Number = 11
    number_of_tasks = 0
    queue = None
    number_of_users_added = 10

    def __init__(self, operation, db, working_collection, active_asset, opening_time, closing_time):
        self.operations = operation
        self.working_collection = db[working_collection]
        self.opening_time = datetime.strptime(opening_time, "%Y-%m-%dT%H:%M:%SZ")
        self.closing_time = datetime.strptime(closing_time, "%Y-%m-%dT%H:%M:%SZ")
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
        print("Users Gathering finished.")

    async def handel_users(self):
        for user in self.users:
            print("user(" + str(user['_id']) + ") started.")
            if self.number_of_users_added <= 0:
                await self.load_user_transactions(user["_id"])
                await self.async_compute_and_save_tv_tn(user["_id"])
            else:
                self.number_of_users_added -= 1
                print("Leave this user.")

    async def load_user_transactions(self, source_account):
        query = [
            {
                "$match": {
                    "source_account": source_account
                }
            },
            {
                "$sort": {
                    "created_at": 1
                }
            }
        ]
        self.user_transactions = self.operations.aggregate(pipeline=query)

    async def async_compute_and_save_tv_tn(self, source_account):
        loop = asyncio.get_event_loop()
        queue = asyncio.Queue(self.TASKs_Number)
        difference_between_o_and_c = (self.closing_time - self.opening_time).total_seconds()
        number_of_TW = int(math.ceil(difference_between_o_and_c / 900.0))
        tasks = [loop.create_task(self.tv_tn_computing(queue)) for _ in range(number_of_TW)]
        difference_between_o_and_c = None
        number_of_TW = None
        current_time = self.opening_time
        while current_time <= self.closing_time:
            self.number_of_tasks += 1
            end_of_time_window = current_time + timedelta(seconds=900)
            transactions = await self.load_time_window_transactions(current_time, end_of_time_window)
            await queue.put({"transactions": transactions,
                            "source_account": source_account,
                             "current_time": current_time,
                             "end_time": end_of_time_window})
            print("put item in queue, Size are : ", queue.qsize())
            current_time = end_of_time_window
            if self.number_of_tasks > 10:
                print("waiting for tasks to join to gether.")
                self.number_of_tasks = 0
                await queue.join()
                await asyncio.sleep(5)
        self.number_of_tasks = 0
        self.user_transactions = None
        await queue.join()
        tasks = []



    async def tv_tn_computing(self, queue):
        obj = await queue.get()
        TV_and_NofT = await self.compute_trading_volume_number_of_trades(obj["transactions"])
        self.cumulative_trading_volume += TV_and_NofT["trading_volume"]
        self.cumulative_number_of_trades += TV_and_NofT["number_of_trades"]
        obj_result = {
            "source_account": obj["source_account"],
            "asset": self.active_asset,
            "time_window": datetime.strftime(obj["current_time"], "%Y-%m-%dT%H:%M:%S.%fZ"),
            "trading_volume": TV_and_NofT["trading_volume"],
            "number_of_trades": TV_and_NofT["number_of_trades"],
            "cumulative_tv": self.cumulative_trading_volume,
            "comulative_nt": self.cumulative_number_of_trades
        }
        await self.save_tv_and_nt_per_user_in_15_minuets(obj_result)
        print("TV and NofT at time " + obj_result["time_window"] + " inserted.")
        await queue.task_done()

    async def compute_and_save_tv_tn(self, queue):
        source_account = await queue.get()
        current_time = self.opening_time
        print(source_account, " started")
        while current_time <= self.closing_time:
            end_of_time_window = current_time + timedelta(seconds=900)
            await self.query_on_user_transactions(source_account, current_time, end_of_time_window)
            TV_and_NofT = await self.compute_trading_volume_number_of_trades()
            # print("Trading volume and number of trades at " + str(current_time) + " computed.")
            self.cumulative_trading_volume += TV_and_NofT["trading_volume"]
            self.cumulative_number_of_trades += TV_and_NofT["number_of_trades"]
            obj = {
                "source_account": source_account,
                "asset": self.active_asset,
                "time_window": datetime.strftime(current_time, "%Y-%m-%dT%H:%M:%S.%fZ"),
                "trading_volume": TV_and_NofT["trading_volume"],
                "number_of_trades": TV_and_NofT["number_of_trades"],
                "cumulative_tv": self.cumulative_trading_volume,
                "comulative_nt": self.cumulative_number_of_trades
            }
            await self.save_tv_and_nt_per_user_in_15_minuets(obj)
            current_time = end_of_time_window
        print(source_account, " finished successfully.")
        queue.task_done()

    async def load_time_window_transactions(self, start_time_window, end_time_window):
        tw_transactions = []
        for transaction in self.user_transactions:
            tmp = datetime.strptime(transaction["created_at"], "%Y-%m-%dT%H:%M:%S.%fZ")
            if tmp >= start_time_window and tmp <= end_time_window:
                tw_transactions.append(transaction)
            else:
                continue
        return tw_transactions

    def query_on_user_transactions(self, source_account, start_time, end_time):
        print(" Start query on transactions at ", str(start_time))
        query = [
            {
                "$match": {
                    "source_account": source_account,
                    "offer_id": "0",
                    "and": [{"created_at": {"$gte": start_time}}, {"created_at": {"$lte": end_time}}]
                }
            },
            {"$sort": {"created_at": 1}}
        ]
        print("transaction query at ", str(start_time) + " finished.")
        return self.operations.aggregate(pipeline=query, allowDiskUse=True)

    async def compute_trading_volume_number_of_trades(self, transactions):
        trading_volume = 0
        number_of_trades = 0
        for transaction in transactions:
            trading_volume += float(transaction["amount"])
            number_of_trades += 1
        return {"trading_volume": trading_volume, "number_of_trades": number_of_trades}

    async def save_tv_and_nt_per_user_in_15_minuets(self, obj):
        self.working_collection.insert(obj)
