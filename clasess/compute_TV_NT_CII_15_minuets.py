from datetime import datetime
from datetime import timedelta
import asyncio
import math
import string
import time
from multiprocessing import Process


class userTvNtCii:
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
        # for user in self.users:
        #     print("user(" + str(user['_id']) + ") started.")
        #     check_user = await self.check_user_exists(user["_id"])
        #     if check_user == False:
        #         user_transactions = await self.load_user_transactions(user["_id"])
        source_account = "GBIRUXHWLTBEMTTGQBBYLELTG5TU7TAVTU6NVM2UKED5JDMO5CSPUNEA"
        user_transactions = await self.load_user_transactions(source_account)
        await self.async_compute_and_save_tv_tn(source_account, user_transactions)
        sa = "GD2HXONXAHY4HETCLEFSFR5AQLAQA3POCQSRJOO4ADDWXXWMSFITWNA2"
        user_transactions = await self.load_user_transactions(sa)
        await self.async_compute_and_save_tv_tn(sa, user_transactions)
            # else:
            #    print("Leave this user.")
        print("Finish.")


    async def check_user_exists(self, source_account):
        query = {
            "source_account": source_account
        }
        processed_users = self.working_collection.find_one(query)
        check = False
        if processed_users != None and len(list(processed_users)) > 0:
            check = True
        return check

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
        return list(self.operations.aggregate(pipeline=query, allowDiskUse=True))

    async def async_compute_and_save_tv_tn(self, source_account, user_transactions):
        loop = asyncio.get_event_loop()
        queue = asyncio.Queue(self.TASKs_Number)
        difference_between_o_and_c = (self.closing_time - self.opening_time).total_seconds()
        number_of_TW = int(math.ceil(difference_between_o_and_c / 900.0))
        tasks = [loop.create_task(self.tv_tn_cii_computing(queue)) for _ in range(number_of_TW)]
        difference_between_o_and_c = None
        number_of_TW = None
        current_time = self.opening_time
        while current_time <= self.closing_time:
            self.number_of_tasks += 1
            end_of_time_window = current_time + timedelta(seconds=900)
            transactions = await self.load_time_window_transactions(current_time, end_of_time_window, user_transactions)
            await queue.put({"transactions": transactions,
                            "source_account": source_account,
                             "current_time": current_time,
                             "end_time": end_of_time_window})
            current_time = end_of_time_window
            if self.number_of_tasks > 10:
                self.number_of_tasks = 0
                await queue.join()
        self.number_of_tasks = 0
        self.user_transactions = None
        if self.number_of_tasks > 0:
            await queue.join()
            tasks = []

    async def tv_tn_cii_computing(self, queue):
        obj = await queue.get()
        TV_and_NofT = await self.compute_trading_volume_number_of_trades(obj["transactions"])
        long_short_obj = await self.long_or_short_position(obj["transactions"])
        change_in_inventory = long_short_obj["short_positions_amount"] - long_short_obj["long_positions_amount"]
        self.cumulative_trading_volume += TV_and_NofT["trading_volume"]
        self.cumulative_number_of_trades += TV_and_NofT["number_of_trades"]
        obj_result = {
            "source_account": obj["source_account"],
            "asset": self.active_asset,
            "time_window": datetime.strftime(obj["current_time"], "%Y-%m-%dT%H:%M:%S.%fZ"),
            "trading_volume": str(TV_and_NofT["trading_volume"]),
            "number_of_trades": TV_and_NofT["number_of_trades"],
            "change_in_inventory": str(change_in_inventory),
            "short_positions_amount": str(long_short_obj["short_positions_amount"]),
            "number_of_short_positions": str(long_short_obj["short_positions_number"]),
            "long_positions_amount": str(long_short_obj["long_positions_amount"]),
            "number_of_long_positions": str(long_short_obj["long_positions_number"]),
            "cumulative_tv": str(self.cumulative_trading_volume),
            "comulative_nt": str(self.cumulative_number_of_trades)
        }
        await self.save_tv_and_nt_per_user_in_15_minuets(obj_result)
        # print("TV and NofT at time " + obj_result["time_window"] + " inserted.")
        try:
            await queue.task_done()
        except Exception as e:
            pass

    async def long_or_short_position(self, transactions):
            long_positions_amount = 0.0
            long_positions_number = 0
            short_positions_amount = 0.0
            short_positions_number = 0
            for transaction in transactions:
                if "selling_asset_type" in transaction and \
                        transaction["selling_asset_type"] == self.active_asset.upper():
                    short_positions_amount += float(transaction["amount"])
                    short_positions_number += 1
                elif "selling_asset_code" in transaction and \
                        transaction["selling_asset_code"] == self.active_asset.upper():
                    short_positions_amount += float(transaction["amount"])
                    short_positions_number += 1
                elif "buying_asset_type" in transaction and \
                        transaction["buying_asset_type"] == self.active_asset.upper():
                    long_positions_amount += float(transaction["amount"])
                    long_positions_number += 1
                elif "buying_asset_code" in transaction and \
                        transaction["buying_asset_code"] == self.active_asset.upper():
                    long_positions_amount += float(transaction["amount"])
                    long_positions_number += 1
            return {"long_positions_amount": long_positions_amount, "long_positions_number": long_positions_number,
                    "short_positions_amount": short_positions_amount, "short_positions_number": short_positions_number}

    async def load_time_window_transactions(self, start_time_window, end_time_window, user_transactions):
        tw_transactions = []
        for transaction in user_transactions:
            tmp = datetime.strptime(transaction["created_at"], "%Y-%m-%dT%H:%M:%S.%fZ")
            if tmp >= start_time_window and tmp <= end_time_window:
                tw_transactions.append(transaction)
            else:
                continue
        return tw_transactions

    async def compute_trading_volume_number_of_trades(self, transactions):
        trading_volume = 0
        number_of_trades = 0
        for transaction in transactions:
            trading_volume += float(transaction["amount"])
            number_of_trades += 1
        return {"trading_volume": trading_volume, "number_of_trades": number_of_trades}

    async def save_tv_and_nt_per_user_in_15_minuets(self, obj):
        self.working_collection.insert(obj)
