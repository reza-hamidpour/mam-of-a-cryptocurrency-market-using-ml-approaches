from datetime import datetime
from datetime import timedelta
import asyncio
import math

class ComputeChangeInInventory:
    transactions = None
    number_of_tasks = 0
    users = None
    queue_size = 11
    user_transactions = None

    def __init__(self, operations, db, working_collection, opening_time, closing_time, active_asset):
        self.operations = operations
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

    async def users_handler(self):
        counter = 1
        for user in self.users:
            check_user = await self.check_user_exists(user["_id"])
            if  check_user == False:
                await self.load_user_transactions(user["_id"])
                print("Source account : ", user["_id"], " Started.")
                await self.tasks_handler_for_net_inventory(user["_id"])
            else:
                counter += 1
                print(counter, "- Leave this user")
        print("Finish.")

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
        self.user_transactions = self.operations.aggregate(query)

    async def tasks_handler_for_net_inventory(self, source_account):
        loop = asyncio.get_event_loop()
        queue = asyncio.Queue(self.queue_size)
        difference_between_o_and_c = (self.closing_time - self.opening_time).total_seconds()
        number_of_TW = int(math.ceil(difference_between_o_and_c / 900.0))
        tasks = [loop.create_task(self.compute_cumulative_net_inventory(queue)) for _ in range(number_of_TW)]
        difference_between_o_and_c = None
        number_of_TW = None
        current_time = self.opening_time
        while current_time <= self.closing_time:
            end_of_time_window = current_time + timedelta(seconds=900)
            try:
                transactions = await self.load_time_window_transactions(current_time, end_of_time_window)
                await queue.put({"transactions": transactions,
                                 "source_account": source_account,
                                 "start_time_window": current_time,
                                 "end_time_window": end_of_time_window,
                                 "task_number": self.number_of_tasks
                                 })
                # print("Task ", str(current_time), " added.")
            except Exception as e:
                print(e)
            self.number_of_tasks += 1
            current_time = current_time + timedelta(seconds=900)
            if self.number_of_tasks >= 10:
                # print(" waiting for tasks complete... .")
                await queue.join()
                self.number_of_tasks = 0
        await queue.join()

    async def compute_cumulative_net_inventory(self, queue):
        data = await queue.get()
        try:
            long_short_obj = await self.long_or_short_position(data["transactions"])
            change_in_inventory = long_short_obj["short_positions_amount"] - long_short_obj["long_positions_amount"]
            obj = {
                "source_account": data["source_account"],
                "asset": self.active_asset,
                "time_window": data["start_time_window"],
                "change_in_inventory": change_in_inventory,
                "short_positions_amount": long_short_obj["short_positions_amount"],
                "number_of_short_positions": long_short_obj["short_positions_number"],
                "long_positions_amount": long_short_obj["long_positions_amount"],
                "number_of_long_positions": long_short_obj["long_positions_number"]
            }
            await self.save_short_and_long_positions(obj)
            # print("Positions at time " + str(data["start_time_window"]) + " inserted.")
            change_in_inventory = None
            await queue.task_done()
        except Exception as e:
            pass

    async def load_time_window_transactions(self, start_time_window, end_time_window):
        tw_transactions = []
        for transaction in self.user_transactions:
            tmp = datetime.strptime(transaction["created_at"], "%Y-%m-%dT%H:%M:%S.%fZ")
            if tmp >= start_time_window and tmp <= end_time_window:
                tw_transactions.append(transaction)
            else:
                continue
        return tw_transactions

    async def query_on_transactions(self, source_account, start_time, end_time):
        query = [{
            "$match": {
                "source_account": source_account,
                "and": [{
                    "created_at": {"$gte": end_time}
                },
                    {
                        "created_at": {"$lte": start_time}
                     }
                ]
            }
        },
            {"$sort": {"created_at": 1}}
        ]
        self.transactions = self.operations.aggregate(pipeline=query, allowDiskUse=True)

    async def long_or_short_position(self, transactions):
        long_positions_amount = 0.0
        long_positions_number = 0
        short_positions_amount = 0.0
        short_positions_number = 0
        for transaction in transactions:
            if (hasattr(transaction, "selling_asset_type") and
                transaction["selling_asset_type"] == self.active_asset) or \
                    (hasattr(transaction, "selling_asset_code") and
                     transaction["selling_asset_code"] == self.active_asset):
                short_positions_amount += float(transaction["amount"])
                short_positions_number += 1
            elif (hasattr(transaction, "buying_asset_type") and
                  transaction["buying_asset_type"] == self.active_asset) or \
                    (hasattr(transaction, "buying_asset_code") and
                     transaction["buying_asset_code"] == self.active_asset):
                long_positions_amount += float(transaction["amount"])
                long_positions_number += 1
        return {"long_positions_amount": long_positions_amount, "long_positions_number": long_positions_number,
                "short_positions_amount": short_positions_amount, "short_positions_number": short_positions_number}

    async def save_short_and_long_positions(self, obj):
        self.working_collection.insert(obj)
        # print("Positions at time " + obj["time_window"] + " saved.")

    async def check_user_exists(self, source_account):
        query = {
            "source_account": source_account
        }
        processed_users = self.working_collection.find_one(query)
        check = False

        if processed_users != None and len(list(processed_users)) > 0:
            check = True
        return check
