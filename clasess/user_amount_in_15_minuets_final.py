from datetime import datetime
from datetime import timedelta
import asyncio
import math
import time
from multiprocessing import Process, Queue, Manager


class userAmountIn15Minuets:
    users = None
    user_transcations = None
    cumulative_trading_volume = 0.0
    cumulative_number_of_trades = 0
    number_of_iterate = 0
    start_time = None
    end_time = None
    TASKs_Number = 101
    number_of_tasks = 0
    queue = None
    existe_users = None
    num_consumers = 7
    SENTINEL = "END"

    def __init__(self, operation, db, working_collection, active_asset, opening_time, closing_time):
        self.operations = operation
        self.working_collection = db[working_collection]

        self.opening_time = datetime.strptime(opening_time, "%Y-%m-%dT%H:%M:%SZ")
        self.closing_time = datetime.strptime(closing_time, "%Y-%m-%dT%H:%M:%SZ")
        self.active_asset = active_asset

    def query_wrong_users(self):
        query = [
            {"$group": {
                "_id": "$source_account", "total": {"$sum": 1}
            }
            },
            {"$match": {"total": {"$lt": 6428}}},
            {"$project": {"_id": "$_id"}}]
        return self.working_collection.aggregate(pipeline=query, allowDiskUse=True)

    def clear_wrong_users(self):
        # print("Start cleaning working collection...")
        for user in self.query_wrong_users():
            self.working_collection.remove({"source_account": user["_id"]})
        # print("Cleaning phase finished.")

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
        # print("Users Gathering finished.")

    async def handel_users(self):
        num_leeaved_user = 0
        self.clear_wrong_users()
        for user in self.users:
            num_leeaved_user += 1
            user_ = "GDXL5QOHISL2SGNEKDR7Q3MIVGVNZGZIZI3LUFFJP2N5XESEHNBTNPG5"
            # print("user(" + str(user['_id']) + ") started.")
            # checker = self.check_user_exist(user['_id'])
            checker = False
            if checker == False:
                transactions = await self.load_user_transactions(user_)
                await self.multi_process_compute_and_save_tv_tn(user_, transactions)
                # await self.async_compute_and_save_tv_tn(user["_id"])
            else:
                pass
                # print(f"{num_leeaved_user}-Leave this user.")
            del checker

    def check_user_exist(self, source_account):
        flag = False
        self.load_processed_users(source_account)
        for user in self.existe_users:
            if source_account == user["source_account"]:
                flag = True
                break
        return flag

    def load_processed_users(self, source_account):
        query = {"source_account": source_account}
        self.existe_users = self.working_collection.find(query)

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
        return self.operations.aggregate(pipeline=query)
        # self.user_transactions = self.operations.aggregate(pipeline=query)

    async def multi_process_compute_and_save_tv_tn(self, source_account, transactions):
        manager = Manager()
        que_ = Queue()

        producer = Process(target=self.task_producer_, args=(source_account,
                                                             self.opening_time,
                                                             self.closing_time,
                                                             transactions,
                                                             que_,
                                                             self.num_consumers))

        producer.start()
        # consumers = []
        # for k in range(self.num_consumers):
        #     consumers.append(
        #         Process(target=self.task_consumer, args=(k, que_))
        #     )
        #     consumers[k].start()
        que_.close()
        que_.join_thread()
        producer.join()
        # for k in range(self.num_consumers):
        #     consumers[k].join()
        #
        # for k in range(self.num_consumers):
        #     consumers[k].terminate()

    def task_producer_(self, source_account, current_time, end_time, transactions, queue, num_workers):
        print(type(current_time), type(end_time))
        while current_time >= end_time:
            end_of_time_window = current_time + timedelta(seconds=900)
            tw_transactions = []
            for tran in transactions:
                tmp = datetime.strptime(tran["created_at"], "%Y-%m-%dT%H:%M:%S.%fZ")
                print(
                    f"start : {current_time} , tmp : {tmp} , end : {end_of_time_window} , res : {current_time <= tmp <= end_of_time_window}")
                if current_time <= tmp <= end_of_time_window:
                    tw_transactions.append(tran)
            queue.put({
                "transactions": tw_transactions,
                "source_account": source_account,
                "current_time": current_time,
                "end_time": end_of_time_window
            })
            current_time = end_of_time_window

        for i in range(num_workers):
            queue.put({
                "transactions": self.SENTINEL
            })

    def task_producer(self, source_account, start_time, end_time, transactions, queue):
        current_time = start_time
        while current_time <= end_time:
            end_of_time_window = current_time + timedelta(seconds=900)
            tw_transactions = []
            for transaction in transactions:
                tmp = datetime.strptime(transaction["created_at"], "%Y-%m-%dT%H:%M:%S.%fZ")
                print(
                    f"start : {current_time} , tmp : {tmp} , end : {end_of_time_window} , res : {current_time <= tmp <= end_of_time_window}")
                if current_time <= tmp <= end_of_time_window:
                    tw_transactions.append(transaction)
            queue.put({"transactions": tw_transactions,
                       "source_account": source_account,
                       "current_time": current_time,
                       "end_time": end_of_time_window})
            current_time = end_of_time_window
        for i in range(self.num_consumers):
            queue.put({
                "transactions": self.SENTINEL
            })
        # print(f"{source_account} time windows finished.")

    def task_consumer(self, worker_num, queue):
        while True:
            obj = queue.get()
            if obj['transactions'] != self.SENTINEL:
                TV_and_NofT = self.compute_trading_volume_number_of_trades(obj["transactions"])
                # self.cumulative_trading_volume += TV_and_NofT["trading_volume"]
                # self.cumulative_number_of_trades += TV_and_NofT["number_of_trades"]
                obj_result = {
                    "source_account": obj["source_account"],
                    "asset": self.active_asset,
                    "time_window": datetime.strftime(obj["current_time"], "%Y-%m-%dT%H:%M:%S.%fZ"),
                    "trading_volume": TV_and_NofT["trading_volume"],
                    "number_of_trades": TV_and_NofT["number_of_trades"],
                    # "cumulative_tv": self.cumulative_trading_volume,
                    # "comulative_nt": self.cumulative_number_of_trades
                }
                # self.save_tv_and_nt_per_user_in_15_minuets(obj_result)
            elif obj['transactions'] == self.SENTINEL:
                print(f"Worker {worker_num} finished.")
                break

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
            # print("put item in queue, Size are : ", queue.qsize())
            current_time = end_of_time_window
            if self.number_of_tasks > 100:
                # print("waiting for tasks to join to gether.")
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
        # print("TV and NofT at time " + obj_result["time_window"] + " inserted.")
        await queue.task_done()

    async def compute_and_save_tv_tn(self, queue):
        source_account = await queue.get()
        current_time = self.opening_time
        # print(source_account, " started")
        while current_time <= self.closing_time:
            end_of_time_window = current_time + timedelta(seconds=900)
            transactions = await self.query_on_user_transactions(source_account, current_time, end_of_time_window)
            TV_and_NofT = await self.compute_trading_volume_number_of_trades(transactions)
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
        # print(source_account, " finished successfully.")
        queue.task_done()

    async def load_time_window_transactions(self, start_time_window, end_time_window, transactions=None):
        tw_transactions = []
        for transaction in transactions:
            tmp = datetime.strptime(transaction["created_at"], "%Y-%m-%dT%H:%M:%S.%fZ")
            print(
                f"start : {start_time_window} , tmp : {tmp} , end : {end_time_window} , res : {start_time_window <= tmp <= end_time_window}")
            if start_time_window <= tmp <= end_time_window:
                tw_transactions.append(transaction)
            else:
                continue
        return tw_transactions

    def query_on_user_transactions(self, source_account, start_time, end_time):
        # print(" Start query on transactions at ", str(start_time))
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
        # print("transaction query at ", str(start_time) + " finished.")
        return self.operations.aggregate(pipeline=query, allowDiskUse=True)

    def compute_trading_volume_number_of_trades(self, transactions):
        trading_volume = 0
        number_of_trades = 0
        for transaction in transactions:
            if (hasattr(transaction, "selling_asset_type") and
                transaction["selling_asset_type"] == self.active_asset) or \
                    (hasattr(transaction, "selling_asset_code") and
                     transaction["selling_asset_code"] == self.active_asset):
                trading_volume += float(transaction["amount"])
            elif (hasattr(transaction, "buying_asset_type") and
                  transaction["buying_asset_type"] == self.active_asset) or \
                    (hasattr(transaction, "buying_asset_code") and
                     transaction["buying_asset_code"] == self.active_asset):
                trading_volume += float(transaction["amount"]) * float(transaction['price'])
            number_of_trades += 1
            # trading_volume += float(transaction["amount"])

        return {"trading_volume": trading_volume, "number_of_trades": number_of_trades}

    def save_tv_and_nt_per_user_in_15_minuets(self, obj):
        self.working_collection.insert(obj)
