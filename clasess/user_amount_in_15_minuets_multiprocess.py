from datetime import datetime, timedelta
import math
import time
from multiprocessing import Process, Queue, Manager
from pymongo import MongoClient


class userAmountIn15Minuets:
    users = None
    cumulative_trading_volume = 0.0
    cumulative_number_of_trades = 0
    number_of_iterate = 0
    start_time = None
    end_time = None
    existe_users = None
    num_consumers = 7
    SENTINEL = "END"
    db_name = "stellar"

    def __init__(self, operation, db, working_collection, active_asset, opening_time, closing_time):
        self.operations = operation
        self.query_working_collection = db[working_collection]
        self.working_collection = working_collection
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
        return self.query_working_collection.aggregate(pipeline=query, allowDiskUse=True)

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
        self.existe_users = self.query_working_collection.find(query)

    def clear_wrong_users(self):
        print("Start cleaning working collection...")
        for user in self.query_wrong_users():
            self.query_working_collection.remove({"source_account": user["_id"]})
        print("Cleaning phase finished.")

    async def user_handler(self):
        num_leeaved_user = 0
        self.clear_wrong_users()
        for user in self.users:
            num_leeaved_user += 1
            print("user(" + str(user['_id']) + ") started.")
            checker = self.check_user_exist(user['_id'])
            if checker == False:
                transactions = self.load_user_transactions(user["_id"])
                self.multi_process_compute_and_save_tv_tn(user["_id"], transactions)
            else:
                pass
                print(f"{num_leeaved_user}-Leave this user.")
            del checker

    def load_user_transactions(self, source_account):
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

    def multi_process_compute_and_save_tv_tn(self, source_account, transactions):
        manager = Manager()
        que_ = Queue()
        producer = Process(target=self.task_producer, args=(source_account,
                                                            self.opening_time,
                                                            self.closing_time,
                                                            que_,
                                                            transactions,
                                                            self.num_consumers))

        producer.start()
        consumers = []
        tv_nt_queue = Queue()
        for k in range(self.num_consumers):
            consumers.append(
                Process(target=self.task_consumer, args=(k, que_, tv_nt_queue, self.db_name, self.working_collection))
            )
            consumers[k].start()
        que_.close()
        que_.join_thread()
        producer.join()
        for k in range(self.num_consumers):
            consumers[k].join()
        for k in range(self.num_consumers):
            consumers[k].terminate()

    def task_producer(self, source_account, current_time, end_time, queue, transactions, num_workers):

        while current_time <= end_time:
            end_of_time_window = current_time + timedelta(seconds=900)
            tw_transactions = self.get_time_window_transactions(current_time, end_of_time_window, transactions)
            queue.put({"transactions": tw_transactions,
                       "source_account": source_account,
                       "current_time": current_time,
                       "end_time": end_of_time_window})
            current_time = end_of_time_window

        for i in range(num_workers):
            queue.put({
                "transactions": self.SENTINEL
            })
        print(f"{source_account} time windows finished.")

    def get_time_window_transactions(self, start_time, end_time, transactions):
        tw_transactions = []
        for transaction in transactions:
            tmp = datetime.strptime(transaction["created_at"], "%Y-%m-%dT%H:%M:%S.%fZ")
            if start_time <= tmp <= end_time:
                tw_transactions.append(transaction)
        return tw_transactions


    def task_consumer(self, worker_num, queue, tv_nt_queue, db_name, collection):
        mongo_client = MongoClient()
        db = mongo_client[db_name]
        working_collection = db[collection]
        while True:
            obj = queue.get()
            if obj['transactions'] != self.SENTINEL:
                TV_and_NofT = self.compute_trading_volume_number_of_trades(obj["transactions"])
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
                self.save_tv_and_nt_per_user_in_15_minuets(obj_result, working_collection)
                # tv_nt_queue.put(obj_result)
            elif obj['transactions'] == self.SENTINEL:
                mongo_client.close()
                # tv_nt_queue.put({"source_account": self.SENTINEL})
                print(f"Worker {worker_num} finished.")
                break

    def compute_trading_volume_number_of_trades(self, transactions):
        trading_volume = 0.0
        number_of_trades = 0
        for transaction in transactions:
            if ("selling_asset_type" in transaction.keys()) and transaction["selling_asset_type"] == self.active_asset:
                trading_volume += float(transaction["amount"])
            elif ("selling_asset_code"  in transaction.keys()) and transaction["selling_asset_code"] == self.active_asset.upper():
                trading_volume += float(transaction["amount"])
            elif ("buying_asset_type" in transaction.keys()) and transaction["buying_asset_type"] == self.active_asset:
                trading_volume += float(float(transaction["amount"]) * float(transaction['price']))
            elif ("buying_asset_code" in transaction.keys()) and transaction["buying_asset_code"] == self.active_asset.upper():
                trading_volume += float(float(transaction["amount"]) * float(transaction['price']))
            number_of_trades += 1


        return {"trading_volume": trading_volume, "number_of_trades": number_of_trades}

    def save_tv_and_nt_per_user_in_15_minuets(self, obj, db_client):
        db_client.insert(obj)
        # self.working_collection.insert(obj)
