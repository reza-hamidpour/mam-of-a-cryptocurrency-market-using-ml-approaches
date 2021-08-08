from datetime import datetime
from datetime import timedelta
import math
from multiprocessing import Process, Queue, Manager
from pymongo import MongoClient


class ComputeCNT:
    transactions = None
    number_of_tasks = 0
    users = None
    queue_size = 11
    user_transactions = None
    num_consumers = 7
    db_name = "stellar"
    SENTINEL = "END"

    def __init__(self, operations, db, working_collection, opening_time, closing_time, active_asset):
        self.operations = operations
        self.query_working_collection = db[working_collection]
        self.working_collection = working_collection
        self.opening_time = datetime.strptime(opening_time, "%Y-%m-%dT%H:%M:%SZ")
        self.closing_time = datetime.strptime(closing_time, "%Y-%m-%dT%H:%M:%SZ")
        self.active_asset = active_asset

    def get_users(self):
        query = [
            {
                "$group": {
                    "_id": "$source_account"
                }
            }
        ]
        return list(self.operations.aggregate(pipeline=query, allowDiskUse=True))

    def query_wrong_users(self):
        query = [
            {"$group": {
                "_id": "$source_account", "total": {"$sum": 1}
            }
            },
            {"$match": {"total": {"$lt": 6428}}},
            {"$project": {"_id": "$_id"}}]
        return self.query_working_collection.aggregate(pipeline=query, allowDiskUse=True)

    def clear_wrong_users(self):
        print("Start cleaning working collection...")
        for user in self.query_wrong_users():
            self.query_working_collection.remove({"source_account": user["_id"]})
        print("Cleaning phase finished.")

    def check_user_exists(self, source_account):
        query = {
            "source_account": source_account
        }
        processed_users = self.query_working_collection.find_one(query)

        check = False
        if processed_users != None and len(list(processed_users)) > 0:
            check = True
        return check

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
        self.user_transactions = list(self.operations.aggregate(query))

    def multi_process_net_inventory(self):
        manager = Manager()
        que_ = Queue()
        self.clear_wrong_users()
        users = self.get_users()
        producer = Process(target=self.task_producer, args=(users,
                                                            que_,
                                                            self.num_consumers))
        producer.start()
        consumers = []
        for k in range(self.num_consumers):
            consumers.append(
                Process(target=self.task_consumer, args=(k, que_,
                                                         self.db_name, self.working_collection,
                                                         self.opening_time, self.closing_time))
            )
            consumers[k].start()
        que_.close()
        que_.join_thread()
        producer.join()
        for k in range(self.num_consumers):
            consumers[k].join()
        for k in range(self.num_consumers):
            consumers[k].terminate()

    def task_producer(self, users, queue, num_consumers):
        for user in users:
            check_user = self.check_user_exists(user["_id"])
            if check_user == False:
                transactions = self.get_user_change_in_inventory_records(user["_id"])
                queue.put({
                    "source_account": user["_id"],
                    "transactions": transactions
                })
        for k in range(num_consumers):
            queue.put({
                "transactions": self.SENTINEL
            })

    def task_consumer(self, worker_num, queue, db_name, collection, opening_time, closing_time):
        mongo_client = MongoClient()
        db = mongo_client[db_name]
        working_collection = db[collection]
        while True:
            data = queue.get()
            if data['transactions'] != self.SENTINEL:
                current_time = opening_time
                end_time_window = current_time + timedelta(seconds=900)
                last_cumulative = 0.0
                print(f"Worker {worker_num} start user {data['source_account']}")
                while current_time <= closing_time:
                    current_time = end_time_window
                    time_window_transactions = self.get_time_window_CNI(data['transactions'], end_time_window)
                    cumulative_net_inventory = self.get_comulative_change_inventory_for_period(time_window_transactions)
                    if cumulative_net_inventory > 0:
                        last_cumulative += cumulative_net_inventory
                    else:
                        last_cumulative += 0
                    obj = self.get_CNI_object(data["source_account"], last_cumulative, end_time_window)
                    self.save_CNI(obj, working_collection)
                    end_time_window = current_time + timedelta(seconds=900)
                print(f"worker {worker_num} finished user {data['source_account']}")
            elif data['transactions'] == self.SENTINEL:
                mongo_client.close()
                print(f"Worker {worker_num} finished.")
                break

    def get_user_change_in_inventory_records(self, source_account):
        query = [
            {"$match": {
                "source_account": source_account
            }
            },
            {"$sort": {"time_window": 1}},
        ]
        return list(self.operations.aggregate(pipeline=query, allowDiskUse=True))

    def get_time_window_CNI(self, transactions, end_of_tw):
        TW_transactions = []
        break_after_2 = 2
        for transaction in transactions:
            if transaction["time_window"] <= end_of_tw:
                TW_transactions.append({"change_in_inventory": transaction["change_in_inventory"]})
            else:
                break_after_2 -= 1
                if break_after_2 == 0:
                    break
        return TW_transactions

    def get_comulative_change_inventory_for_period(self, transactions):
        cumulative_net_inventory = 0.0
        for transaction in transactions:
            cumulative_net_inventory += float(transaction["change_in_inventory"])
        return cumulative_net_inventory

    def get_CNI_object(self, source_account, CNI, end_of_period):
        return {
            "source_account": source_account,
            "cumulative_net_inventory": CNI,
            "end_of_time_period": end_of_period
        }

    def save_CNI(self, current_CNI, working_collection_handler):
        working_collection_handler.insert(current_CNI)
