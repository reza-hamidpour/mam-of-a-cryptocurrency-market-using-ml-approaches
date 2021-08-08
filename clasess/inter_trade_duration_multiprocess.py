from datetime import datetime, timedelta
import math
from multiprocessing import Process, Queue, Manager
from pymongo import MongoClient

class InterTradeDuration:
    users = None
    num_consumers = 7
    SENTINEL = "END"
    db_name = "stellar"


    def __init__(self, db, operations, working_collection, asset, opening_time, closing_time):
        self.operations = operations
        self.working_collection = working_collection
        self.query_working_collection = db[working_collection]
        self.asset_name = asset
        self.opening_time = datetime.strptime(opening_time, "%Y-%m-%dT%H:%M:%SZ")
        self.closing_time = datetime.strptime(closing_time, "%Y-%m-%dT%H:%M:%SZ")

    def get_users(self):
        query = [{
            "$match": {"offer_id": "0"}
        }, {
            "$sort": {"created_at": 1}
        }, {
            "$group": {
                "_id": "$source_account"
            }
        }
        ]
        self.users = list(self.operations.aggregate(pipeline=query, allowDiskUse=True))

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
        return list(self.operations.aggregate(query))


    def check_user_exists(self, source_account):
        query = {
            "source_account": source_account
        }
        processed_users = self.query_working_collection.find_one(query)
        check = False
        if processed_users != None and len(list(processed_users)) > 0:
            check = True
        return check

    def multi_process_handler(self):
        manager = Manager()
        que_ = Queue()
        producer =  Process(target=self.task_producer, args=(self.users, self.num_consumers, que_))
        producer.start()

        consumers = []
        for i in range(self.num_consumers):
            consumers.append(
                Process(target=self.task_consumer, args=(
                    que_,
                    self.opening_time,
                    self.closing_time,
                    i,
                    self.db_name,
                    self.working_collection
                ))
            )
            consumers[i].start()
        que_.close()
        que_.join_thread()
        producer.join()
        for i in range(self.num_consumers):
            consumers[i].join()
        for i in range(self.num_consumers):
            consumers[i].terminate()

    def task_producer(self, users, num_worker, queue):
        for user in users:
            check = self.check_user_exists(user['_id'])
            if check == False:
                transactions = self.load_user_transactions(user['_id'])
                queue.put({
                    "source_account": user['_id'],
                    "transactions": transactions
                })
            else:
                print(f"Leave this user")
        for i in range(num_worker):
            queue.put({
                'transactions': self.SENTINEL
            })
        print("Producer Finished its job.")

    def task_consumer(self, queue, opening_time, closing_time,  worker_num, db_name, collection):
        mongo_client = MongoClient()
        db = mongo_client[db_name]
        working_collection = db[collection]
        while True:
            data = queue.get()
            if data['transactions'] != self.SENTINEL:
                print(f'Worker {worker_num} started user {data["source_account"]}')
                current_time = opening_time
                while current_time <= closing_time:
                    end_time_window = current_time + timedelta(seconds=900)
                    tw_transactions = self.load_time_window_transactions(data['transactions'], current_time, end_time_window)
                    inter_trade_duration = 900
                    if len(tw_transactions) > 0:
                        inter_trade_duration = self.calculate_inter_trade_duration(tw_transactions)
                    obj = {
                        'source_account': data['source_account'],
                        'start_time_window': current_time,
                        'end_time_window': end_time_window,
                        'inter_trade_duration': inter_trade_duration
                    }
                    self.save_inter_trade_duration(obj, working_collection)
                    current_time = end_time_window
                print(f'Worker {worker_num} finished user {data["source_account"]}')
            elif data['transactions'] == self.SENTINEL:
                print(f'Worker {worker_num} finished.')
                mongo_client.close()
                break

    def load_time_window_transactions(self, transactions, start_tw, end_tw):
        tw_transactions = []
        for transaction in transactions:
            tmp = datetime.strptime(transaction["created_at"], "%Y-%m-%dT%H:%M:%S.%fZ")
            if start_tw <= tmp <= end_tw:
                tw_transactions.append(tmp)
        return tw_transactions

    def calculate_inter_trade_duration(self, transactions):
        distances = []
        l_time = None
        for tw_position in transactions:
            if l_time != None:
                distance = l_time - tw_position
                distances.append(distance.seconds)
            else:
                l_time = tw_position
        number_of_distances = len(distances)
        if number_of_distances == 0:
            return 900
        else:
            return sum(distances) // number_of_distances

    def save_inter_trade_duration(self, obj, working_collection):
        working_collection.insert(obj)