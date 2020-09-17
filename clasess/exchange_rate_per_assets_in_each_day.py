from bson.son import SON
from datetime import datetime
from datetime import timedelta
import pymongo


class ExchangeRatePerAssetsEachDay:
    assets = None
    c_asset = None
    sorted_transactions = None
    st_current_asset = None
    et_current_asset = None
    today_transactions = None
    amount_and_total_num = None
    insert_bucket = []

    def __init__(self, database, operations, ledgers, save_object):
        self.db = database
        self.operations = operations
        self.ledgers = ledgers
        self.save_object = save_object

    def get_exchange_rate(self, asset):
        self.set_current_asset(asset)
        print("get Sorted Transaction.")
        self.get_sorted_transactions()
        print("Transaction sorted has finished")
        last_date = None
        for current_item in self.sorted_transactions:
            response = self.set_start_and_end_time(current_item["created_at"], last_date)
            if response:
                self.get_today_transactions_current_asset()
                first_last = self.get_today_first_last_transactions()
                max_min = self.get_today_max_min_transactions()
                count_amount = self.transaction_count_and_amount()
                exchange_result = {"selling_asset_type": current_item["selling_asset_type"],
                                   "selling_asset_code": current_item["selling_asset_code"],
                                   "selling_asset_issuer": current_item["selling_asset_issuer"],
                                   "buying_asset_type": current_item["buying_asset_type"],
                                   "buying_asset_code": current_item["buying_asset_code"],
                                   "buying_asset_issuer": current_item["buying_asset_issuer"],
                                   "exchange": {"first": first_last["first"],
                                                "last": first_last["last"],
                                                "max": max_min["max_transaction"],
                                                "min": max_min["min_transaction"]},
                                   "start_at": self.st_current_asset,
                                   "end_at": self.et_current_asset,
                                   "total_transaction_count": count_amount["count"],
                                   "total_transaction_amount": count_amount["amount"]}
                self.insert_bucket.append(exchange_result)
                if len(self.insert_bucket) == 10:
                    self.save_object.save(self.insert_bucket)
                    self.insert_bucket = []
                    print(len(self.insert_bucket), " asset exchange rate have inserted successfully.")
                else:
                    self.save_object.save(self.insert_bucket)

                last_date = self.et_current_asset
                # Clear all variables
                exchange_result = None
                self.today_transactions = None
                self.st_current_asset = None
                self.et_current_asset = None

    def get_assets(self):
        get_assets_q = [{"$group": {"_id": {"selling_asset_type": "$selling_asset_type",
                                            "selling_asset_code": "$selling_asset_code",
                                            "selling_asset_issuer": "$selling_asset_issuer"},
                                    "assets": {"$push": {"buying_asset_type": "$buying_asset_type",
                                                         "buying_asset_code": "$buying_asset_code",
                                                         "buying_asset_issuer": "$buying_asset_issuer"}}}}]
        current_object = self.operations.aggregate(pipeline=get_assets_q, allowDiskUse=True)
        return current_object

    def set_current_asset(self, asset):
        self.c_asset = asset

    def get_sorted_transactions(self):
        if self.c_asset["selling_asset_type"] == "native":
            query = [{"$match": {"selling_asset_type": self.c_asset["selling_asset_type"],
                                 "buying_asset_type": self.c_asset["buying_asset_type"],
                                 "buying_asset_code": self.c_asset["buying_asset_code"],
                                 "buying_asset_issuer": self.c_asset["buying_asset_issuer"]}},
                     {"$sort": {"created_at": 1}}]
        elif self.c_asset["buying_asset_type"] == "native":
            query = [{"$match": {"selling_asset_type": self.c_asset["selling_asset_type"],
                                 "selling_asset_code": self.c_asset["selling_asset_code"],
                                 "selling_asset_issuer": self.c_asset["selling_asset_issuer"],
                                 "buying_asset_type": self.c_asset["buying_asset_type"]}},
                     {"$sort": {"created_at": 1}}]
        else:
            query = [{"$match": {"selling_asset_type": self.c_asset["selling_asset_type"],
                                 "selling_asset_code": self.c_asset["selling_asset_code"],
                                 "selling_asset_issuer": self.c_asset["selling_asset_issuer"],
                                 "buying_asset_type": self.c_asset["buying_asset_type"],
                                 "buying_asset_code": self.c_asset["buying_asset_code"],
                                 "buying_asset_issuer": self.c_asset["buying_asset_issuer"]}},
                     {"$sort": {"created_at": 1}}]
        self.sorted_transactions = self.operations.aggregate(query, allowDiskUse=True)

    def set_start_and_end_time(self, current_date, last_date):
        transaction_date = datetime.strptime(current_date, "%Y-%m-%dT%H:%M:%S.%fZ")
        self.st_current_asset = datetime.strftime(transaction_date,  "%Y-%m-%dT00:00:00.%fZ")
        self.et_current_asset = datetime.strftime(transaction_date,  "%Y-%m-%dT23:59:59.%fZ")
        if last_date is None:
            return True
        else:
            difference = last_date - self.st_current_asset
            if difference.days >= 1:
                return True
            else:
                return False

    def get_today_transactions_current_asset(self):
        if self.c_asset["selling_asset_type"] == "native":
            query = [{"$match": {"selling_asset_type": self.c_asset["selling_asset_type"],
                                 "buying_asset_type": self.c_asset["buying_asset_type"],
                                 "buying_asset_code": self.c_asset["buying_asset_code"],
                                 "buying_asset_issuer": self.c_asset["buying_asset_issuer"],
                                 "created_at": {"$gte": self.st_current_asset, "$lte": self.et_current_asset}}},
                     {"$sort": {"created_at": 1}}]
        elif self.c_asset["buying_asset_type"] == "native":
            query = [{"$match": {"selling_asset_type": self.c_asset["selling_asset_type"],
                                 "selling_asset_code": self.c_asset["selling_asset_code"],
                                 "selling_asset_issuer": self.c_asset["selling_asset_issuer"],
                                 "buying_asset_type": self.c_asset["buying_asset_type"],
                                 "created_at": {"$gte": self.st_current_asset, "$lte": self.et_current_asset}}},
                     {"$sort": {"created_at": 1}}]
        else:
            query = [{"$match": {"selling_asset_type": self.c_asset["selling_asset_type"],
                                 "selling_asset_code": self.c_asset["selling_asset_code"],
                                 "selling_asset_issuer": self.c_asset["selling_asset_issuer"],
                                 "buying_asset_type": self.c_asset["buying_asset_type"],
                                 "buying_asset_code": self.c_asset["buying_asset_code"],
                                 "buying_asset_issuer": self.c_asset["buying_asset_issuer"],
                                 "created_at": {"$gte": self.st_current_asset, "$lte": self.et_current_asset}}},
                     {"$sort": {"created_at": 1}}]
        print("Today Transaction query :")
        print(query)
        self.today_transactions = list(self.operations.aggregate(query, allowDiskUse=True))

    def get_today_first_last_transactions(self):
        first_transaction = self.today_transactions[0]
        last_transaction = None
        i = 0
        for td_transaction in self.today_transactions:
            last_transaction = td_transaction
            i = i + 1
        print("Number of iteration: ", i)
        print(first_transaction)
        print(last_transaction)
        return {"first": first_transaction, "last": last_transaction}

    def get_today_max_min_transactions(self):
        max_price = 0.0
        max_id = ""
        min_price = 0.0
        min_id = ""
        for transaction in self.today_transactions:
            if transaction.price >= max_price:
                max_price = transaction["price"]
                max_id = str(transaction["_id"])
            elif min_price >= transaction.price:
                min_price = transaction["price"]
                min_id = transaction["_id"]
        return {"max_transaction": {"price": max_price,
                                    "id": max_id},
                "min_transaction": {"price": min_price, "id": min_id}}

    def transaction_count_and_amount(self):
        if self.c_asset["selling_asset_type"] == "native":
            query = [{'$match': {"selling_asset_type": self.c_asset["selling_asset_type"],
                                 "buying_asset_type": self.c_asset["buying_asset_type"],
                                 "buying_asset_code": self.c_asset["buying_asset_code"],
                                 "buying_asset_issuer": self.c_asset["buying_asset_issuer"],
                                 "created_at": {"$gte": self.st_current_asset,
                                                "$lte": self.et_current_asset}}},
                     {"$group": {"_id": {"selling_asset_type": self.c_asset["selling_asset_type"],
                                         "buying_asset_type": self.c_asset["buying_asset_type"],
                                         "buying_asset_code": self.c_asset["buying_asset_code"],
                                         "buying_asset_issuer": self.c_asset["buying_asset_issuer"]},
                                 "amount": {"$sum": "price"},
                                 "total_number": {"$sum": 1}}}]
        elif self.c_asset["buying_asset_type"] == "native":
            query = [{'$match': {"selling_asset_type": self.c_asset["selling_asset_type"],
                                 "selling_asset_code": self.c_asset["selling_asset_code"],
                                 "selling_asset_issuer": self.c_asset["selling_asset_issuer"],
                                 "buying_asset_type": self.c_asset["buying_asset_type"],
                                 "created_at": {"$gte": self.st_current_asset,
                                                "$lte": self.et_current_asset}}},
                     {"$group": {"_id": {"selling_asset_type": self.c_asset["selling_asset_type"],
                                         "selling_asset_code": self.c_asset["selling_asset_code"],
                                         "selling_asset_issuer": self.c_asset["selling_asset_issuer"],
                                         "buying_asset_type": self.c_asset["buying_asset_type"]},
                                 "amount": {"$sum": "price"},
                                 "total_number": {"$sum": 1}}}]
        else:
            query = [{'$match': {"selling_asset_type": self.c_asset["selling_asset_type"],
                                 "selling_asset_code": self.c_asset["selling_asset_code"],
                                 "selling_asset_issuer": self.c_asset["selling_asset_issuer"],
                                 "buying_asset_type": self.c_asset["buying_asset_type"],
                                 "buying_asset_code": self.c_asset["buying_asset_code"],
                                 "buying_asset_issuer": self.c_asset["buying_asset_issuer"],
                                 "created_at": {"$gte": self.st_current_asset,
                                                "$lte": self.et_current_asset}}},
                     {"$group": {"_id": {"selling_asset_type": self.c_asset["selling_asset_type"],
                                         "buying_asset_type": self.c_asset["buying_asset_type"],
                                         "buying_asset_code": self.c_asset["buying_asset_code"],
                                         "buying_asset_issuer": self.c_asset["buying_asset_issuer"]},
                                 "amount": {"$sum": "price"},
                                 "total_number": {"$sum": 1}}}]
        amount_and_total_num = list(self.operations.aggregate(query, allowDiskUse=True))
        return {"amount": amount_and_total_num[0]["amount"], "count": amount_and_total_num[0]["total_number"]}
