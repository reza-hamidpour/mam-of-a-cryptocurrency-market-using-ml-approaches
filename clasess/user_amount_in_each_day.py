from datetime import datetime


class UserAmountInEachDay:
    user_transaction_per_asset = None
    bulk_insert = []
    u_wc_sb_per_each_d = False
    u_wc_s_per_each_d = False
    u_wc_b_per_each_d = False
    user_groups = None
    def __init__(self, db, operations, user_transactions_base_sb,
                 user_transactions_base_s, user_transactions_base_b):
        self.db = db
        self.operations = operations
        self.coll_sb_transaction = db[user_transactions_base_sb]
        self.u_wc_sb_per_each_day = db[user_transactions_base_sb]
        # self.coll_s_transactions = user_transactions_base_s
        # self.coll_b_transactions = user_transactions_base_b
        # self.u_wc_s_per_each_day = db.u_working_capital_s_per_15_minuets
        # self.u_wc_b_per_each_day = db.u_working_capital_b_per_15_minuets

    def get_users(self):
        query = [{"$group": {
            "_id": "$source_account"
        }}]
        self.users = self.operations.aggregate(pipeline=query, allowDiskUse=True)

    def user_handler(self):
        for user in self.users:

    def get_amount_and_number_t_per_15_minuets(self, source_account):


    def get_users_tranasctions(self, asset):
        if asset != "native":
            query = [{
                "$group": {
                    "_id": {"selling_asset_code": asset,
                            "source_account": "$source_account"}
                }
            }]
        else:
            query = [{
                "$group": {
                    "_id": {
                        "selling_asset_type": asset,
                        "source_account": "$source_account"
                    }
                }
            }]
        self.user_groups = self.operations.aggregate(pipeline=query, allowDiskUse=True)

    def get_user_transactions_base_sb(self):
        self.u_wc_sb_per_each_d = True
        for user_asset in self.coll_sb_transaction.find():
            if user_asset["_id"]["selling_asset_type"] == "native":
                query_day_transaction = [{
                    "$match": {
                        "source_account": user_asset["_id"]["source_account"],
                        "selling_asset_type": user_asset["_id"]["selling_asset_type"],
                        "buying_asset_issuer": user_asset["_id"]["buying_asset_issuer"],
                        "buying_asset_code": user_asset["_id"]["buying_asset_code"],
                        "buying_asset_type": user_asset["_id"]["buying_asset_type"]
                    }
                },
                    {"$sort": {"created_at": 1}}]
            else:
                query_day_transaction = [{
                    "$match": {
                        "source_account": user_asset["_id"]["source_account"],
                        "selling_asset_type": user_asset["_id"]["selling_asset_type"],
                        "selling_asset_code": user_asset["_id"]["selling_asset_code"],
                        "selling_asset_issuer": user_asset["_id"]["selling_asset_issuer"],
                        "buying_asset_issuer": user_asset["_id"]["buying_asset_issuer"],
                        "buying_asset_code": user_asset["_id"]["buying_asset_code"],
                        "buying_asset_type": user_asset["_id"]["buying_asset_type"]
                    }
                },
                    {"$sort": {"created_at": 1}}]
            transactions = self.operations.aggregate(pipeline=query_day_transaction, allowDiskUse=True)
            self.amount_and_number_t_each_day(transactions)

    def get_user_transactions_base_s(self):
        self.u_wc_s_per_each_d = True
        for user_asset in self.user_groups:
            try:
                query_day_transaction = [{
                    "$match": {
                        "source_account": user_asset["_id"]["source_account"],
                        "selling_asset_code": user_asset["_id"]["selling_asset_code"]
                    }
                },
                    {"$sort": {"created_at": 1}}]
            except:
                query_day_transaction = [{
                    "$match": {
                        "source_account": user_asset["_id"]["source_account"],
                        "selling_asset_type": user_asset["_id"]["selling_asset_type"]
                    }
                },
                    {"$sort": {"created_at": 1}}]
            transactions = self.operations.aggregate(pipeline=query_day_transaction, allowDiskUse=True)
            self.amount_and_number_t_each_day(transactions)
        print("Finished.")

    def get_user_transactions_base_b(self):
        self.u_wc_b_per_each_d = True
        for user_asset in self.coll_b_transactions:
            if user_asset["_id"]["buying_asset_type"] == "native":
                query_day_transaction = [{
                    "$match": {
                        "source_account": user_asset["_id"]["source_account"],
                        "buying_asset_type": user_asset["_id"]["buying_asset_type"]
                    }
                },
                    {"$sort": {"created_at": 1}}]
            else:
                query_day_transaction = [{
                    "$match": {
                        "source_account": user_asset["_id"]["source_account"],
                        "buying_asset_type": user_asset["_id"]["buying_asset_type"],
                        "buying_asset_code": user_asset["_id"]["buying_asset_code"],
                        "buying_asset_issuer": user_asset["_id"]["buying_asset_issuer"]
                    }
                },
                    {"$sort": {"created_at": 1}}]
            transactions = self.operations.aggregate(pipeline=query_day_transaction, allowDiskUse=True)
            self.amount_and_number_t_each_day(transactions)

    def amount_and_number_t_each_day(self, transactions):
        current_date = None
        current_amount = 0
        next_date = None
        number = 0
        l_transaction = None
        active_asset = False
        inserted_number = 0
        time_window_started = None
        for transaction in transactions:
            if current_date != None:
                tmp = datetime.strptime(transaction["created_at"], "%Y-%m-%dT%H:%M:%S.%fZ")
                next_date = datetime(tmp.year, tmp.month, tmp.day)
                delta = next_date - current_date
                if delta.seconds <= 900:
                    current_amount = current_amount + transaction["amount"]
                    number = number + 1
                    active_asset = True
                else:
                    inserted_number += 1
                    today_object = self.get_today_obejct(l_transaction, current_amount, number, time_window_started)
                    self.save_today_transactions(today_object, inserted_number=inserted_number)
                    # Clear last day data.
                    current_date = next_date
                    current_amount = transaction["amount"]
                    number = 1
                    time_window_started = tmp
                    l_transaction = transaction
                    active_asset = False
            elif next_date == None and l_transaction == None:
                tmp = datetime.strptime(transaction["created_at"], "%Y-%m-%dT%H:%M:%S.%fZ")
                time_window_started = tmp
                current_date = datetime(tmp.year, tmp.month, tmp.day)
                current_amount = transaction["amount"]
                number = number + 1
                l_transaction = transaction
                active_asset = True
            # else:
        if active_asset:
            today_object = self.get_today_obejct(l_transaction, current_amount, number, time_window_started)
            self.save_today_transactions(today_object, inserted_number, True)

    def get_today_obejct(self, transaction, amount, number, t_date):
        if transaction["selling_asset_type"] == "native":
            today_object = {
                "source_account": transaction["source_account"],
                "selling_asset_type": transaction["selling_asset_type"],
                "total_amount": amount,
                "total_number": number,
                "time_window_start":  t_date
            }
        elif transaction["buying_asset_type"] == "native":
            today_object = {
                "source_account": transaction["source_account"],
                "selling_asset_type": transaction["selling_asset_type"],
                "selling_asset_code": transaction["selling_asset_code"],
                "selling_asset_issuer": transaction["selling_asset_issuer"],
                "total_amount": amount,
                "total_number": number,
                "transactions_date": t_date
            }
        else:
            today_object = {
                "source_account": transaction["source_account"],
                "selling_asset_type": transaction["selling_asset_type"],
                "selling_asset_code": transaction["selling_asset_code"],
                "selling_asset_issuer": transaction["selling_asset_issuer"],
                "total_amount": amount,
                "total_number": number,
                "transactions_date": t_date
            }
        return today_object

    def save_today_transactions(self, today_object, inserted_number, last_object=False):
        if len(self.bulk_insert) >= 10 and not last_object:
            self.bulk_insert.append(today_object)
            if self.u_wc_sb_per_each_d:
                return_id = self.u_wc_sb_per_each_day.insert_many(self.bulk_insert)
            elif self.u_wc_s_per_each_d:
                return_id = self.u_wc_sb_per_each_day.insert_many(self.bulk_insert)
            elif self.u_wc_b_per_each_d:
                return_id = self.u_wc_sb_per_each_day.insert_many(self.bulk_insert)
            print(inserted_number, " Have saved.")
            self.bulk_insert = []
        elif last_object:
            self.bulk_insert.append(today_object)
            if self.u_wc_sb_per_each_d:
                return_id = self.u_wc_sb_per_each_day.insert_many(self.bulk_insert)
            elif self.u_wc_s_per_each_d:
                return_id = self.u_wc_sb_per_each_day.insert_many(self.bulk_insert)
            elif self.u_wc_b_per_each_d:
                return_id = self.u_wc_sb_per_each_day.insert_many(self.bulk_insert)
            print(inserted_number, " Have saved.")
            self.bulk_insert = []
        else:
            self.bulk_insert.append(today_object)
