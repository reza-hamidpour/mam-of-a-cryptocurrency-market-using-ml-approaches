from datetime import datetime


class ChangeInInventory:
    users_transactions = None

    def __init__(self, transactions, short_positions_coll, long_positions_coll, change_in_inventory_coll, native_asset):
        self.transactions_coll = transactions
        self.short_positions_coll = short_positions_coll
        self.long_positions_coll = long_positions_coll
        self.change_in_inventory = change_in_inventory_coll
        self.native_asset = native_asset

    def get_short_positions(self):
        if self.native_asset == "native":
            query = [{
                "$match": {
                    "selling_asset_type": self.native_asset,
                    "offer_id": "0"
                }
            },
                {
                    "$sort": {"created_at": 1}
                },
                {
                    "$group": {
                        "_id": "$source_account",
                        "details": {
                            "$push": {
                                "created_at": "$created_at"
                            }
                        }
                    }}]
        else:
            query = [{
                "$match": {
                    "selling_asset_code": self.native_asset,
                    "offer_id": "0"
                }
            },
                {
                    "$sort": {"created_at": 1}
                },
                {
                    "$group": {
                        "_id": "$source_account",
                        "$push": {
                            "created_at": "$created_at"
                        }
                    }}]
        self.users_transactions = self.transactions_coll.aggregate(pipeline=query, allowDiskUse=True)

    def get_long_positions(self):
        if self.native_asset == "native":
            query = [{
                "$match": {
                    "buying_asset_type": self.native_asset,
                    "offer_id": "0"
                }
            },
                {
                    "$sort": {"created_at": 1}
                },
                {
                    "$group": {
                        "_id": "$source_account",
                        "details": {
                            "$push": {
                                "created_at": "$created_at"
                            }
                        }
                    }}]
        else:
            query = [{
                "$match": {
                    "buying_asset_code": self.native_asset,
                    "offer_id": "0"
                }
            },
                {
                    "$sort": {"created_at": 1}
                },
                {
                    "$group": {
                        "_id": "$source_account",
                        "$push": {
                            "created_at": "$created_at",
                            "amount": "$amount"
                        }
                    }}]
        self.users_transactions = self.transactions_coll.aggregate(pipeline=query, allowDiskUse=True)

    def get_positions_in_time_windows(self, long=False):
        current_time = None
        flag = False
        number_of_positions = 0
        position_amount = 0.0
        first_source_account = ""
        T_not_saved = False
        for users in self.users_transactions:
            for nx_transaction in users["created_at"]:
                if current_time != None:
                    tmp = datetime.strftime(nx_transaction["details"]["created_at"], "%Y-%m-%dT%H:%M:%S.%fZ")
                    distance = current_time - tmp
                    if distance.seconds <= 900:
                        number_of_positions += 1
                        position_amount += float(nx_transaction["details"]["amount"])
                        flag = False
                        T_not_saved = True
                    else:
                        obj = self.get_object(nx_transaction["_id"], current_time, number_of_positions, long)
                        self.save_positions(obj, long)
                        obj = None
                        number_of_positions = 1
                        position_amount = float(nx_transaction["details"]["amount"])
                        current_time = tmp
                        flag = True
                        T_not_saved = False
                else:
                    number_of_positions = 1
                    position_amount = float(nx_transaction["details"]["amount"])
                    first_source_account = nx_transaction["_id"]
                    current_time = datetime.strftime(nx_transaction["details"]["created_at"], "%Y-%m-%dT%H:%M:%S.%fZ")
                    flag = True
            if T_not_saved:
                obj = self.get_object(first_source_account, current_time, number_of_positions, position_amount, long)
                self.save_positions(obj, False)
                T_not_saved = False
            if flag:
                obj = self.get_object(first_source_account, current_time, number_of_positions, long)
                self.save_positions(obj, False)
                flag = False

    def get_object(self, source_account, start_t_w, number_of_positions, position_amount, long):
        if self.native_asset == "native" and long:
            obj = {"source_account": source_account,
                   "buying_asset_type": self.native_asset,
                   "start_time_window": datetime.strftime(start_t_w, "%Y-%m-%dT%H:%M:%S.%fZ"),
                   "number_of_positions": number_of_positions,
                   "amount": position_amount,
                   "long_position": True}
        elif self.native_asset != "native" and long:
            obj = {"source_account": source_account,
                   "buying_asset_code": self.native_asset,
                   "start_time_window": datetime.strftime(start_t_w, "%Y-%m-%dT%H:%M:%S.%fZ"),
                   "number_of_positions": number_of_positions,
                   "amount": position_amount,
                   "long_position": True}
        elif self.native_asset == "native" and not long:
            obj = {"source_account": source_account,
                   "selling_asset_type": self.native_asset,
                   "start_time_window": datetime.strftime(start_t_w, "%Y-%m-%dT%H:%M:%S.%fZ"),
                   "number_of_positions": number_of_positions,
                   "amount": position_amount,
                   "short_position": True}
        elif self.native_asset != "native" and not long:
            obj = {"source_account": source_account,
                   "selling_asset_code": self.native_asset,
                   "start_time_window": datetime.strftime(start_t_w, "%Y-%m-%dT%H:%M:%S.%fZ"),
                   "number_of_positions": number_of_positions,
                   "amount": position_amount,
                   "short_position": True}
        return obj

    def save_positions(self, obj, longPosition=True):
        if longPosition:
            self.long_positions_coll.insert(obj)
        else:
            self.short_positions_coll.insert(obj)
