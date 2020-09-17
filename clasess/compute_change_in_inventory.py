from datetime import datetime
from datetime import timedelta


class ComputeChangeInInventory:
    transactions_per_user = None
    number_of_short_positions = 0
    number_of_long_positions = 0
    long_positions_amount = 0.0
    short_positions_amount = 0.0

    def __init__(self, transactions, db, active_asset):
        self.transactions_coll = transactions
        self.change_in_inventory = db['ETH_change_in_inventory_per_15_minuts']
        self.active_asset = active_asset

    def get_users(self):
        query = [
            {
                "$sort": {"created_at": 1}
            },
            {"$group": {
                "_id": "$source_account"
            }}
        ]
        result = self.transactions_coll.aggregate(pipeline=query, allowDiskUse=True)
        for user in result:
            self.get_user_transactions(user)
            self.compute_change_in_inventory()

    def get_user_transactions(self, user):
        query = [
            {
                "$sort": {"created_at": 1}
            },
            {"$match": {
                "source_account": user['_id']
            }}
        ]
        self.transactions_per_user = self.transactions_coll.aggregate(pipeline=query, allowDiskUse=True)


    def compute_change_in_inventory(self):
        current_time = None
        source_account = ""
        T_not_saved = False
        for transaction in self.transactions_per_user:
            if current_time != None:
                tmp = datetime.strptime(transaction["created_at"], "%Y-%m-%dT%H:%M:%S.%fZ")
                distance = current_time - tmp
                if distance.seconds <= 900:
                    self.long_or_short_position(transaction, adding=True)
                    T_not_saved = True
                else:
                    change_in_inventory = self.long_positions_amount - self.short_positions_amount
                    obj = self.get_object(source_account, current_time, change_in_inventory)
                    self.save_change_in_inventory(obj)
                    obj = None
                    self.long_or_short_position(transaction, adding=False)
                    current_time = tmp
                    source_account = transaction["source_account"]
                    T_not_saved = False
            else:
                self.long_or_short_position(transaction, adding=False)
                source_account = transaction["source_account"]
                current_time = datetime.strptime(transaction["created_at"], "%Y-%m-%dT%H:%M:%S.%fZ")
        if T_not_saved:
            change_in_inventory = self.long_positions_amount - self.short_positions_amount
            obj = self.get_object(source_account, current_time, change_in_inventory)
            self.save_change_in_inventory(obj)
            T_not_saved = False

    def long_or_short_position(self, transaction, adding=False):
        if not adding:
            self.short_positions_amount = 0.0
            self.number_of_short_positions = 0
            self.long_positions_amount = 0.0
            self.number_of_long_positions = 0
        if (hasattr(transaction, "selling_asset_type") and
            transaction["selling_asset_type"] == self.active_asset) or \
                (hasattr(transaction, "selling_asset_code") and
                 transaction["selling_asset_code"] == self.active_asset):
            self.short_positions_amount += float(transaction["amount"])
            self.number_of_short_positions += 1
        elif (hasattr(transaction, "buying_asset_type") and
              transaction["buying_asset_type"] == self.active_asset) or \
                (hasattr(transaction, "buying_asset_code") and
                 transaction["buying_asset_code"] == self.active_asset):
            self.long_positions_amount += float(transaction["amount"])
            self.number_of_long_positions += 1

    def get_object(self, source_account, start_t_w, change_in_inventory):
        obj = {
            "source_account": source_account,
            "asset": self.active_asset,
            "start_time_window": start_t_w,
            "change_in_inventory": change_in_inventory,
            "number_of_selling": self.number_of_short_positions,
            "selling_amount": self.short_positions_amount,
            "number_of_buying": self.number_of_long_positions,
            "buying_amount": self.long_positions_amount
        }
        return obj

    def save_change_in_inventory(self, time_window_object):
        self.change_in_inventory.insert(time_window_object)
        print("One Record Added");
