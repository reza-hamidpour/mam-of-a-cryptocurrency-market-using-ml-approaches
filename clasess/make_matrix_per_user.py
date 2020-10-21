from datetime import datetime
from datetime import timedelta
import pandas as pd
import asyncio

class MatrixPerUser:

    def __init__(self, db, collections, source_account, assets, opening_time, closing_time):
        self.db = db
        self.collections = collections
        self.source_account = source_account
        self.assets = assets
        self.opening_time = opening_time
        self.closing_time = closing_time


    def time_window_handler(self):
        current_time = self.opening_time
        df = pd.DataFrame(columns=["unixtime",
                                   "NT",
                                   "TV",
                                   "CII",
                                   "CNI",
                                   "asset_code"])
        asset_code = 1
        while current_time <= self.closing_time:
            unixtime = current_time.timestamp()
            for asset in self.assets:
                NT_TV = self.load_NT_TV_user(asset, current_time)
                CII = self.load_CII_user(asset, current_time)
                CNI = self.load_CNI_user(asset, current_time)
                if asset == "btc":
                    asset_code = 2
                elif asset == "eth":
                    asset_code = 3
                df.append({
                    "unixtime": unixtime,
                    "NT": NT_TV["nt"],
                    "TV": NT_TV["tv"],
                    "CII": CII,
                    "CNI": CNI,
                    "asset_code": asset_code
                })
            current_time = current_time + timedelta(seconds=900)
        df.to_csv(str(self.source_account) + ".csv")


    def load_NT_TV_user(self, asset, tw):
        query = {
            "source_account": self.source_account,
            "time_window": tw
        }
        transaction = list(self.db[str(asset) + "_user_working_capital_selling_per_15_minuets"].findOne(query))
        result = {"nt": 0.0,
                  "tv": 0.0}
        if len(transaction) > 0:
            result["nt"] = transaction[0]["number_of_trades"]
            result["tv"] = transaction[0]["trading_volume"]
        return {"nt": transaction}

    def load_CII_user(self, asset, tw):
        query = {
            "source_account": self.source_account,
            "time_window": tw
        }
        transaction = list(self.db[str(asset) + "_change_in_inventory_per_15_minuets"].findOne(query))
        change_in_inventory = 0.0
        if len(transaction) > 0:
            change_in_inventory = transaction[0]["change_in_inventory"]
        return change_in_inventory

    def load_CNI_user(self, asset, tw):
        query = {
            "source_account": self.source_account,
            "end_of_time_period": tw
        }
        transaction = list(self.db[str(asset) + "_cumulative_net_inventory_per_15_minuets"].findOne(query))
        cumulative_net_inventory = 0.0
        if len(transaction) > 0:
            cumulative_net_inventory = transaction[0]["cumulative_net_inventory"]
        return cumulative_net_inventory
