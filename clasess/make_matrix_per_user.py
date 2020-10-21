from datetime import datetime
from datetime import timedelta
import pandas as pd
import asyncio
import math

class MatrixPerUser:
    users           = None
    source_account  = None
    queue_size      = 10

    def __init__(self, db, operations, collections, assets, opening_time, closing_time):
        self.db                     = db
        self.collections            = collections
        self.operations             = operations
        self.assets                 = assets
        self.opening_time           = opening_time
        self.closing_time           = closing_time
        difference_between_o_and_c  = (self.closing_time - self.opening_time).total_seconds()
        self.number_of_tasks        = int(math.ceil(difference_between_o_and_c / 900.0))

    def query_on_users(self):
        query = [
            {"$group": {
                "_id": "$source_account"
            }
            },
            {"$sort": {"_id": 1}}
        ]
        self.users = self.operations.aggregate(pipeline=query, allowDiskUse=True)

    async def handler_users(self):
        for user in self.users:
          self.source_account = user["_id"]
          await self.time_window_handler()

    async def time_window_handler(self):
        loop = asyncio.get_event_loop()
        queue = asyncio.Queue(100)
        tasks = [loop.create_task(self.map_time_window_into_csv_format(queue)) for _ in range(self.number_of_tasks)]
        current_time = self.opening_time
        df = pd.DataFrame(columns=["unixtime",
                                   "NT",
                                   "TV",
                                   "CII",
                                   "CNI",
                                   "asset_code"])
        while current_time <= self.closing_time:
            for asset in self.assets:
                if self.queue_size <= 10:
                    await queue.put({
                            "asset"      : asset,
                            "time_window": current_time,
                            "df"         : df
                        })
                    self.queue_size += 1
                else:
                    await queue.join()
                    self.queue_size = 0
            current_time = current_time + timedelta(seconds=900)
        await df.to_csv(str(self.source_account) + ".csv")

    async def map_time_window_into_csv_format(self, queue):
        obj =  await queue.get()
        unixtime = obj["time_window"].timestamp()
        asset_code = 1
      # if obj["asset"] == "native":
      #     asset_code = 1
        if obj["asset"] == "btc":
            asset_code = 2
        elif obj["asset"] == "eth":
            asset_code = 3
        NT_TV   = await self.load_NT_TV_user(obj["asset"], obj["time_window"])
        CII     = await self.load_CII_user(obj["asset"], obj["time_window"])
        CNI     = await self.load_CNI_user(obj["asset"], obj["time_window"])
        obj["df"].append({
                "unixtime"  : unixtime,
                "NT"        : NT_TV["nt"],
                "TV"        : NT_TV["tv"],
                "CII"       : CII,
                "CNI"       : CNI,
                "asset_code": asset_code
        })
        queue.task_done()

    async def load_NT_TV_user(self, asset, tw):
        query = {
            "source_account": self.source_account,
            "time_window"   : tw
        }
        transaction = list(await self.db[str(asset) + "_user_working_capital_selling_per_15_minuets"].findOne(query))
        result = {"nt": 0.0,
                  "tv": 0.0}
        if len(transaction) > 0:
            result["nt"] = transaction[0]["number_of_trades"]
            result["tv"] = transaction[0]["trading_volume"]
        return {"nt": transaction}

    async def load_CII_user(self, asset, tw):
        query = {
            "source_account": self.source_account,
            "time_window"   : tw
        }
        transaction = list(await self.db[str(asset) + "_change_in_inventory_per_15_minuets"].findOne(query))
        change_in_inventory = 0.0
        if len(transaction) > 0:
            change_in_inventory = transaction[0]["change_in_inventory"]
        return change_in_inventory

    async def load_CNI_user(self, asset, tw):
        query = {
            "source_account"    : self.source_account,
            "end_of_time_period": tw
        }
        transaction = list(await self.db[str(asset) + "_cumulative_net_inventory_per_15_minuets"].findOne(query))
        cumulative_net_inventory = 0.0
        if len(transaction) > 0:
            cumulative_net_inventory = transaction[0]["cumulative_net_inventory"]
        return cumulative_net_inventory
