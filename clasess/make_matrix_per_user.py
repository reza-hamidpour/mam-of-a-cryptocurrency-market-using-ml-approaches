from datetime import datetime
from datetime import timedelta
import pandas as pd
import asyncio
import math

class MatrixPerUser:
    users           = None
    source_account  = None
    queue_size      = 2
    NT_TV_records   = {}
    CNI_records     = {}
    CII_records     = {}


    def __init__(self, collections, operations, assets, opening_time, closing_time):
        self.collections                         = collections
        self.operations                 = operations
        self.assets                     = assets
        for asset in self.assets:
            self.NT_TV_records[asset]   = None
            self.CNI_records[asset]     = None
            self.CII_records[asset]     = None
        self.opening_time               = datetime.strptime(opening_time, "%Y-%m-%dT%H:%M:%SZ")
        self.closing_time               = datetime.strptime(closing_time, "%Y-%m-%dT%H:%M:%SZ")
        difference_between_o_and_c      = (self.closing_time - self.opening_time).total_seconds()
        self.number_of_tasks            = int(math.ceil(difference_between_o_and_c / 900.0))

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
          print("Starting ", user["_id"], " ... .")
          await self.time_window_handler(user["_id"])
        print("Finish")

    async def time_window_handler(self, source_account):
        iterator = 0
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

        for asset in self.assets:
            while current_time <= self.closing_time:
                if iterator <= self.queue_size:
                    await queue.put({
                            "source_account": source_account,
                            "asset"      : asset,
                            "time_window": current_time,
                            "df"         : df
                        })
                    iterator += 1
                    print("Task pushed.")
                else:
                    print("waiting for tasks")
                    await queue.join()
                    iterator = 0
                current_time = current_time + timedelta(seconds=900)
        await df.to_csv(str(source_account) + ".csv")

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
        print("gathering records ...")
        NT_TV   = await self.load_NT_TV_user(obj["source_account"], obj["asset"], obj["time_window"])
        CII     = await self.load_CII_user(obj["source_account"], obj["asset"], obj["time_window"])
        CNI     = await self.load_CNI_user(obj["source_account"], obj["asset"], obj["time_window"])
        obj["df"].append({
                "unixtime"  : unixtime,
                "NT"        : await self.log( NT_TV["nt"] ),
                "TV"        : await self.log( NT_TV["tv"] ),
                "CII"       : await self.log( CII ),
                "CNI"       : await self.log( CNI ),
                "asset_code": asset_code
        })
        print("Task ", obj["asset"], " at ", obj["time_window"], " pushed.")
        queue.task_done()

    async def load_NT_TV_user(self, source_account, asset, tw):
        if self.NT_TV_records[asset] != None:
            result = await self.search_in_RAM(self.NT_TV_records,
                                              asset,
                                              tw,
                                              "time_window")
            if result != False:
                return {
                    "nt": result["number_of_trades"],
                    "tv": result["trading_volume"]
                }
        self.NT_TV_records[asset] = None
        next_tw = tw + timedelta(seconds=9000)
        query = [
            {"$match": {
                "source_account": source_account,
                "time_window": {
                    "$gte": datetime.strftime(tw, "%Y-%m-%dT%H:%M:%S.%fZ"),
                    "$lte": datetime.strftime(next_tw, "%Y-%m-%dT%H:%M:%S.%fZ")
                }
            }}
        ]

        self.NT_TV_records[asset] = await self.collections[asset]["uwc"].aggregate(pipeline=query)
        result = {"nt": 0.0,
                  "tv": 0.0}
        transaction = await list(self.NT_TV_records[asset])
        if transaction > 0:
            result["nt"] = transaction[0]["number_of_trades"]
            result["tv"] = transaction[0]["trading_volume"]
            transaction = None
        return result

    async def load_CII_user(self, source_account, asset, tw):
        if self.CII_records[asset] != None:
            change_in_inventory = await self.search_in_RAM(self.CII_records, asset,
                                                           tw,
                                                           "time_window",
                                                           "change_in_inventory")
            if change_in_inventory != False:
                return change_in_inventory
        self.CII_records[asset] = None
        next_tw = tw + timedelta(seconds=9000)
        query = [
            {"$match": {
            "source_account": source_account,
            "time_window"   : {
                "$gte" : tw,
                "$lte" : next_tw
            }
        }}
        ]
        self.CII_records[asset] = await self.collections[asset]["cii"].aggregate(pipeline=query)
        transaction = await list(self.CII_records[asset])
        change_in_inventory = 0.0
        if len(transaction) > 0:
            change_in_inventory = transaction[0]["change_in_inventory"]
        return change_in_inventory

    async def load_CNI_user(self, source_account, asset, tw):
        if self.CNI_records[asset] != None:
            cumulative_net_inventory = await self.search_in_RAM(self.CNI_records,
                                                                asset,
                                                                tw,
                                                                "end_of_time_period",
                                                                "cumulative_net_inventory")
            if cumulative_net_inventory != False :
                return cumulative_net_inventory
        self.CNI_records[asset] = None
        next_tw = tw + timedelta(seconds=9000) # get 10 records
        query = [
            {"$match": {
            "source_account"    : source_account,
            "end_of_time_period": {
                "$gte": tw,
                "$lte": next_tw
            }
        }}
        ]
        self.CNI_records[asset] = await self.collections[asset]["cni"].aggregate(pipeline=query)
        transaction = await list(self.CNI_records[asset])
        cumulative_net_inventory = 0.0
        if len(transaction) > 0:
            cumulative_net_inventory = transaction[0]["cumulative_net_inventory"]
        return cumulative_net_inventory

    async def search_in_RAM(self, transactions, asset, time_window, con_attr, get_attr="" ):
        for transaction in transactions[asset]:
            if transaction[con_attr] == time_window:
                if get_attr != "":
                    return transaction[get_attr]
                else:
                    return transaction
        return False

    async def log(self, x):
        if x < 0:
            return (-1) * math.log10((-1) * x) + 1
        elif x > 0:
            return math.log10(x) + 1
        else:
            return 0