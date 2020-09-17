import requests_async as requests
from datetime import datetime
import asyncio

class PrepareData:
    assets = None
    totalNumber = 0
    inserted = 0
    u_o_n = "u_operations"
    operations = []

    def __init__(self, db, valid_col):
        self.db = db
        self.collection = valid_col
        self.query = {"buying_asset_type": {"$ne": "native"},
                      "buying_asset_code": {"$exists": False},
                      "buying_asset_issuer": {"$exists": False}}
        self.u_operations = self.db[self.u_o_n]

    def get_assets(self):
        self.assets = self.collection.find(self.query)
        # self.totalNumber = self.assets.count()

    async def each_asset(self):
        # print("Number of transactions which are going to update, are ", str(self.totalNumber))
        current_tasks = []
        for asset in self.assets:
            current_tasks.append(
                asyncio.ensure_future(self.request_and_insert(asset["_id"]))
            )
            if len(current_tasks) >= 10:
                for task in current_tasks:
                    await task

    async def request_and_insert(self, asset_id):
        response = await requests.get("https://horizon.stellar.org/operations" + asset_id)
        print(response)

    async def insert_asset(self, operations):
        self.u_operations.insetmany(operations)

    async def map_data(self, operation, ledger_id):
        date = datetime(operation["created_at"])
        return {
            "_id":                      operation.id,
            "ledger_id":                ledger_id,
            "transaction_successful":   operation["transaction_successful"],
            "source_account":           operation["source_account"],
            "type":                     operation["type"],
            "type_i":                   operation["type_i"],
            "created_at":               date.isoformat(),
            "transaction_hash":         operation["transaction_hash"],
            "amount":                   float(operation["amount"]),
            "price":                    float(operation["price"]),
            "price_r":                  {"n":float(operation["price_r"]["n"]),"d":float(operation["price_r"]["d"])},
            "buying_asset_type":        operation["buying_asset_type"],
            "selling_asset_type":       operation["selling_asset_type"],
            "selling_asset_code":       operation["selling_asset_code"],
            "selling_asset_issuer":     operation["selling_asset_issuer"],
            "offer_id":                 operation["offer_id"]
        }