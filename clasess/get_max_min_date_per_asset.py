from bson.son import SON
from datetime import datetime
from datetime import timedelta


class MaxMinDatePerAssets:
    assets = None
    max_min_col = "mm_per_asset"

    def __init__(self, database, operations, ledgers):
        self.db = database
        self.operations_handler = operations
        self.ledger_handler = ledgers

    def set_assets(self):
        get_assets = [{"$group": {"_id": {"selling_asset_type": "$selling_asset_type",
                                          "selling_asset_code": "$selling_asset_code",
                                          "selling_asset_issuer": "$selling_asset_issuer"}}}]
        self.assets = self.operations_handler.aggregate(get_assets)

    def get_max_min_per_asset(self):
        insert_array = []
        bulk_number = 0
        for asset in self.assets:
            qr = [{"$match": {"selling_asset_type": asset.selling_asset_type,
                              "selling_asset_code": asset.selling_asset_code,
                              "selling_asset_issuer": asset.selling_asset_issuer}}]
            assets_data = self.operations_handler.aggregate(qr)
            max_min_date = self.get_max_min(assets_data)
            if max_min_date is not False:
                data = {"selling_asset_type": asset.selling_asset_type,
                        "selling_asset_code": asset.selling_asset_code,
                        "selling_asset_issuer": asset.selling_asset_issuer,
                        "max": {"transaction_id": max_min_date["max_date"]["_id"],
                                "created_at": max_min_date["max_date"]["created_at"]},
                        "min": {"transaction_id": max_min_date["min_date"]["_id"],
                                "created_at": max_min_date["min_date"]["created_at"]}}
                insert_array.append(data)
                data = None
                if len(insert_array) >= 100:
                    self.save_max_min_per_asset(data)
                    insert_array = []
                    bulk_number += 1
                    print(str(bulk_number), " have inserted yet.")

    def get_max_min(self, assets):
        if len(assets) > 0:
            max_date = datetime.strptime(assets[0]["created_at"], "%Y-%m-%dT%H:%M:%S.%fZ")
            min_date = datetime.strptime(assets[0]["created_at"], "%Y-%m-%dT%H:%M:%S.%fZ")
            tran_id_max = assets[0]["_id"]
            tran_id_min = assets[0]["_id"]
            for asset in assets:
                next_date = datetime.strptime(asset["created_at"], "%Y-%m-%dT%H:%M:%S.%fZ")
                difference = next_date - max_date
                if difference.microseconds > 0:
                    max_date = next_date
                    tran_id_max = asset["_id"]
                else:
                    difference = min_date - next_date
                    if difference.microseconds > 0:
                        min_date = next_date
                        tran_id_min = asset["_id"]
            return {"max_date": {"_id": tran_id_max, "created_at": max_date}, "min_date": {"_id": tran_id_min, "created_at": min_date}}
        else:
            return False

    def save_max_min_per_asset(self, data):
        insert_coll = self.db[self.max_min_col]
        insert_coll.insert_many(data)
