
class AssetAmountPerUser:

    def __init__(self, operations):
        self.operations = operations

    def get_selling_amount_per_user(self):
        query = {"$group": {"_id": {"source_account": "$source_account",
                                    "selling_asset_type": "$selling_asset_type",
                                    "selling_asset_code": "$selling_asset_code",
                                    "selling_asset_issuer": "$selling_asset_issuer"},
                            "amount": {"$sum": {"$price_r.d"}},
                            "number": {"$sum": 1}}}