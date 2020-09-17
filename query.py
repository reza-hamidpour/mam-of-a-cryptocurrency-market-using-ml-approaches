
# We could not use this query becase of memory limits.
query = [{"$group":
            {"_id": {

              "selling_asset_type": "$selling_asset_type",
              "selling_asset_code": "$selling_asset_code",
              "selling_asset_issuer": "$selling_asset_issuer",
              "buying_asset_type": "$buying_asset_type",
              "buying_asset_code": "$buying_asset_code",
              "buying_asset_issuer": "$buying_asset_issuer"
                    },
          "attributes": {
            "$push": {
              "amount": "$amount",
              "price": "$price",
              "created_at": "$created_at"
            }
          }
          }
        },
        {
          "$sort": {
            "created_at": 1
          }
        },
        {"$out": "users_transactions_per_asset"}]

# We use this query instead of top query.
u_working_capital_sb_per_each_day = [{"$group":
               {"_id": {
                        "source_account": "$source_account",
                        "selling_asset_type": "$selling_asset_type",
                        "selling_asset_code": "$selling_asset_code",
                        "selling_asset_issuer": "$selling_asset_issuer",
                        "buying_asset_type": "$buying_asset_type",
                        "buying_asset_code": "$buying_asset_code",
                        "buying_asset_issuer": "$buying_asset_issuer"
                        }
                }}, {
    "$out":
        "u_transactions_base_sb"
    }]

u_working_capital_s_per_each_day = [
    {"$group":
               {"_id": {
                        "source_account": "$source_account",
                        "selling_asset_type": "$selling_asset_type",
                        "selling_asset_code": "$selling_asset_code",
                        "selling_asset_issuer": "$selling_asset_issuer",
                        }
                }}, {
    "$out":
        "u_transactions_base_s"
    }]

u_working_capital_b_per_each_day = [
    {"$group":
               {"_id": {
                        "source_account": "$source_account",
                        "buying_asset_type": "$buying_asset_type",
                        "buying_asset_code": "$buying_asset_code",
                        "buying_asset_issuer": "$buying_asset_issuer",
                        }
                }}, {
    "$out":
        "u_transactions_base_b"
    }]


query_day_transaction = [{
                    "$match": {
                        "source_account": "GAOVLZQ6YVJMIE46EAA2ZAYUU6S3ETKHDDFASFINH4YLWUZKXAUBYBB2",
                        "selling_asset_type": "credit_alphanum4",
                        "selling_asset_code": "BCNY",
                        "selling_asset_issuer": "GBCNYBHAAPDSU3UIHXXQTHYZVSBJBI4YUNWXMKJBCPDHTVYR75G6NFHD",
                        "buying_asset_type": "native"
                    }
                },
                    {"$sort": {"created_at": 1}}]

compute_bucket = [
    {"$match": {
        "$or": [{"selling_asset_code": "BTC"}, {"buying_asset_code": "BTC"}],
        "offer_id": "0"
    }},
    {"$out": "btc_bucket"}
]

compute_bucket = [
    {"$match": {
        "$or": [{"selling_asset_code": "ETH"}, {"buying_asset_code": "ETH"}],
        "offer_id": "0"
    }},
    {"$out": "eth_bucket"}
]

compute_bucket = [
    {"$match": {
        "$or": [{"selling_asset_type": "native"}, {"buying_asset_type": "native"}],
        "offer_id": "0"
    }},
    {"$out": "native_bucket"}
]