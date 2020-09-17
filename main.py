from clasess.database import Database
from clasess.exchange_rate_per_assets_in_each_day import ExchangeRatePerAssetsEachDay
from clasess.save_processes import SaveProcesses
from clasess.user_amount_in_each_day import UserAmountInEachDay
from clasess.inter_trade_duration import InterTradeDuration
from clasess.compute_change_in_inventory import ComputeChangeInInventory
from clasess.compute_cumulative_net_inventory import CumulativeNetInventory


def get_asset_object(selling_part, buying_part):
    if selling_part["selling_asset_type"] == "native":
        asset = {"selling_asset_type": selling_part["selling_asset_type"],
                 "buying_asset_type": buying_part["buying_asset_type"],
                 "buying_asset_code": buying_part["buying_asset_code"],
                 "buying_asset_issuer": buying_part["buying_asset_issuer"]}
    elif buying_part["buying_asset_type"] == "native":
        asset = {"selling_asset_type": selling_part["selling_asset_type"],
                 "selling_asset_code": selling_part["selling_asset_code"],
                 "selling_asset_issuer": selling_part["selling_asset_issuer"],
                 "buying_asset_type": buying_part["buying_asset_type"]}
    else:
        asset = {"selling_asset_type": selling_part["selling_asset_type"],
                 "selling_asset_code": selling_part["selling_asset_code"],
                 "selling_asset_issuer": selling_part["selling_asset_issuer"],
                 "buying_asset_type": buying_part["buying_asset_type"],
                 "buying_asset_code": buying_part["buying_asset_code"],
                 "buying_asset_issuer": buying_part["buying_asset_issuer"]}
    return asset


def get_amount_and_trade_number():
    mydb = Database()
    bucket = mydb.select_collection("btc_bucket")
    u_wc_s_per_each_day = "BTC_user_working_capital_selling_per_15_minuets"
    user_a_and_n_of_trade_per_day = UserAmountInEachDay(mydb.get_db(), bucket, u_wc_s_per_each_day, None, None)
    user_a_and_n_of_trade_per_day.get_users_tranasctions("BTC")
    user_a_and_n_of_trade_per_day.get_user_transactions_base_s()


def exchange():
    mydb = Database()
    E_B_L_bucket = mydb.select_collection("E_B_L_bucket")
    ledger = mydb.select_collection("ledgers")
    save_er = SaveProcesses(mydb.get_db(), "exchange_rate")
    exchange_object = ExchangeRatePerAssetsEachDay(mydb.get_db(), E_B_L_bucket, ledger, save_er)
    print("Gathering all transactions.")
    assets = exchange_object.get_assets()
    print("all assets are ready.")
    for s_asset in assets:
        for b_asset in s_asset["assets"]:
            if s_asset["selling_asset_type"] == "native":
                asset = {"buying_asset_type": b_asset["buying_asset_type"],
                         "buying_asset_code": b_asset["buying_asset_code"],
                         "buying_asset_issuer": b_asset["buying_asset_issuer"],
                         "selling_asset_type": s_asset["selling_asset_type"]}
            elif b_asset["buying_asset_type"] == "native":
                asset = {"buying_asset_type": b_asset["buying_asset_type"],
                         "selling_asset_type": s_asset["selling_asset_type"],
                         "selling_asset_code": s_asset["selling_asset_code"],
                         "selling_asset_issuer": s_asset["selling_asset_issuer"]}
            else:
                asset = {"buying_asset_type": b_asset["buying_asset_type"],
                         "buying_asset_code": b_asset["buying_asset_code"],
                         "buying_asset_issuer": b_asset["buying_asset_issuer"],
                         "selling_asset_type": s_asset["selling_asset_type"],
                         "selling_asset_code": s_asset["selling_asset_code"],
                         "selling_asset_issuer": s_asset["selling_asset_issuer"]}
            exchange_rate = exchange_object.get_exchange_rate(asset)
    # save_er.save(er_object)


def inter_trade_duration():
    database = Database()
    E_B_bucket = database.select_collection("eth_bucket")
    inter_trade = InterTradeDuration(database.get_db(), E_B_bucket)
    inter_trade.get_users()
    inter_trade.get_users_transaction()


def compute_change_in_inventory():
    database = Database()
    bucket = database.select_collection("eth_bucket")
    change_in_inventory_bucket = database.select_collection("ETH_change_in_inventory_per_15_minuts")
    c_in_i = ComputeChangeInInventory(bucket, database.get_db(), "ETH")
    c_in_i.get_users()


def compute_comulative_net_inventory():
    max = "2019-12-15T14:26:38Z"
    min = "2019-10-09T15:30:38Z"
    database = Database()
    operations = database.select_collection("native_change_in_inventory_per_15_minuts")
    cni_collection_name = "native_cumulative_change_in_inventory"
    cni_obj = CumulativeNetInventory(operations, database.get_db(), cni_collection_name, min, max)
    cni_obj.get_users()
    cni_obj.handel_users()

compute_comulative_net_inventory()
# compute_change_in_inventory()
# inter_trade_duration()
# get_amount_and_trade_number() # finish