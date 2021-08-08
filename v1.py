import asyncio as asy
from clasess.database import Database
from clasess.cumpute_change_in_inventory_15_minuets_final import ComputeChangeInInventory
from clasess.user_amount_in_15_minuets_multiprocess import userAmountIn15Minuets
from clasess.compute_cumulative_net_inventory_multiprocess import ComputeCNT
from clasess.clean_result_collection import CleanCollection
from clasess.inter_trade_duration_multiprocess import InterTradeDuration

async def compute_user_working_capital(asset, opening_time, closing_time):
    db_handler = Database()
    working_collection = asset + "_user_working_capital_selling_per_15_minuets"
    working_db = db_handler.select_another_db("stellar")
    operations = db_handler.select_collection(asset + "_bucket")
    UWC = userAmountIn15Minuets(operations, working_db, working_collection, asset, opening_time, closing_time)
    UWC.get_users()
    await UWC.user_handler()


async def compute_change_in_inventory(asset, opening_time, closing_time):
    db_handler = Database()
    working_collection = asset + "_change_in_inventory_per_15_minuets"
    working_db = db_handler.select_another_db("stellar")
    operations = db_handler.select_collection(asset + "_bucket")
    ChangeInINvetory = ComputeChangeInInventory(operations, working_db,
                                                working_collection, opening_time, closing_time, asset)
    ChangeInINvetory.get_users()
    ChangeInINvetory.users_handler()


def compute_cumulative_net_inventory(asset, opening_time, closing_time):
    db_handler = Database()
    working_collection = asset + "_cumulative_net_inventory_per_15_minuets"
    working_db = db_handler.select_another_db("stellar")
    operations = working_db[asset + "_change_in_inventory_per_15_minuets"]
    CNT = ComputeCNT(operations, working_db,
                     working_collection, opening_time, closing_time, asset)
    CNT.multi_process_net_inventory()


async def computer_inter_trade_duration(asset, opening_time, closing_time):
    db_handler = Database()
    working_collection = asset + "_inter_trade_duration_per_u_15_minuets"
    db = db_handler.select_another_db("stellar")
    operations = db[asset + "_bucket"]
    inter_trade_duration = InterTradeDuration(db, operations, working_collection, asset, opening_time, closing_time)
    inter_trade_duration.get_users()
    inter_trade_duration.multi_process_handler()


async def clean_working_collection(asset, wc_name):
    db_handler = Database()
    stellar_result = db_handler.select_another_db("stellar")
    working_collection = stellar_result[asset + wc_name]
    operation_coll = db_handler.select_collection(asset + "_bucket")
    c_w_c = CleanCollection(working_collection, operation_coll)
    c_w_c.query_on_working_c()
    c_w_c.clean_operation()
#

# clean_working_collection("native", "_change_in_inventory_per_15_minuets")
loop = asy.get_event_loop()
# loop.run_until_complete(compute_user_working_capital("eth", "2019-10-09T15:30:38Z", "2019-12-15T14:26:38Z"))
# loop.run_until_complete(compute_cumulative_net_inventory("eth", "2019-10-09T15:30:38Z", "2019-12-15T14:26:38Z"))
loop.run_until_complete(computer_inter_trade_duration("eth", "2019-10-09T15:30:38Z", "2019-12-15T14:26:38Z"))
# loop.run_until_complete(clean_working_collection("native", "_change_in_inventory_per_15_minuets_part2"))
loop.close()

