import asyncio as asy
from clasess.database import Database
from clasess.cumpute_change_in_inventory_15_minuets_final import ComputeChangeInInventory
from clasess.user_amount_in_15_minuets_final import userAmountIn15Minuets
from clasess.compute_cumulative_net_inventory_final import ComputeCNT
from clasess.clean_result_collection import CleanCollection
from clasess.make_matrix_per_user import MatrixPerUser
from clasess.compute_TV_NT_CII_15_minuets import userTvNtCii


async def compute_TV_NT_CII(asset, opening_time, closing_time):
    db_handler = Database()
    working_collection = asset + "_TV_NT_CII_per_15_minuets"
    working_db = db_handler.select_another_db("stellar_result")
    operations = working_db[asset + "_bucket"]
    TV_NT_CII = userTvNtCii(operations, working_db, working_collection, asset, opening_time, closing_time)
    TV_NT_CII.get_users()
    await TV_NT_CII.handel_users()

async def compute_user_working_capital(asset, opening_time, closing_time):
    db_handler = Database()
    working_collection = asset + "_correct_user_working_capital_selling_per_15_minuets"
    working_db = db_handler.select_another_db("stellar_result")
    operations = db_handler.select_collection(asset + "_bucket")
    UWC = userAmountIn15Minuets(operations, working_db, working_collection, asset, opening_time, closing_time)
    UWC.get_users()
    await UWC.handel_users()

async def compute_change_in_inventory(asset, opening_time, closing_time):
    db_handler = Database()
    working_collection = asset + "_change_in_inventory_per_15_minuets_part2"
    working_db = db_handler.select_another_db("stellar_result")
    operations = db_handler.select_collection(asset + "_bucket")
    ChangeInINvetory = ComputeChangeInInventory(operations, working_db,
                                                working_collection, opening_time, closing_time, asset)
    ChangeInINvetory.get_users()
    await ChangeInINvetory.users_handler()

async def compute_cumulative_net_inventory(asset, opening_time, closing_time):
    db_handler = Database()
    working_collection = asset + "_CNI_per_15_minuets_bugfix"
    working_db = db_handler.select_another_db("stellar_result")
    operations = working_db[asset + "_TV_NT_CII_per_15_minuets"]
    CNT = ComputeCNT(operations, working_db,
                     working_collection, opening_time, closing_time, asset)
    CNT.get_users()
    await CNT.handle_users()

async def clean_working_collection(asset, wc_name):
    db_handler = Database()
    stellar_result = db_handler.select_another_db("stellar_result")
    working_collection = stellar_result[asset + wc_name]
    operation_coll = db_handler.select_collection(asset + "_bucket")
    c_w_c = CleanCollection(working_collection, operation_coll)
    c_w_c.query_on_working_c()
    c_w_c.clean_operation()

async def makeMatrix(opening_time, closing_time):
    db_handler = Database()
    stellar_result = db_handler.select_another_db("stellar_result")
    operations = stellar_result["operations"]
    collections = {
        "eth": {
            "tv_nt_cii": stellar_result["eth_TV_NT_CII_per_15_minuets"],
            "cni": stellar_result["eth_CNI_per_15_minuets"]
        },
        "btc": {
            "tv_nt_cii": stellar_result["btc_TV_NT_CII_per_15_minuets"],
            "cni": stellar_result["btc_CNI_per_15_minuets"]
        }
    }
    stellar_result = None
    assets = ["eth", "btc"]
    matrix_creator = MatrixPerUser(collections, operations, assets, opening_time, closing_time)
    matrix_creator.query_on_users()
    await matrix_creator.handler_users()

# clean_working_collection("native", "_change_in_inventory_per_15_minuets")
loop = asy.get_event_loop()

loop.run_until_complete(makeMatrix("2019-10-09T15:30:38Z", "2019-12-15T14:26:38Z"))
#loop.run_until_complete(compute_cumulative_net_inventory("btc", "2019-10-09T15:30:38Z", "2019-12-15T14:26:38Z"))
# loop.run_until_complete(compute_TV_NT_CII("native", "2019-10-09T15:30:38Z", "2019-12-15T14:26:38Z"))
# loop.run_until_complete(clean_working_collection("native", "_change_in_inventory_per_15_minuets_part2"))
loop.close()
