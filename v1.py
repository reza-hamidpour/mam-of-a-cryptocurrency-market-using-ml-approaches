import asyncio as asy
from clasess.database import Database
from clasess.cumpute_change_in_inventory_15_minuets_final import ComputeChangeInInventory
from clasess.user_amount_in_15_minuets_final import userAmountIn15Minuets


async def compute_user_working_capital(asset, opening_time, closing_time):
    db_handler = Database()
    working_collection = asset + "_user_working_capital_selling_per_15_minuets"
    working_db = db_handler.select_another_db("stellar_result")
    operations = db_handler.select_collection(asset + "_bucket")
    UWC = userAmountIn15Minuets(operations, working_db, working_collection, asset, opening_time, closing_time)
    UWC.get_users()
    await UWC.handel_users()

async def compute_change_in_inventory(asset, opening_time, closing_time):
    db_handler = Database()
    working_collection = asset + "_change_in_inventory_per_15_minuets"
    working_db = db_handler.select_another_db("stellar_result")
    operations = db_handler.select_collection(asset + "_bucket")
    ChangeInINvetory = ComputeChangeInInventory(operations, working_db,
                                                working_collection, opening_time, closing_time, asset)
    ChangeInINvetory.get_users()
    await ChangeInINvetory.users_handler()


loop  = asy.get_event_loop()
loop.run_until_complete(compute_change_in_inventory("native", "2019-10-09T15:30:38Z", "2019-12-15T14:26:38Z"))
loop.close()
