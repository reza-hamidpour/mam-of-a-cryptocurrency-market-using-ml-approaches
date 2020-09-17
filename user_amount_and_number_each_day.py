from clasess.database import Database
from clasess.user_amount_in_each_day import UserAmountInEachDay

my_db = Database()
op_coll = my_db.select_collection("operations")
users_transactions = my_db.select_collection("users_t_per_asset")
user_amount_and_number = UserAmountInEachDay(my_db.get_db(), op_coll, users_transactions,
                                             )
user_amount_and_number.get_user_transactions()
user_amount_and_number.get_all_transactions_per_user()