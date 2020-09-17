from datetime import datetime
from datetime import timedelta

class InterTradeDuration:
    users = None
    inter_trade_duration = None

    def __init__(self, db, collection):
        self.db = db
        self.transactions = collection
        self.inter_trade_duration = db["ETH_inter_trade_duration_per_u_15_minuets"]

    def get_users(self):
        query = [{
                "$match": {"offer_id": "0"}
            }, {
            "$sort": {"created_at": 1}
        }, {
                "$group": {
                    "_id": "$source_account"
                }
            }
            ]
        self.users = self.transactions.aggregate(pipeline=query, allowDiskUse=True)

    def get_users_transaction(self):
        for user in self.users:
            query = [{
                    "$match": {"source_account": user["_id"],
                               "offer_id": "0"}
                }, {"$sort": {"created_at": 1}}]
            user_transactions = self.transactions.aggregate(pipeline=query, allowDiskUse=True)
            self.time_window_transactions(user["_id"], user_transactions)

    def time_window_transactions(self, source_account, transactions):
        start_time_window = None
        time_window_transactions = []
        flag = False
        for current_transaction in transactions:
            if start_time_window == None:
                start_time_window = datetime.strptime(current_transaction["created_at"], "%Y-%m-%dT%H:%M:%S.%fZ")
                time_window_transactions.append(start_time_window)
                flag = True
            else:
                tmp = datetime.strptime(current_transaction["created_at"], "%Y-%m-%dT%H:%M:%S.%fZ")
                distance = tmp - start_time_window
                if distance.seconds > 900:
                                                            # Removing first time window.
                    inter_trade_duration = [self.get_time_window_inter_trade_duration(source_account, time_window_transactions)]
                    gap = self.fill_gap_between_two_time_window(source_account, start_time_window + timedelta(0, 900), tmp)
                    last_tw = gap[-1]
                    # del gap[-1]
                    # gap.append(inter_trade_duration)
                    # print("source account: ", source_account)
                    # print(result_inter_trades)
                    result_inter_trades = gap + inter_trade_duration
                    self.save_user_trade_duration(gap)
                    start_time_window = last_tw["time_window"]
                    time_window_transactions = [start_time_window, tmp]
                    flag = False
                else:
                    time_window_transactions.append(tmp)
                    flag = True
                tmp = None
        if flag:
            inter_trade_duration = [self.get_time_window_inter_trade_duration(current_transaction["source_account"], time_window_transactions)]
            self.save_user_trade_duration(inter_trade_duration)
            flag = False

    def get_time_window_inter_trade_duration(self, source_account, time_window_t):
        l_time = None
        distances = []
        if len(time_window_t) > 1:
            for tw_position in time_window_t:
                if l_time != None:
                    distance = l_time - tw_position
                    distances.append(distance.seconds)
                    l_time = tw_position
                else:
                    l_time = tw_position
            number_of_distance = len(distances)
            sum_all_elements = sum(distances)
            inter_trade_duration = sum_all_elements // number_of_distance
        else:
            inter_trade_duration = 900
        start_time_window = datetime.strftime(time_window_t[0], "%Y-%m-%dT%H:%M:%S.%fZ")
        return {"source_account": source_account,
                "time_window": start_time_window,
                "inter_trade": inter_trade_duration}

    def fill_gap_between_two_time_window(self, source_account, current_tw, next_t):
        gap = []
        distance = next_t - current_tw
        next_tw = current_tw
        while next_tw < next_t:
            if distance.seconds > 900:
                gap.append({"source_account": source_account,
                            "time_window": next_tw,
                            "inter_trade": 900})
            else:
                gap.append({"source_account": source_account,
                            "time_window": next_tw,
                            "inter_trade": distance.seconds})
            next_tw = next_tw + timedelta(0, 900)
            distance = next_t - next_tw
        return gap

    def save_user_trade_duration(self, time_windows):
        self.inter_trade_duration.insert_many(time_windows)
        print(len(time_windows), " Time Window saved.")
