import os
import numpy as np
import pandas as pd
from multiprocessing import Process, Queue, Manager
#from fastdtw import fastdtw
import asyncio
from scipy.cluster.hierarchy import single, average, complete, ward, dendrogram
from matplotlib import pyplot as plt
from dtw import *
from datetime import datetime
import sys

class DistaceMatrix:

    path_dataset = "./Dataset"
    path_save = "dataset/"
    distance_matrix_name = "separated_users_distance_matrix_500_users.csv"
    users = []
    my_series = []
    distance_matrix = None
    queue_size = 100
    SENTINEL = "END"
    n_series = 0
    num_cpu = 8

    def eth_btc_clustering(self):
        # self.load_dataset()
        # print("Preparing BTC distance matrix...")
        # self.prepare_matrix("2.0")
        # self.__task_handler()
        # self.save_our_matrix("btc_")
        # print("Preparing BTC distance matrix finished successfully. ")
        print("Preparing ETH distance matrix...")
        self.load_dataset()
        self.prepare_matrix("3.0")
        self.__task_handler()
        self.save_our_matrix("eth_")
        print("Preparing ETH distance matrix finished successfully. ")
        # result = self.hierachical_clustering(self.distance_matrix, "complete")

    def load_dataset(self):
        print("Gathering data phase... ")
        # print("Normalizing Dataset.")
        self.users = []
        for dirname, filename, files in os.walk(self.path_dataset):
            for file in files:
                user = pd.read_csv( self.path_dataset + "/" + file, sep=",", header=[0, 1, 2, 3, 4, 5, 6])
                # user = user.interpolate()
                user_id = os.path.splitext(os.path.basename(file))[0]
                user.columns = ["id", "unixtime", "Date", "NT", "TV", "CII", "CNI", "asset_code"]
                user.drop('unixtime', inplace=True, axis=1)
                user.drop('id', inplace=True, axis=1)
                user["source_account"] = user_id
                user["Date"] = pd.to_datetime([dt for dt in user["Date"].squeeze().tolist()], format="%Y-%m-%dT%H:%M:%S")
                user = user.set_index("Date")
                self.users.append(user.loc['2019-10-15':'2019-11-15'])
        self.users = pd.concat(self.users)
        print("Data gathered successfully.")

    def prepare_matrix(self, asset_code):
        print("Preparing Dataset into standard form...")
        self.users = self.users.query("asset_code == " + asset_code)
        self.users = self.users.reset_index()
        self.users = self.users.set_index(["Date", "source_account"])
        self.users = self.users.groupby("source_account")

        for user in self.users:
            self.my_series.append(user[1].values)
        self.users = None
        for i in range(len(self.my_series)):
            length = len(self.my_series[i])
            self.my_series[i] = self.my_series[i].reshape((length, 5))

    def __task_handler(self):
        mangers = Manager()
        print("Calculating distance matrix... ")
        self.n_series = len(self.my_series)
        self.distance_matrix = np.zeros(shape=(self.n_series, self.n_series))
        print(f"{self.n_series} series detected.")
        print("Distance matrix prepared.")
        for i in range(self.n_series):
            print(f"Time series {i} started at {datetime.now()}... ")

            qu = Queue()
            producer = Process(target=self.task_producer, args=(i, qu))
            producer.start()

            consumers = []
            dtw_dict = Queue()
            for k in range(self.num_cpu):
                consumers.append(
                    Process(target=self.task_consumer, args=(k, qu, dtw_dict))
                )
                consumers[k].start()
            qu.close()
            qu.join_thread()

            producer.join()
            flag = True
            while flag:
                data = dtw_dict.get()
                if "i" in data.keys():
                    if data["i"] != self.SENTINEL:
                        self.distance_matrix[data["i"], data["j"]] = data["dtw"]
                        self.distance_matrix[data["j"], data["i"]] = data["dtw"]
                    elif data["i"] == self.SENTINEL:
                        print("break")
                        flag = False
                elif "i" not in data.keys():
                    print("Finished.")
                    flag = False
            print(f"Time series {i} finished at {datetime.now()}")
            for k in range(self.num_cpu):
                consumers[k].terminate()
        print("Distance matrix created successfully.")

    def task_producer(self, index_i, qu):
        for j in range(self.n_series):
            qu.put({
                   "i_index": index_i,
                   "j_index": j,
                   "i_series": self.my_series[index_i],
                   "j_series": self.my_series[j]})

        for j in range(self.num_cpu):
            qu.put({
                "i_index": self.SENTINEL
            })

    def task_consumer(self, worker_index, queue, res_q):
        while True:
            data = queue.get()
            if data['i_index'] != self.SENTINEL:
                distance = dtw(data['i_series'], data['j_series'])
                res_q.put({
                    "i": data['i_index'],
                    "j": data["j_index"],
                    "dtw": distance.distance
                })
            elif data['i_index'] == self.SENTINEL:
                print(f"Worker {worker_index} finished.")
                break
        res_q.put({
            "i_index": self.SENTINEL
        })

    def save_our_matrix(self, bucket_name):
        print(f"Saving distance matrix into {self.path_save}{bucket_name}{self.distance_matrix_name} ... ")
        # fmt_str = [["%s" for j in range(self.n_series)] for i in range(self.n_series)]
        with open(self.path_save + bucket_name + self.distance_matrix_name, "a") as file_handler:
            np.savetxt( file_handler, self.distance_matrix, delimiter=",")
        print("Distance matrix saved successfully.")

    def hierachical_clustering(self, dist_mat, method='complete'):
        sys.setrecursionlimit(10000)
        if method == "complete":
            Z = complete(dist_mat)
        elif method == "single":
            Z = single(dist_mat)
        elif method == "average":
            Z = average(dist_mat)
        elif method == "ward":
            Z = ward(dist_mat)
        fig = plt.figure(figsize=(16, 8))
        dn = dendrogram(Z)
        plt.savefig(self.path_save + "separated_500_users_hierachical_clustering.png")
        plt.title(f" BTC AND ETH Dendogram plot with {method}.")
        plt.show()
        return Z
