from itertools import groupby

import numpy as np
import pandas as pd
import os
from sklearn.preprocessing import MinMaxScaler, MaxAbsScaler
from dtw import *
from multiprocessing import Manager, Queue, Process
from matplotlib import pyplot as plt
import math
import statistics


class KMeans_clustering:
    path_dataset = "./dataset_end/"
    path_dataset_sample_400 = "./dataset_small_sample_400/"
    path_dataset_sample_40 = "./sample_data_40/"
    users = []
    series = [[], []]
    btc_users = None
    eth_users = None
    SENTINEL = "END"
    btc_daily_transaction_volume = None
    eth_daily_transaction_volume = None
    plot_path = "./clustering_plots/fast_kmeans_/"
    active_scaller_method = ""
    clusters_color = [
        "green",
        "blue",
        "red",
        "black",
        "magenta"
    ]
    features_list = [
        "Date",
        "Transaction Volume",
        "Number of Transactions",
        "Change in Inventory",
        "Cumulative net Inventory",
        "Inter trade duration",
    ]
    assets = [
        "btc",
        "eth",
        # "native",
    ]
    active_asset = 0
    df_columns = ["Date", "NT", "TV", "CII", "CNI", "inter_trade"]
    inertia_matrix = []
    distortion_matrix = []

    def __init__(self, metric="dtw", num_workers=1, normalization_method=""):
        self.metric = metric
        self.num_workers = num_workers
        self.normalization_method = normalization_method
        print(f"Kmeans Clustering loaded by {metric} distance metric and {num_workers} Workers...")

    def load_dataset(self):
        print("Gathering data phase... ")
        for dirname, filename, files in os.walk(self.path_dataset_sample_40):
            for file in files:
                user_id = os.path.splitext(os.path.basename(file))[0]
                user = pd.read_csv(self.path_dataset_sample_40 + "/" + file, sep=",", header=[0, 1, 2, 3, 4, 5, 6, 7])
                user.columns = ["id", "Date", "NT", "TV", "CII", "CNI", "inter_trade", "asset_code"]
                user.drop('id', inplace=True, axis=1)
                user["source_account"] = user_id
                user["Date"] = pd.to_datetime([dt for dt in user["Date"].squeeze().tolist()],
                                              format="%Y-%m-%dT%H:%M:%S")
                user = user.set_index("Date")
                if self.normalization_method == "z_score":
                    user = self.z_score(user)
                elif self.normalization_method == "min_max":
                    min_max_scaller = MinMaxScaler()
                    min_max = min_max_scaller.fit(user.iloc[:, 0:5])
                    user.iloc[:, 0:5] = min_max.fit_transform(user.iloc[:, 0:5])
                    # user = self.min_max_scaller(user)
                elif self.normalization_method == "log":
                    user = self.log_scaller(user, "e")
                elif self.normalization_method == "log_10":
                    user = self.log_scaller(user, "10")
                elif self.normalization_method == "tanh":
                    user = self.tanh_scaller(user)
                elif self.normalization_method == "MaxAbsScaler":
                    max_abs_scaller = MaxAbsScaler()
                    transforme_obj = max_abs_scaller.fit(user.iloc[:, 0:5])
                    user.iloc[:, 0:5] = transforme_obj.fit_transform(user.iloc[:, 0:5])
                # user = user.interpolate(method="linear", limit_direction="forward")
                # self.users.append(user.loc['2019-10-15':'2019-11-15'])
                self.users.append(user)
        del dirname, filename, files, file, user
        self.users = pd.concat(self.users)
        # split Data ETH and BTC users from each other.
        self.btc_users = self.users.query("asset_code == 2.0")
        self.eth_users = self.users.query("asset_code == 3.0")
        # Prepare BTC users time series
        self.btc_users = self.btc_users.reset_index()
        self.btc_users = self.btc_users.set_index(["Date", "source_account"])
        self.btc_users = self.btc_users.groupby("source_account")

        # Prepare BTC users time series
        self.eth_users = self.eth_users.reset_index()
        self.eth_users = self.eth_users.set_index(["Date", "source_account"])
        self.eth_users = self.eth_users.groupby("source_account")
        print("Prepare buckets data...")
        for user in self.btc_users:
            tmp = user[1].reset_index()
            tmp = tmp[["Date", "NT", "TV", "CII", "CNI", "inter_trade"]]
            self.series[0].append(tmp)  # BTC Dataset
        self.series[0] = np.array(self.series[0])
        self.btc_daily_transaction_volume = self.users.query("asset_code == 2.0")
        tmp = self.btc_daily_transaction_volume.groupby(pd.Grouper(freq="d"))[
            "NT", "TV", "CII", "CNI"].sum()
        # self.btc_daily_transaction_volume = self.btc_daily_transaction_volume.groupby(pd.Grouper(freq="d")).iloc[:, 0:5].sum()

        for user in self.eth_users:
            tmp = user[1].reset_index()
            tmp = tmp[["Date", "NT", "TV", "CII", "CNI", "inter_trade"]]  # , "source_account"
            self.series[1].append(tmp)  # ETH Dataset
        self.series[1] = np.array(self.series[1])
        self.eth_daily_transaction_volume = self.users.query("asset_code == 3.0")
        self.eth_daily_transaction_volume = \
            self.eth_daily_transaction_volume.groupby(pd.Grouper(freq="d"))["TV", "CII"].sum()
        # self.eth_daily_transaction_volume = \
        #     self.eth_daily_transaction_volume.groupby(pd.Grouper(freq="d")).iloc[:, 0:5].sum()

    def tanh_scaller(self, df):
        self.active_scaller_method = "_tanh_"
        df_normalized = df.copy()
        for indx_col, column in enumerate(df_normalized.columns):
            if column == "Date" or column == "inter_trade" or column == "source_account" or column == "asset_code":
                continue
            df_normalized[column] = np.tanh(df_normalized[column].replace(0, np.nan))
            df_normalized[column] = df_normalized[column].replace(np.nan, 0)
        return df_normalized

    def log_scaller(self, df, log_method="10"):
        self.active_scaller_method = "_log_" + log_method
        df_normalized = df.copy()
        for indx_col, column in enumerate(df_normalized.columns):
            if column == "Date" or column == "inter_trade" or column == "source_account" or column == "asset_code":
                continue
            for indx, value in enumerate(df_normalized[column]):
                if log_method == "10":
                    df_normalized.iloc[indx, indx_col] = self.log_10(value)
                elif log_method == "e":
                    df_normalized.iloc[indx, indx_col] = self.log_(value)
                # df_normalized[column].iloc[indx] = self.log_10(value)
            # df_normalized[column] = np.log10(df_normalized[column])
        return df_normalized

    def log_10(self, value):
        if value > 0:
            return np.log10(value) + 1
        elif value < 0:
            return -abs(np.log10(abs(value))) -1
        else:
            return 0

    def log_(self, value):
        if value > 0:
            return np.log(value) + 1
        elif value < 0:
            return -abs(np.log(abs(value))) -1
        else:
            return 0

    def min_max_scaller(self, df):
        self.active_scaller_method = "_min_max_scaller_"
        df_normalized = df.copy()
        for column in df_normalized.columns:
            if column == "Date" or column == "inter_trade" or column == "source_account" or column == "asset_code":
                continue
            max = df_normalized[column].max()
            min = df_normalized[column].mean()
            if max == 0 or min == 0:
                continue
            if column == "TV" or column == "NT":
                df_normalized[column] = (df_normalized[column] - min) / (max - min)
        return df_normalized

    def z_score(self, df):
        self.active_scaller_method = "_z_s_core_"
        # copy the dataframe
        df_std = df.copy()
        # apply the z-score method
        for column in df_std.columns:
            max = df_std[column].max()
            if max == 0 and column == "CII":
                continue
            elif max == 0 and column == "CNI":
                continue
            elif max == 0 and column == "TV":
                continue
            elif max == 0 and column == "NT":
                continue
            elif column == "Date" or column == "inter_trade" or column == "source_account" or column == "asset_code":
                continue
            if column != "TV" or column != "NT":
                df_std[column] = (df_std[column] - df_std[column].mean()) / df_std[column].std()
        return df_std

    def euclid_dist(self, t1, t2):
        return np.sqrt(((t1 - t2) ** 2).sum(axis=1))

    def dtw_dist(self, t1, t2):
        dist = dtw(t1, t2)
        return dist.distance

    def distance_worker(self, worker_num, data_queue, distance_queue, distance_metric):
        while True:
            data = data_queue.get()
            if data["t1_index"] != self.SENTINEL:
                if distance_metric == "dtw":
                    dist = dtw(data["t1"], data["t2"])
                    distance = dist.distance
                    # distance = 0
                    # distance = self.dtw_dist(data["t1"], data["t2"])
                elif distance_metric == "euclid":
                    distance = np.sqrt(((data["t1"] - data["t2"]) ** 2).sum(axis=1))
                    # distance = self.euclid_dist(data["t1"], data["t2"])
                else:
                    distance = 0
                distance_queue.put({
                    "distance": distance,
                    "t1_index": data["t1_index"],
                    "t2_index": data["t2_index"]
                })
            else:
                distance_queue.put({
                    "t1_index": self.SENTINEL
                })
                print(f"Distance worker {worker_num} finished.")
                break

    def calc_centroids(self, data, centroids):
        queue = Queue()
        manager = Manager()
        workers = []
        distance_queue = Queue()

        # Prepare workers data
        print(f"Start Calc centroids...")
        dist = np.zeros([len(data), centroids.shape[0]])
        for idx, centroid in enumerate(centroids):
            for i in range(len(data)):
                # dist[i, idx] = self.dtw_dist(node, cen)
                if self.metric == "dtw":
                    columns = ["Date", "NT", "TV", "CII", "CNI", "inter_trade"]
                    if centroid.shape[1] > 6:
                        cen = pd.DataFrame(centroid[:, 1:], columns=columns)
                    elif centroid.shape[1] == 6:
                        cen = pd.DataFrame(centroid, columns=columns)
                    cen = cen.set_index("Date")
                    node = pd.DataFrame(data[i], columns=["Date", "NT", "TV", "CII", "CNI", "inter_trade"])
                    node = node.set_index("Date")
                elif self.metric == "euclid":
                    cen = centroid
                    node = data[i]
                else:
                    cen = None
                    node = None
                queue.put({
                    "t1": cen,
                    "t1_index": idx,
                    "t2": node,
                    "t2_index": i,
                })
        # Declare and start Distance Workers
        for k in range(self.num_workers):
            workers.append(
                Process(target=self.distance_worker, args=(k, queue, distance_queue, self.metric))
            )
            workers[k].start()
        for k in range(self.num_workers):
            queue.put({
                "t1_index": self.SENTINEL
            })

        queue.close()
        queue.join_thread()

        n_finished_workers = self.num_workers
        flag = True
        while flag:
            data = distance_queue.get()
            if "t1_index" in data.keys():
                if data["t1_index"] != self.SENTINEL:
                    dist[data["t2_index"], data["t1_index"]] = data["distance"]
                    # print("Distance detected.")
                elif data["t1_index"] == self.SENTINEL and n_finished_workers <= 0:
                    print("Distance matrix created.")
                    flag = False
                elif data["t1_index"] == self.SENTINEL and n_finished_workers > 0:
                    n_finished_workers = n_finished_workers - 1
                    # print(f"Num active workers {n_finished_workers}")
                    if n_finished_workers <= 0:
                        flag = False
                        print("Distance matrix created")
            elif "t1_index" not in data.keys() or data == None:
                print("Distance matrix created.")
                flag = False
        for k in range(self.num_workers):
            workers[k].terminate()
        del manager, queue, distance_queue
        print(f"Calc centroids finished.")
        return np.array(dist)

    def closest_centroids(self, data, centroids):
        dist = self.calc_centroids(data, centroids)
        # print(f"Result distance : {dist}")
        # print(f"Result closest clusters : {np.argmin(dist, axis=1)}")
        return {"closest": np.argmin(dist, axis=1), "distance": dist}

    def move_centroids(self, data, closest, centroids):
        k = centroids.shape[0]

        new_centroids_dataframe = []
        for c in np.unique(closest):
            series = []
            for ind, serie in enumerate(data[closest == c]):
                series.append(
                    pd.DataFrame(serie, columns=["Date", "NT", "TV", "CII", "CNI", "inter_trade"]))
            series = pd.concat(series)
            series["NT"] = series["NT"].astype(float)
            series["TV"] = series["TV"].astype(float)
            series["CII"] = series["CII"].astype(float)
            series["CNI"] = series["CNI"].astype(float)
            series["inter_trade"] = series["inter_trade"].astype(float)
            tmp = series.set_index("Date").groupby(pd.Grouper(freq="900s"))[
                ["NT", "TV", "CII", "CNI", "inter_trade"]].mean()
            tmp = tmp.reset_index()
            new_centroids_dataframe.append(tmp)
        new_centroids = np.array(new_centroids_dataframe)
        # new_centroids = np.array([data[closest == c][:, :, 1:].mean(axis=0) for c in np.unique(closest)])
        if k - new_centroids.shape[0] > 0:
            print("adding {} centroid(s)".format(k - new_centroids.shape[0]))
            additional_centroids = data[np.random.randint(0, len(data), k - new_centroids.shape[0])]
            print(f"additional_centroids shape : {additional_centroids.shape}")
            new_centroids = np.append(new_centroids, additional_centroids, axis=0)

        return new_centroids

    def init_centroids(self, data, num_clust):

        centroids = []

        for i in enumerate(np.random.randint(0, data.shape[0], num_clust)):
            centroids.append(data[i])
        centroids = np.array(centroids)

        # centroids = np.zeros([num_clust, data.shape[1]])
        #
        # centroids[0, :] = data[np.random.randint(0, data.shape[0], 1)]

        for i in range(1, num_clust):
            D2 = np.min([np.linalg.norm(data - c, axis=1) ** 2 for c in centroids[0:i, :]], axis=0)

            probs = D2 / D2.sum()

            cumprobs = probs.cumsum()

            ind = np.where(cumprobs >= np.random.random())[0][0]

            centroids[i, :] = np.expand_dims(data[ind], axis=0)

        return centroids

    def plot_user_features_error_base_mean_data(self, data, centroids, closests, num_clusters, feature_index=1):
        fig, axs = plt.subplots(num_clusters, 1, figsize=(25, 25))
        fig.suptitle(
            f"Clustering result base on {self.features_list[feature_index]} feature on Stellar {self.assets[self.active_asset].upper()} users.")

        for c in np.unique(closests):
            # mean_values[0] == Lower bound mean
            # mean_values[1] == upper bound mean
            mean_values = [[], []]
            lower_centroids = [[] for k in range(0, centroids[c].shape[0])]
            upper_centroids = [[] for k in range(0, centroids[c].shape[0])]
            for user in data[closests == c]:
                for indx, feature_value in enumerate(user[:, feature_index]):
                    if feature_value < centroids[c, indx, feature_index]:
                        lower_centroids[indx].append(feature_value)
                    elif feature_value >= centroids[c, indx, feature_index]:
                        upper_centroids[indx].append(feature_value)
            for i in range(len(lower_centroids)):
                if len(lower_centroids[i]) > 0:
                    mean_values[0].append(statistics.mean(lower_centroids[i]))
                else:
                    mean_values[0].append(0)
            for i in range(len(upper_centroids)):
                if len(upper_centroids[i]) > 0:
                    mean_values[1].append(statistics.mean(upper_centroids[i]))
                else:
                    mean_values[1].append(0)

            p1 = axs[c].plot(centroids[c, :, 0], centroids[c, :, feature_index], lw=0.7, c="gray", label="Other Nodes")
            p2 = axs[c].fill_between(centroids[c, :, 0], mean_values[0], mean_values[1], lw=1.0, facecolor="blue", alpha=0.4, label="Centroid")
            axs[c].set_title(f"Cluster {c}")
            plt.legend()
            # Line, Labels = axs[c].get_legend_handles_labels()
            # plot_labels = []
            # plot_lines = []
            # for indx, label in enumerate(Labels):
            #     if label not in plot_labels:
            #         plot_labels.append(label)
            #         plot_lines.append(Line[indx])
            #         if len(plot_labels) >= 2:
            #             axs[c].legend(plot_lines, plot_labels, loc="upper right")
            axs[c].set_xlabel(self.features_list[0])
            axs[c].set_ylabel(self.features_list[feature_index])
        fig.savefig(
            self.plot_path + self.active_scaller_method + "error_bar_mean_"  + self.features_list[feature_index].replace(" ", "_").lower() + "_" +
            self.assets[
                self.active_asset] + "_num_clus_" + str(num_clusters) + ".png")
        # fig.show()

    def plot_user_features_error_base_min_max(self, data, centroids, closests, num_clusters, feature_index=1):
        fig, axs = plt.subplots(num_clusters, 1, figsize=(25, 25))
        fig.suptitle(
            f"Clustering result base on {self.features_list[feature_index]} feature on Stellar {self.assets[self.active_asset].upper()} users.")

        for c in np.unique(closests):
            min_error = []
            max_error = []
            for user in data[closests == c]:
                for indx, feature_value in enumerate(user[:, feature_index]):
                    if indx <= (len(min_error) - 1):
                        if indx <= (len(min_error) - 1):
                            if feature_value < min_error[indx]:
                                min_error[indx] = feature_value
                        if indx <= (len(max_error) - 1):
                            if feature_value >= max_error[indx]:
                                max_error[indx] = feature_value
                    else:
                        min_error.append(feature_value)
                        max_error.append(feature_value)
            # axs[c].errorbar(centroids[c, :, 0], centroids[c, :, feature_index], yerr=[max_error, min_error], fmt='o')
            p1 = axs[c].plot(centroids[c, :, 0], centroids[c, :, feature_index], lw=0.5, color='gray', label="Other Nodes")
            p2 = axs[c].fill_between(centroids[c, :, 0], min_error, max_error, facecolor="blue", lw=1.0, alpha=0.3, label="Centroid")
            axs[c].set_title(f"Cluster {c}")
            plt.legend()

            axs[c].set_xlabel(self.features_list[0])
            axs[c].set_ylabel(self.features_list[feature_index])
        fig.savefig(
            self.plot_path + self.active_scaller_method + "error_bar_min_max_" + self.features_list[feature_index].replace(" ", "_").lower() + "_" +
            self.assets[
                self.active_asset] + "_num_clus_" + str(num_clusters) + ".png")
        # fig.show()
        plt.close('all')
        del fig, axs, min_error, max_error, feature_index, feature_value, c

    def plot_user_features(self, data, centroids, closests, num_clusters, feature_index=1):
        fig, axs = plt.subplots(num_clusters, 1, figsize=(25, 25))
        fig.suptitle(
            f"Clustering result base on {self.features_list[feature_index]} feature on Stellar {self.assets[self.active_asset].upper()} users.")

        for c in np.unique(closests):
            for user in data[closests == c]:
                axs[c].plot(user[:, 0], user[:, feature_index], c="gray", alpha=0.5)
            axs[c].plot(centroids[c, :, 0], centroids[c, :, feature_index], c=self.clusters_color[c])
            if c == 2:
                axs[c].set_ylabel(self.features_list[feature_index])
            axs[c].set_xlabel(self.features_list[0])
        plt.savefig(self.plot_path + self.active_scaller_method + self.features_list[feature_index].replace(" ", "_").lower() + "_" + self.assets[
            self.active_asset] + "_num_clus_" + str(num_clusters) + ".png")
        # plt.show()
        plt.close('all')
        del fig, axs

    def plot_user_features_daily(self, data, centroids, closests, num_clusters, feature_index=1):
        fig, axs = plt.subplots(num_clusters, 1, figsize=(25, 25))
        fig.suptitle(
            f"Clustering result base on {self.features_list[feature_index]} feature on Stellar {self.assets[self.active_asset].upper()} users.")

        for c in np.unique(closests):
            for user in data[closests == c]:
                columns = ["Date", "NT", "TV", "CII", "CNI", "inter_trade"]
                data_df = pd.DataFrame(user, columns=columns)
                data_df = data_df.set_index("Date").groupby(pd.Grouper(freq="d"))[self.df_columns[feature_index]].sum()
                axs[c].plot(data_df, c="gray", alpha=0.5, label="Other Nodes")
            columns = ["Date", "NT", "TV", "CII", "CNI", "inter_trade"]
            data_df = pd.DataFrame(centroids[c], columns=columns)
            data_df = data_df.set_index("Date").groupby(pd.Grouper(freq="d"))[self.df_columns[feature_index]].sum()
            axs[c].plot(data_df, c="Blue", label="Centroid")
            axs[c].set_ylabel(self.features_list[feature_index])
            axs[c].set_xlabel(self.features_list[0])
            Line, Labels = axs[c].get_legend_handles_labels()
            plot_labels = []
            plot_lines = []
            for indx, label in enumerate(Labels):
                if label not in plot_labels:
                    plot_labels.append(label)
                    plot_lines.append(Line[indx])
                    if len(plot_labels) >= 2:
                        axs[c].legend(plot_lines, plot_labels, loc="upper right")
                        axs[c].legend(plot_lines, plot_labels, loc="upper right")
        plt.savefig(self.plot_path + "_daily_plot_" + self.active_scaller_method + self.features_list[feature_index].replace(" ", "_").lower() + "_" + self.assets[
            self.active_asset] + "_num_clus_" + str(num_clusters) + ".png")
        plt.close('all')
        del fig, axs

    def plot_user_behaviors_features(self, data, centroids, closests, num_clusters):
        fig_portion, axs_portion = plt.subplots(num_clusters, 1, figsize=(25, 25))
        fig_portion.suptitle(f"Stellar Network's User Portion Volume on {self.assets[self.active_asset].upper()} ")

        fig_inventory, axs_inventory = plt.subplots(num_clusters, 1, figsize=(25, 25))
        fig_inventory.suptitle(f"Stellar Network's User Portion Volume on {self.assets[self.active_asset].upper()} ")

        for c in np.unique(closests):
            for user in data[closests == c]:
                user_behavioral = self.get_user_behavioral_features(user)
                axs_portion[c].plot(user_behavioral["portion_volume"],
                                    c="gray", alpha=0.5, label="Other Nodes")
                axs_inventory[c].plot(user_behavioral["inventory_trade_ratio"], c="gray", alpha=0.5, label="Other Nodes")

            user_behavioral = self.get_user_behavioral_features(centroids[c])
            axs_portion[c].plot(user_behavioral["portion_volume"],
                                c="Red", label="Centroid")
            axs_inventory[c].plot(user_behavioral["inventory_trade_ratio"],
                                  c="Red", label="Centroid")
            # axs_portion[c].legend(("Other Nodes", "Centroid"), loc=2)
            # axs_inventory[c].legend(("Other Nodes", "Centroid"), loc=2)
            axs_portion[c].set_ylabel("Portion Volume")
            axs_inventory[c].set_ylabel("Inventory Trade ratio")
            axs_portion[c].set_xlabel("Date")
            axs_inventory[c].set_xlabel("Date")

        for ax_indx in range(0, num_clusters):
            plot_labels = []
            plot_lines = []
            Line, Labels = axs_portion[ax_indx].get_legend_handles_labels()
            for indx, label in enumerate(Labels):
                if label not in plot_labels:
                    plot_labels.append(label)
                    plot_lines.append(Line[indx])
                    if len(plot_labels) >= 2:
                        axs_portion[ax_indx].legend(plot_lines, plot_labels, loc="upper right")
                        axs_inventory[ax_indx].legend(plot_lines, plot_labels, loc="upper right")

        fig_portion.savefig(
            self.plot_path + self.active_scaller_method + self.assets[self.active_asset] + "_" + "portion_volume" + "_num_clust_" + str(
                num_clusters) + ".png")
        # fig_portion.show()
        fig_inventory.savefig(
            self.plot_path + self.assets[self.active_asset] + "_" + "inventory_trade_ratio" + "_num_clust_" + str(
                num_clusters) + ".png")
        # fig_portion.show()
        plt.close('all')
        del fig_portion, fig_inventory, axs_inventory, axs_portion

    def get_user_behavioral_features(self, user):
        user_daily_transaction_volume = pd.DataFrame(user,
                                                     columns=["Date", "NT", "TV", "CII", "CNI", "inter_trade"])
        user_daily_transaction_volume = \
            user_daily_transaction_volume.set_index("Date").groupby(pd.Grouper(freq="d"))["TV"].sum()
        if self.active_asset == 2:

            if user_daily_transaction_volume.max() == 0.0:
                portion_volume = user_daily_transaction_volume
                inventory_trade_ratio = user_daily_transaction_volume
            else:
                portion_volume = user_daily_transaction_volume / self.btc_daily_transaction_volume.TV
                inventory_trade_ratio = user_daily_transaction_volume / self.btc_daily_transaction_volume.CII
        else:

            if user_daily_transaction_volume.max() == 0:
                portion_volume = user_daily_transaction_volume
                inventory_trade_ratio = user_daily_transaction_volume
            else:
                portion_volume = user_daily_transaction_volume / self.eth_daily_transaction_volume.TV
                inventory_trade_ratio = user_daily_transaction_volume / self.eth_daily_transaction_volume.CII
        return {"portion_volume": portion_volume, "inventory_trade_ratio": inventory_trade_ratio}

    def inertia(self, distance_matrics, closest):
        total_inertia = np.sum([np.sum(distance_matrics[c == closest, c]) for c in np.unique(closest)])
        return total_inertia

    def distortion(self, distance_matrix, closest):

        total_distortion = np.sum(
            [np.sum(distance_matrix[c == closest, c]) / len(distance_matrix[c == closest])
             for c in np.unique(closest)]
        )

        return total_distortion

    def k_means(self, data, num_clust, num_iter):
        # centroids = self.init_centroids(data, num_clust) K-Means++
        centroids = []

        for _, i in enumerate(np.random.randint(0, data.shape[0], num_clust)):
            centroids.append(data[i])

        centroids = np.array(centroids)

        last_centroids = centroids
        for n in range(num_iter):
            print(f"Iteration {n}")
            closest_distance = self.closest_centroids(data, centroids)
            centroids = self.move_centroids(data, closest_distance["closest"], centroids)
            if not np.any(last_centroids != centroids):
                print("early finish!")
                break
            last_ctroids = centroids
        return {"centroids": centroids, "closest": closest_distance["closest"]}

    def plot_elbow_method(self, method):
        if method == "inertia":
            plt.plot(self.inertia_matrix[:, 0], self.inertia_matrix[:, 1], linestyle="--", marker="o", color="b")
            plt.ylabel("inertia")
        elif method == "distortion":
            plt.plot(self.distortion_matrix[:, 0], self.distortion_matrix[:, 1], linestyle="--", marker="o", color="b")
            plt.ylabel("Distortion")

        plt.xlabel("K")
        plt.title("Elbow Method for optimal K")
        plt.savefig(self.plot_path + "elbow_method_by_" + method + ".png")
        plt.close('all')
    def k_means_and_plot_results(self, num_clust, num_iter):

        for ind, asset in enumerate(self.assets):
            self.active_asset = ind
            centroids = []
            print(
                f"K-Means Clustering on {self.assets[self.active_asset].upper()} with {num_clust} clusters started...")
            for _, i in enumerate(np.random.randint(0, self.series[ind].shape[0], num_clust)):
                centroids.append(self.series[ind][i])

            centroids = np.array(centroids)

            last_centroids = centroids
            for n in range(num_iter):
                print(f"Iteration {n}")
                closest_distance = self.closest_centroids(self.series[ind], centroids)
                centroids = self.move_centroids(self.series[ind], closest_distance["closest"], centroids)
                if not np.any(last_centroids != centroids):
                    print("early finish!")
                    break
                last_centroids = centroids
            self.inertia_matrix.append([num_clust, self.inertia(closest_distance["distance"], closest_distance["closest"])])
            self.distortion_matrix.append([num_clust, self.distortion(closest_distance["distance"], closest_distance["closest"])])
            print(self.inertia_matrix)
            self.plot_user_behaviors_features(self.series[ind], centroids, closest_distance["closest"], num_clust)
            #for i in range(1, 5):
            #    # self.plot_user_features(self.series[ind], centroids, closest_distance["closest"], num_clust, feature_index=i)
            #    self.plot_user_features_daily(self.series[ind], centroids, closest_distance["closest"], num_clust, feature_index=i)
            #    self.plot_user_features_error_base_min_max(self.series[ind], centroids,
            #                                               closest_distance["closest"], num_clust, feature_index=i)
            #    self.plot_user_features_error_base_mean_data(self.series[ind], centroids,
            #                                               closest_distance["closest"], num_clust, feature_index=i)
