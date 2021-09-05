from scipy.cluster.hierarchy import single, average, complete, ward, dendrogram
# from tslearn.clustering import TimeSeriesKMeans
# from dtw import dtw
from sklearn.cluster import AffinityPropagation
import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
import sys
import os
# from minisom import MiniSom
import math
from sklearn.preprocessing import MinMaxScaler
import gc
from kmeans_cluster import ts_cluster
from sys import getsizeof

class Stellar_dataset_clustering:
    dataset_path = "./dataset/"
    path_dataset = "./Dataset"
    matrics_file_name = "separated_users_distance_matrix_eth_btc_500_users_normalized.csv"
    btc_matrix_file_name = "btc_separated_users_distance_matrix_500_users.csv"
    eth_matrix_file_name = "eth_separated_users_distance_matrix_500_users.csv"
    distance_matrics = None
    plot_path = "./clustering_plots/smo/"
    num_series = 500
    users = []
    my_series = []
    series = [[], []] # first Index Of his list gather BTC dataset and the second one gather ETH dataset.
    # Note/ If you want to add another asset into your code just add your asset code and asset name
    # into self.assets list and then add another list into self.series.
    protion_volume_file = ["btc_protion_volume_test.csv", "eth_protion_volume_text.csv"]
    inventory_trade_ratio_file = ["btc_inventory_trade_ratio_test.csv", "eth_inventory_trade_ratio_text.csv"]
    protion_volume_matrix = [None, None]
    inventory_trade_ratio_matrix = [None, None]
    clusters_protion_volume = [[], []]
    clusters_inventory_trade_ratio = [[], []]
    btc_users = []
    eth_users = []
    assets = [{"asset_name": "btc", "asset_code": "2.0"},
              {"asset_name": "eth", "asset_code": "3.0"}]
    active_asset_id = 0


    def process_handler(self):
        self.load_dataset()
        for ind, asset in enumerate(self.assets):
            self.active_asset_id = ind
            plot_name = "normalized_serie_" + asset["asset_name"] + "_smo_clustering_500_users_based_on_TV_1-0_0-2_x_y_2_3"

            # self.kmeans_clustering("without_transform_kmeans_clustering_500_users_num_clu_4")
            # self.ts_clustering(5)
            # self.affinity_propagation_clustering("affinity_propagation_default")

            self.load_matrics()
            self.load_behavioral_matrics()
            print("Loading dataset into clustering method")
            self.heirachical_clustering(self.distance_matrics, "average", "normalized_average_method_dendogram_all_users")
            self.smo_clustering(plot_name)
            # self.smo_clusternig_base_on_real_data(plot_name)

    def load_dataset(self):
        print("Gathering data phase... ")
        for dirname, filename, files in os.walk(self.path_dataset):
            for file in files:
                user = pd.read_csv(self.path_dataset + "/" + file, sep=",", header=[0, 1, 2, 3, 4, 5, 6])
                user_id = os.path.splitext(os.path.basename(file))[0]
                user.columns = ["id", "Date", "NT", "TV", "CII", "CNI", "inter_trade", "asset_code"]
                user.drop('unixtime', inplace=True, axis=1)
                user.drop('id', inplace=True, axis=1)

                user["source_account"] = user_id
                user["Date"] = pd.to_datetime([dt for dt in user["Date"].squeeze().tolist()], format="%Y-%m-%dT%H:%M:%S")
                user = user.set_index("Date")
                user = user.interpolate(method="linear", limit_direction="forward")
                # self.users.append(user.loc['2019-10-15':'2019-11-15'])
                self.users.append(user)
        del dirname, filename, files, file, user
        self.users = pd.concat(self.users)
        # split Data ETH and BTC users from each other.
        self.btc_users = self.users.query("asset_code == 2.0")
        self.eth_users = self.users.query("asset_code == 3.0")
        #Prepare BTC users time series
        self.btc_users = self.btc_users.reset_index()
        self.btc_users = self.btc_users.set_index(["Date", "source_account"])
        self.btc_users = self.btc_users.groupby("source_account")

        # Prepare BTC users time series
        self.eth_users = self.eth_users.reset_index()
        self.eth_users = self.eth_users.set_index(["Date", "source_account"])
        self.eth_users = self.eth_users.groupby("source_account")
        for user in self.btc_users:
            tmp = user[1].reset_index()
            tmp_ = tmp[["Date", "NT", "TV", "CII", "CNI", "source_account"]]
            # scaler = MinMaxScaler()
            # tmp_[["NT", "TV", "CII", "CNI"]] = scaler.fit_transform(tmp_[["NT", "TV", "CII", "CNI"]])
            tmp_ = tmp_.set_index("Date")
            self.series[0].append(tmp_) #BTC Dataset

        for user in self.eth_users:
            tmp = user[1].reset_index()
            tmp_ = tmp[["Date", "NT", "TV", "CII", "CNI", "source_account"]]
            # scaler = MinMaxScaler()
            # tmp_[["NT", "TV", "CII", "CNI"]] = scaler.fit_transform(tmp_[["NT", "TV", "CII", "CNI"]])
            tmp_ = tmp_.set_index("Date")
            self.series[1].append(tmp_) #ETH Dataset

        # self.users = self.users.reset_index()
        # self.users = self.users.set_index(["Date", "source_account"])
        # self.users = self.users.groupby("source_account")
        # for user in self.users:
        #     tmp = user[1].reset_index()
        #     tmp_ = tmp[["Date", "NT", "TV", "CII", "CNI", "asset_code"]]
        #     tmp_ = tmp_.set_index("Date")
        #     self.series.append(tmp_)
        del self.users, tmp, user, tmp_
        ### On SMO clustering comment these lines.
        # print(f"number of users {self.series[0]}")
        # for i in range(0, 500):
        #     self.series[i] = self.series[i].values
        # self.series = self.series[:500]
        #

        #     self.my_series.append( c_tmp.reshape(5, len(c_tmp)))
        #     self.my_series.append(MinMaxScaler().fit_transform(self.series[i].values))
        #     self.my_series[i] = self.my_series[i].reshape(len(self.my_series[i]), 5)
        #     tmp = MinMaxScaler().fit_transform(self.series[i].values)
        #     self.my_series.append(tmp.reshape(len(tmp), 5))
        ### Comment those lines until it.
        print(f"Number of series {len(self.my_series)}")
        print("Data gathered successfully.")
        gc.collect()

    def load_matrics(self):
        print("Loading dataset into Pandas... ")
        if self.assets[self.active_asset_id]["asset_name"] == "btc":
            self.distance_matrics = pd.read_csv(self.dataset_path + self.btc_matrix_file_name, sep=",", header=None)
        elif self.assets[self.active_asset_id]["asset_name"] == "eth":
            self.distance_matrics = pd.read_csv(self.dataset_path + self.eth_matrix_file_name, sep=",", header=None)
        # self.distance_matrics = pd.read_csv(self.dataset_path + self.matrics_file_name, sep=",", header=None)
        # self.my_series = self.distance_matrics.values
        self.my_series = self.distance_matrics.iloc[:500,:500].values
        print(f"{len(self.my_series)} series detected.")
        # self.distance_matrics = self.distance_matrics.interpolate(method="linear", limit_direction="forward")
        print("Data loaded successfully.")

    def load_behavioral_matrics(self):
        print(f"Loading {self.active_asset_id} Protion volume matrix")
        self.protion_volume_matrix[self.active_asset_id] = pd.read_csv(self.dataset_path + self.protion_volume_file[self.active_asset_id], header=[0,1,2])
        self.protion_volume_matrix[self.active_asset_id].columns = ['source_account', 'Date', 'protion_volume']
        print(f"Loading {self.active_asset_id} Inventory/Trade ratio matrix")
        self.inventory_trade_ratio_matrix[self.active_asset_id] = pd.read_csv(self.dataset_path + self.inventory_trade_ratio_file[self.active_asset_id])
        self.inventory_trade_ratio_matrix[self.active_asset_id].columns = ['source_account', 'Date', 'inventory']

    def ts_clustering(self, n_clusters):
        print("Clustering method")

        ts = ts_cluster(n_clusters)
        ts.k_means_clust_mp(self.my_series, 100, 0, True, 7)

    def test_plot(self):
        fig, axs = plt.subplots(2, 2, figsize=(25, 25))
        fig.suptitle(f"Clustering {len(self.my_series)} users in Stellar network")
        cluster = [
            [],
            [],
            [],
            []
        ]
        for i in range(len(self.my_series)):
            axs[0, 0].plot(self.series[i].index, self.series[i]["TV"], c="gray", alpha=0.4)
            cluster[0].append(self.series[i]["TV"])
            if i >= 100:
                break

        axs[0, 0].plot(self.series[0].index, np.average(np.vstack(cluster[0]), axis=0), c="red")
        plt.savefig(self.plot_path + "TV-Test-normalized" + ".png")

    def heirachical_clustering(self, dist_mat, method, plot_name):
        sys.setrecursionlimit(1000000)
        if method == "complete":
            Z = complete(dist_mat)
        elif method == "single":
            Z = single(dist_mat)
        elif method == "average":
            Z = average(dist_mat)
        elif method == "ward":
            Z = ward(dist_mat)
        fig = plt.figure(figsize=(50, 19))
        dn = dendrogram(Z, leaf_rotation=90)
        plt.axhline(y=2900000000000, color='r', linestyle='--')
        plt.axhline(y=5999000000000, color='gray', linestyle='--')

        plt.savefig(self.plot_path + plot_name + ".png")
        print(f"Plot {plot_name} saved.")
        plt.title(f" BTC AND ETH Dendogram plot with {method}.")
        plt.show()
        return Z

    def kmeans_clustering(self, plot_name):
        print("Preparing KMeans parameters...")
        n_clusters = 4#My suggest 4
        # n_clusters = math.ceil(math.sqrt(len(self.my_series)))

        km = TimeSeriesKMeans(n_clusters=n_clusters, metric="dtw", verbose=True, n_jobs=7, max_iter=10, init="random")
        labels = km.fit_predict(self.series)
        print("KMeans finished successfully.")
        self.kmeans_plot(labels, n_clusters, plot_name)

    def kmeans_plot(self, labels, n_clusters, plot_name):
        print("Preparing cluster plots...")
        fig, axs = plt.subplots(2, 2, figsize=(25, 25))
        fig.suptitle(f"Clustering {len(self.my_series)} users in Stellar network")
        cluster = [
            [],
            [],
            [],
            []
        ]
        ax_index = [[0, 0],
                    [0, 1],
                    [1, 0],
                    [1, 1],
                    ]
        for i in range(len(labels)):
            axs[ax_index[labels[i]][0], ax_index[labels[i]][1]].plot(self.series[i].index, self.series[i]["NT"], c="gray", alpha=0.4)
            cluster[labels[i]].append(self.series[i]["NT"])

        axs[0, 0].plot(self.series[0].index, np.average(np.vstack(cluster[0]), axis=0), c="red")
        axs[0, 1].plot(self.series[0].index, np.average(np.vstack(cluster[1]), axis=0), c="red")
        axs[1, 0].plot(self.series[0].index, np.average(np.vstack(cluster[2]), axis=0), c="red")
        axs[1, 1].plot(self.series[0].index, np.average(np.vstack(cluster[3]), axis=0), c="red")

        axs[0, 0].set_title(f"Cluster 0")
        axs[0, 1].set_title(f"Cluster 1")
        axs[1, 0].set_title(f"Cluster 2")
        axs[1, 1].set_title(f"Cluster 3")

        plt.savefig(self.plot_path + plot_name + ".png")
        plt.show()
        print("Plots drew successfully.")

    def smo_clusternig_base_on_real_data(self, plot_name):
        som_x = 2
        som_y = 2
        matrix_shape = (len(self.series[self.active_asset_id][0]), 4)
        som = MiniSom(som_x, som_y, len(self.series[self.active_asset_id]), sigma=1.0, learning_rate=0.2)
        som.random_weights_init(self.series[self.active_asset_id])
        som.train(self.series[self.active_asset_id], 100000)
        win_map = som.win_map(self.series[self.active_asset_id])
        # self.plot_som_based_on_real_dataset(som_x, som_y, win_map, som, plot_name)
        print(win_map)

    def plot_som_based_on_real_dataset_(self, som_x, som_y, win_map, som_model, plot_name):
        fig, axs = plt.subplots(som_x, som_y, figsize=(25, 25))
        fig.suptitle(f"Clustering {len(self.series[self.active_asset_id])} {self.assets[self.active_asset_id]['asset_name'].upper()} series of Stellar Dataset base on Self-Organized Mapping method.")


    def smo_clustering(self, plot_name):
        # som_x = som_y = math.ceil(math.sqrt(math.sqrt(len(self.my_series))))
        som_x = 2
        som_y = 2
        som = MiniSom(som_x, som_y, len(self.my_series[0]), sigma=1.0, learning_rate=0.2)#len(self.my_series[0])
        som.random_weights_init(self.my_series)
        som.train(self.my_series, 100000)
        win_map = som.win_map(self.my_series)
        self.plot_som_based_on_real_dataset(som_x, som_y, win_map, som, plot_name)
        # self.plot_som_series_averaged_center(som_x, som_y, win_map, plot_name)

    def plot_som_based_on_real_dataset(self, som_x, som_y, win_map, som_model, plot_name,):
        fig, axs = plt.subplots(som_x, som_y, figsize=(25, 25))
        # fig, axs = plt.subplots(figsize=(25, 25))
        fig.suptitle(f"Clustering {len(self.my_series)} {self.assets[self.active_asset_id]['asset_name'].upper()} series of  Stellar Dataset Base on Self-Organized Mapping method")

        avg_clusters = [
            [],
            [],
            [],
            [],
            []
        ]
        self.clusters_inventory_trade_ratio[self.active_asset_id] = [
            [],
            [],
            [],
            [],
            []
        ]
        self.clusters_protion_volume[self.active_asset_id] = [
            [],
            [],
            [],
            [],
            []
        ]
        clusters_plot_res = [
            [],
            [],
            [],
            [],
            []
        ]
        cluster_plot_color = [
            "red",
            "green",
            "blue",
            "black",
            "magenta"
        ]
        for i in range(0, len(self.my_series) - 1):
            node_cluster_number = som_model.winner(self.my_series[i])
            num_cluster = node_cluster_number[0] * 2**1 + node_cluster_number[1] * 2**0
            clusters_plot_res[num_cluster].append(self.series[self.active_asset_id][i].TV)
            axs[node_cluster_number].plot(self.series[self.active_asset_id][i].TV, c="gray", alpha=0.5)
            # Load behavioral attributes into matrix
            # Protion volume
            tmp = pd.DataFrame(self.protion_volume_matrix[self.active_asset_id].query(
                " source_account == '" + self.series[self.active_asset_id][i].source_account[0] + "'"
            ).values, columns=["source_account", "Date", "protion_volume"])
            tmp["Date"] = pd.to_datetime([dt for dt in tmp["Date"].squeeze().tolist()], format="%Y-%m-%d")
            self.clusters_protion_volume[self.active_asset_id][num_cluster].append(tmp)
            del tmp
            # Inventory/Trade ratio
            tmp = pd.DataFrame(self.inventory_trade_ratio_matrix[self.active_asset_id].query(
                " source_account == '" + self.series[self.active_asset_id][i].source_account[0] + "'"
            ).values, columns=["source_account", "Date", "inventory"])
            tmp["Date"] = pd.to_datetime([dt for dt in tmp["Date"].squeeze().tolist()], format="%Y-%m-%d")
            self.clusters_inventory_trade_ratio[self.active_asset_id][num_cluster].append(tmp)
            del tmp

            avg_clusters[num_cluster].append(self.series[self.active_asset_id][i].TV)
            axs[node_cluster_number].set_ylabel("Trade Volume")
            axs[node_cluster_number].set_xlabel("Date")
            yticklabels = pd.to_datetime([dt for dt in self.series[self.active_asset_id][0].index], format="%Y-%m-%d")
            axs[node_cluster_number].set_xticklabels(yticklabels, rotation=45)
        # for num_c, cluster in enumerate(clusters_plot_res):
        # for x in range(som_x):
        #     for y in range(som_y):
        #         cluster = (x, y)
        #         cluster_number = x * 2**1 + y * 2**0
        #         if len(avg_clusters[cluster_number]) > 1:
        #             axs[cluster].plot(np.average(np.vstack(avg_clusters[cluster_number]), axis=0),
        #                               c=cluster_plot_color[cluster_number], alpha=0.7)
            # axs[cluster].plot(np.average(np.vstack(win_map[cluster]), axis=0), c="red")

        #         axs[cluster].set_title(f"Cluster {cluster_number}")
        plt.savefig(self.plot_path + plot_name + ".png")
        plt.show()
        # self.plot_protion_volume_ratio(som_x, som_y)
        # self.plot_inventory_trade_ratio(som_x, som_y)

    def plot_protion_volume_ratio(self, som_x, som_y):
        fig, axs = plt.subplots(som_x, som_y, figsize=(25, 25))
        fig.suptitle(f" {self.assets[self.active_asset_id]['asset_name'].upper()} Protion Volume of  Stellar Dataset Base on Self-Organized Mapping method")
        #fig.tight_layout()
        for cluster_num, cluster in enumerate(self.clusters_protion_volume[self.active_asset_id]):
            if len(cluster) > 0:
                cluster_bi_num = self.convert_cluster_num_to_binary_tuple(cluster_num)
                cluster = pd.concat(cluster)
                cluster = cluster.set_index("Date")
                cluster["protion_volume"] = cluster["protion_volume"].apply(pd.to_numeric)
                mean_per_day = cluster.groupby(pd.Grouper(freq="d"))["protion_volume"].mean()
                axs[cluster_bi_num].plot(mean_per_day)
                axs[cluster_bi_num].set_xticklabels(mean_per_day.index, rotation=45)
        name = self.assets[self.active_asset_id]["asset_name"] + "_protion_volume.png"
        plt.savefig(self.plot_path + name)
        plt.show()

    def plot_inventory_trade_ratio(self, n_row, n_col):
        fig, axs = plt.subplots(n_row, n_col, figsize=(25, 25))
        fig.suptitle(f"{self.assets[self.active_asset_id]['asset_name'].upper()} Inventory/Trade ratio of Stellar Distributed Exchange Network")
        for cluster_num, cluster in enumerate(self.clusters_inventory_trade_ratio[self.active_asset_id]):
            if len(cluster) > 0:
                cluster = pd.concat(cluster)
                cluster = cluster.set_index("Date")
                cluster["inventory"] = cluster["inventory"].apply(pd.to_numeric)
                mean_per_day = cluster.groupby(pd.Grouper(freq="d"))["inventory"].mean()
                cluster_bi_num = self.convert_cluster_num_to_binary_tuple(cluster_num)
                axs[cluster_bi_num].plot(mean_per_day)
                axs[cluster_bi_num].set_xticklabels(mean_per_day.index, rotation=45)
        name = self.assets[self.active_asset_id]["asset_name"] + "_inventory_trade_ratio.png"
        plt.savefig(self.plot_path + name)
        plt.show()

    def convert_cluster_num_to_binary_tuple(self, num):
        if num == 0:
            return (0, 0)
        elif num == 1:
            return (0, 1)
        elif num == 2:
            return (1, 0)
        elif num == 3:
            return (1, 0)
        else:
            return (1, 1)

    def plot_som_series_averaged_center(self, som_x, som_y, win_map, plot_name):
        fig, axs = plt.subplots(som_x, som_y, figsize=(25, 25))
        fig.suptitle('Clusters')
        for x in range(som_x):
            for y in range(som_y):
                cluster = (x, y)
                if cluster in win_map.keys():
                    for series in win_map[cluster]:
                        axs[cluster].plot(series, c="gray", alpha=0.5)
                    axs[cluster].plot(np.average(np.vstack(win_map[cluster]), axis=0), c="red")
                cluster_number = x * som_y + y + 1
                axs[cluster].set_title(f"Cluster {cluster_number}")
        plt.savefig(self.plot_path + plot_name + ".png")
        plt.show()


    def affinity_propagation_clustering(self, plot_name, damping=0.9):
        print("Preparing Affinity Propagation method...")
        model = AffinityPropagation(damping=damping)
        model.fit(self.distance_matrics)
        cluster_centers = model.cluster_centers_indices_
        labels = model.labels_

        # yhats = model.predict(self.distance_matrics)
        clusters = len(cluster_centers)
        print("Affinity Propagation completed successfully.")
        print("preparing Affinity Propagation plot...")
        for cluster in clusters:
            # get row indexes for samples with this cluster
            row_ix = labels == cluster
            # create scatter of these samples
            plt.scatter(self.my_series[row_ix, 0], self.my_series[row_ix, 1])
        plt.savefig(self.plot_path + plot_name + ".png")
        # show the plot
        plt.show()







