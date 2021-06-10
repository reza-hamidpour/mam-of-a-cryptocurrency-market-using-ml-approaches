from scipy.cluster.hierarchy import single, average, complete, ward, dendrogram
from tslearn.clustering import TimeSeriesKMeans
import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
import sys
import os
from minisom import MiniSom
import math
from sklearn.preprocessing import MinMaxScaler


class Stellar_dataset_clustering:
    dataset_path = "/home/reza/Clustering/Result_code/mam-of-a-cryptocurrency-market-using-ml-approaches/dataset/"
    path_dataset = "./Dataset"
    matrics_file_name = "separated_users_distance_matrix_eth_btc_all_users_normalized.csv"
    distance_matrics = None
    plot_path = "/home/reza/Clustering/Result_code/mam-of-a-cryptocurrency-market-using-ml-approaches/clustering_plots/smo_results/"
    num_series = 500
    users = []
    my_series = []

    def process_handler(self):
        # self.load_matrics()
        self.load_dataset()

        # print("Checking series...")
        # self.check_series()

        print("Loading dataset into clustering method")
        # self.heirachical_clustering(self.distance_matrics, "complete", "normalized_complete_method_dendogram_500_users")
        # self.smo_clustering("som_clustering_all_users_1-0_0-2")
        self.kmeans_clustering("kmeans_clustering_700_users_num_clu_4")

    def load_dataset(self):
        print("Gathering data phase... ")
        for dirname, filename, files in os.walk(self.path_dataset):
            for file in files:
                user = pd.read_csv(self.path_dataset + "/" + file, sep=",", header=[0, 1, 2, 3, 4, 5, 6])
                user_id = os.path.splitext(os.path.basename(file))[0]
                user.columns = ["id", "unixtime", "Date", "NT", "TV", "CII", "CNI", "asset_code"]
                user.drop('unixtime', inplace=True, axis=1)
                user.drop('id', inplace=True, axis=1)
                user.interpolate(limit_direction="both", inplace=True)
                user["source_account"] = user_id
                user["Date"] = pd.to_datetime([dt for dt in user["Date"].squeeze().tolist()], format="%Y-%m-%dT%H:%M:%S")
                user = user.set_index("Date")
                self.users.append(user.loc['2019-10-15':'2019-11-15'])
        self.users = pd.concat(self.users)
        self.users = self.users.reset_index()
        self.users = self.users.set_index(["Date", "source_account"])
        self.users = self.users.groupby("source_account")
        series = []
        for user in self.users:
            series.append(user[1].values)
        for i in range(len(series)):
            scaler = MinMaxScaler()
            tmp = MinMaxScaler().fit_transform(series[i])
            self.my_series.append(tmp.reshape(len(tmp), 5))
        self.my_series = self.my_series[:500]
        print(f"Number of series {len(self.my_series)}")
        print("Data gathered successfully.")

    def load_matrics(self):
        print("Loading dataset into Pandas... ")
        # self.distance_matrics = pd.Dataframe(np.loadtxt(self.dataset_path + self.matrics_file_name, 'float', delimiter=","))
        self.my_series = pd.read_csv(self.dataset_path + self.matrics_file_name, sep=",", header=None)
        self.my_series = self.my_series.iloc[:500,:500].values
        # self.distance_matrics = self.distance_matrics.interpolate(method="linear", limit_direction="forward")
        print("Data loaded successfully.")
        # print(self.distance_matrics)

    def check_series(self):
        default_len = len(self.my_series[0])
        bigger_series = []
        bigger_than_default = 0
        smaller_than_default = 0
        smaller_series = []
        k = 0
        for elm in self.my_series:
            if len(elm) > default_len:
                bigger_than_default += 1
                bigger_series.append(k)
            elif len(elm) < default_len:
                smaller_than_default += 1
                smaller_series.append(k)
            k += 1
        print(f"Default series length is {default_len}")
        print(f"Number of series which are bigger than default length are {bigger_than_default}")
        print(bigger_series)
        print(f"Number of series which are smaller than default length are {smaller_than_default}")
        print(smaller_series)

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
        # plt.axhline(y=6, color='r', linestyle='--')
        plt.savefig(self.plot_path + plot_name + ".png")
        print(f"Plot {plot_name} saved.")
        plt.title(f" BTC AND ETH Dendogram plot with {method}.")
        plt.show()
        return Z

    def kmeans_clustering(self, plot_name):
        print("Preparing KMeans parameters...")
        n_clusters = 4#My suggest 4
        # n_clusters = math.ceil(math.sqrt(len(self.my_series)))

        km = TimeSeriesKMeans(n_clusters=n_clusters, metric="dtw", verbose=True, n_jobs=8)
        labels = km.fit_predict(self.my_series)
        print("KMeans finished successfully.")
        self.kmeans_plot(labels, n_clusters, plot_name)

    def kmeans_plot(self, labels, n_clusters, plot_name):
        print("Preparing cluster plots...")
        som_y = math.ceil(math.sqrt(math.sqrt(len(self.my_series))))
        plot_count = math.ceil(math.sqrt(n_clusters))

        fig, axs = plt.subplots(plot_count, plot_count, figsize=(25,25))
        fig.suptitle("Clustering 500 users in Stellar network")
        row_i = 0
        column_j = 0
        cluster = []
        for label in set(labels):
            for i in range(len(labels)):
                if labels[i] == label:
                    axs[row_i,column_j].plot(self.my_series[i], c="gray", alpha=0.4)
                    cluster.append(self.my_series[i])
            if len(cluster) > 0:
                axs[row_i, column_j].plot(np.average(np.vstack(cluster), axis=0), c="red")
            axs[row_i, column_j].set_title(f"Cluster {str(row_i * som_y + column_j)}")
            column_j += 1
            if column_j % plot_count == 0:
                row_i += 1
                column_j =0
        plt.savefig(self.plot_path + plot_name + ".png")
        plt.show()
        print("Plots drew successfully.")

    def smo_clustering(self, plot_name):
        som_x = som_y = math.ceil(math.sqrt(math.sqrt(len(self.my_series))))
        som = MiniSom(som_x, som_y, len(self.my_series), sigma=1.0, learning_rate=0.2)#len(self.my_series[0])
        som.random_weights_init(self.my_series)
        som.train(self.my_series, 100000)
        win_map = som.win_map(self.my_series)
        self.plot_som_series_averaged_center(som_x, som_y, win_map, plot_name)

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


