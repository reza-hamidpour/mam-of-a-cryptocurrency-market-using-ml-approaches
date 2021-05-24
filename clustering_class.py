from scipy.cluster.hierarchy import single, average, complete, ward, dendrogram
import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
import sys


class Stellar_dataset_clustering:
    dataset_path = "/home/reza/Clustering/dataset/"
    matrics_file_name = "distance_matrix_eth_btc_500_user.csv"
    distance_matrics = None
    plot_path = "/home/reza/Clustering/clustering_plots/"

    def process_handler(self):
        self.load_dataset()
        print("Loading dataset into clustering method")
        self.heirachical_clustering(self.distance_matrics, "complete", "complete_method_dendogram_500_users")

    def load_dataset(self):
        print("Loading dataset into Numpy... ")
        self.distance_matrics = np.loadtxt(self.dataset_path + self.matrics_file_name, 'float', delimiter=",")
        print("Data loaded successfully.")
        # print(self.distance_matrics)

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
        fig = plt.figure(figsize=(165, 90))
        dn = dendrogram(Z, leaf_rotation=90, color_threshold=1000, above_threshold_color="grey")
        # plt.axhline(y=6, color='r', linestyle='--')
        plt.savefig(self.plot_path + plot_name + ".png")
        plt.title(f" BTC AND ETH Dendogram plot with {method}.")
        plt.show()
        return Z

    def kmeans_clustering(self, plot_name):
        print("You should impliment this method.")