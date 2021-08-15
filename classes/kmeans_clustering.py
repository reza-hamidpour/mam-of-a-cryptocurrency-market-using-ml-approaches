import numpy as np
import pandas as pd
import os
from sklearn.preprocessing import MinMaxScaler
from dtw import *
from multiprocessing import Manager, Queue, Process


class KMeans_clustering:
    path_dataset = "./dataset/"
    path_dataset_sample = "./dataset_small_sample/"
    users = []
    series = [[], []]
    btc_users = None
    eth_users = None
    SENTINEL = "END"

    def __init__(self, metric="dtw", num_workers=1):
        self.metric = metric
        self.num_workers = num_workers
        print(f"Kmeans Clustering loaded by {metric} distance metric and {num_workers} Workers...")

    def load_dataset(self):
        print("Gathering data phase... ")
        for dirname, filename, files in os.walk(self.path_dataset_sample):
            for file in files:
                user_id = os.path.splitext(os.path.basename(file))[0]
                user = pd.read_csv(self.path_dataset_sample + "/" + file, sep=",", header=[0, 1, 2, 3, 4, 5, 6, 7])
                user.columns = ["id", "Date", "NT", "TV", "CII", "CNI", "inter_trade", "asset_code"]
                user.drop('id', inplace=True, axis=1)
                user["source_account"] = user_id
                user["Date"] = pd.to_datetime([dt for dt in user["Date"].squeeze().tolist()],
                                              format="%Y-%m-%dT%H:%M:%S")
                user = user.set_index("Date")
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
        for user in self.btc_users:
            tmp = user[1].reset_index()
            tmp_ = tmp[["Date", "NT", "TV", "CII", "CNI", "inter_trade"]]
            # scaler = MinMaxScaler()
            # tmp_[["NT", "TV", "CII", "CNI"]] = scaler.fit_transform(tmp_[["NT", "TV", "CII", "CNI"]])
            # tmp_ = tmp_.set_index("Date")
            self.series[0].append(tmp_)  # BTC Dataset
        self.series[0] = np.array(self.series[0])

        for user in self.eth_users:
            tmp = user[1].reset_index()
            tmp_ = tmp[["Date", "NT", "TV", "CII", "CNI", "inter_trade"]]  # , "source_account"
            # scaler = MinMaxScaler()
            # tmp_[["NT", "TV", "CII", "CNI"]] = scaler.fit_transform(tmp_[["NT", "TV", "CII", "CNI"]])
            # tmp_ = tmp_.set_index("Date")
            self.series[1].append(tmp_)  # ETH Dataset
        self.series[1] = np.array(self.series[1])

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
                    cen = pd.DataFrame(centroid, columns=["Date", "NT", "TV", "CII", "CNI", "inter_trade"])
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
        print(f"Calc centroids finished.")
        return np.array(dist)

    def closest_centroids(self, data, centroids):
        dist = self.calc_centroids(data, centroids)
        return np.argmin(dist, axis=1)

    def move_centroids(self, data, closest, centroids):
        k = centroids.shape[0]

        new_centroids_dataframe = []
        series = []
        iter = 0
        for c in np.unique(closest):
            for _, serie in enumerate(data[closest == c]):
                series.append(
                    pd.DataFrame(serie, columns=["Date", "NT", "TV", "CII", "CNI", "inter_trade"]))
            series = pd.concat(series)
            tmp = series.groupby(pd.Grouper(key="Date", freq="900s")).mean()
            series = tmp
            new_centroids_dataframe.append(series)
            iter += 1
        print(new_centroids_dataframe[0])
        print(np.array(new_centroids_dataframe[0]).shape)
        quit()
        new_centroids = np.array([data[closest == c][:, :, 1:].mean(axis=0) for c in np.unique(closest)])
        print(f"new_centroids shape : {new_centroids.shape}")

        if k - new_centroids.shape[0] > 0:
            print("adding {} centroid(s)".format(k - new_centroids.shape[0]))
            additional_centroids = data[np.random.randint(0, len(data), k - new_centroids.shape[0])]
            print(f"additional_centroids shape : {additional_centroids.shape}")
            new_centroids = np.append(new_centroids, additional_centroids, axis=0)
        quit
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

    def k_means(self, data, num_clust, num_iter):
        # centroids = self.init_centroids(data, num_clust) K-Means++

        centroids = []

        for _, i in enumerate(np.random.randint(0, data.shape[0], num_clust)):
            centroids.append(data[i])

        centroids = np.array(centroids)

        last_centroids = centroids
        for n in range(num_iter):
            print(f"Iteration {n}")
            closest = self.closest_centroids(data, centroids)
            centroids = self.move_centroids(data, closest, centroids)
            if not np.any(last_centroids != centroids):
                print("early finish!")
                break
            last_centroids = centroids
        return {"centroids": centroids, "closest": closest}
