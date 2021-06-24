import matplotlib.pylab as plt
import numpy as np
import random
from multiprocessing import Process, Queue, Manager
import asyncio
from datetime import datetime

class ts_cluster(object):
    SENTINEL = "END"

    def __init__(self, num_clust):
        '''
        num_clust is the number of clusters for the k-means algorithm
        assignments holds the assignments of data points (indices) to clusters
        centroids holds the centroids of the clusters
        '''
        self.num_clust = num_clust
        self.assignments = {}
        self.centroids = []


    def k_means_clust_mp(self, data, num_iter, w, progress=False, n_workers=1):
        mangers = Manager()
        self.centroids = random.sample(data, self.num_clust)

        #producer and consumer
        for n in range(num_iter):
            qu = Queue()
            producer = Process(target=self.task_producer, args=(data, qu, n_workers))
            producer.start()

            consumers = []
            assigned_data_qu = Queue()
            for i in range(n_workers):
                    consumers.append(
                        Process(target=self.task_consumer, args=(qu, assigned_data_qu, w, i))
                    )
                    consumers[i].start()
            if progress:
                print(f"Iteration {n}, {n_workers} workers started at {datetime.now()} ...")
            qu.close()
            qu.join_thread()

            producer.join()

            flag = True
            while flag:
                data = assigned_data_qu.get()
                if "data_index" in data.keys():
                    if data["data_index"] == self.SENTINEL:
                        flag = False
                        if progress:
                            print("Task %d finished." % data['consumer_id'])
                        continue
                    if data["closest_cent_index"] not in self.assignments:
                        self.assignments[data["closest_cent_index"]] = []
                    self.assignments[data["closest_cent_index"]].append(data["data_index"])

            for k in range(n_workers):
                consumers[k].terminate()

            del flag, producer, qu, assigned_data_qu, consumers, producer

            if progress:
                print("Recalculating centroids...")

            for key in self.assignments:
                clust_sum = 0
                for k in self.assignments[key]:
                    clust_sum = clust_sum + data[k]
                self.centroids[key] = [m / len(self.assignments[key]) for m in clust_sum]
            if progress:
                print(f"Iteration {n} finished at {datetime.now()}")

    def task_producer(self, data, qu, num_workers):
        for ind, i  in enumerate(data):
            qu.put({
                "data": [i],
                "data_index": ind
            })
        for i in range(num_workers):
            qu.put({
                "data": self.SENTINEL
            })

    def task_consumer(self, queue, assignment_queue, w, consumer_id):
        while True:
            data = queue.get()
            if data["data"] != self.SENTINEL:
                print(f"{consumer_id} is working")
                min_dist = float('inf')
                closest_clust = None
                for c_ind, centroid in enumerate(self.centroids):
                    if self.LB_Keogh(data["data"][0], centroid, 5) < min_dist:
                        cur_dist = self.DTWDistance(data["data"][0], centroid, w)
                        if cur_dist < min_dist:
                            min_dist = cur_dist
                            closest_clust = c_ind
                assignment_queue.put({
                    "data_index": data["data_index"],
                    "closest_cent_index": closest_clust,
                    "distance": min_dist
                })

            elif data["data"] == self.SENTINEL:
                del cur_dist, min_dist, closest_clust, c_id, j, cent
                assignment_queue.put({
                    "data_index": self.SENTIEL,
                    "consumer_id": consumer_id,
                })
                break

    def k_means_clust(self, data, num_iter, w, progress=False):
        '''
        k-means clustering algorithm for time series data.  dynamic time warping Euclidean distance
         used as default similarity measure.
        '''
        self.centroids = random.sample(data, self.num_clust)

        for n in range(num_iter):
            if progress:
                print('iteration ' + str(n + 1))
            # assign data points to clusters
            self.assignments = {}
            for ind, i in enumerate(data):
                min_dist = float('inf')
                closest_clust = None
                for c_ind, j in enumerate(self.centroids):
                    if self.LB_Keogh(i, j, 5) < min_dist:
                        cur_dist = self.DTWDistance(i, j, w)
                        if cur_dist < min_dist:
                            min_dist = cur_dist
                            closest_clust = c_ind
                if closest_clust in self.assignments:
                    self.assignments[closest_clust].append(ind)
                else:
                    self.assignments[closest_clust] = []

            # recalculate centroids of clusters
            for key in self.assignments:
                clust_sum = 0
                for k in self.assignments[key]:
                    clust_sum = clust_sum + data[k]
                self.centroids[key] = [m / len(self.assignments[key]) for m in clust_sum]

    def get_centroids(self):
        return self.centroids

    def get_assignments(self):
        return self.assignments

    def plot_centroids(self):
        for i in self.centroids:
            plt.plot(i)
        plt.show()

    def DTWDistance(self, s1, s2, w=None):
        '''
        Calculates dynamic time warping Euclidean distance between two
        sequences. Option to enforce locality constraint for window w.
        '''
        DTW = {}

        if w:
            w = max(w, abs(len(s1) - len(s2)))

            for i in range(-1, len(s1)):
                for j in range(-1, len(s2)):
                    DTW[(i, j)] = float('inf')

        else:
            for i in range(len(s1)):
                DTW[(i, -1)] = float('inf')
            for i in range(len(s2)):
                DTW[(-1, i)] = float('inf')

        DTW[(-1, -1)] = 0

        for i in range(len(s1)):
            if w:
                for j in range(max(0, i - w), min(len(s2), i + w)):
                    dist = (s1[i] - s2[j]) ** 2
                    dtw_list = [DTW[(i - 1, j)], DTW[(i, j - 1)], DTW[(i - 1, j - 1)]]
                    min_value = dtw_list[np.argmin(dtw_list)]
                    for dist_val in dist:
                        min_value += dist_val
                    DTW[(i, j)] = min_value
            else:
                for j in range(len(s2)):
                    dist = (s1[i] - s2[j]) ** 2
                    dtw_list = [DTW[(i - 1, j)], DTW[(i, j - 1)], DTW[(i - 1, j - 1)]]
                    min_value = dtw_list[np.argmin(dtw_list)]
                    for dist_val in dist:
                        min_value += dist_val
                    DTW[(i, j)] = min_value

        return np.sqrt(DTW[len(s1) - 1, len(s2) - 1])

    def LB_Keogh(self, s1, s2, r):
        '''
        Calculates LB_Keough lower bound to dynamic time warping.Linear
        complexity compared to quadratic complexity of dtw.
        '''
        LB_sum = 0

        for ind, i in enumerate(s1):
            # lower_bound = min(s2[(ind - r if (ind - r) >= 0 else 0):(ind + r)])
            # upper_bound = max(s2[(ind - r if (ind - r) >= 0 else 0):(ind + r)])
            lower_bound = s2[np.argmin(s2[(ind - r if (ind - r) >= 0 else 0):(ind + r)])]
            upper_bound = s2[np.argmax(s2[(ind - r if (ind - r) >= 0 else 0):(ind + r)])]

            if (i > upper_bound).all():
                LB_sum = LB_sum + (i - upper_bound).all() ** 2
            elif (i < lower_bound).all():
                LB_sum = LB_sum + (i - lower_bound).all() ** 2

        return np.sqrt(LB_sum)

# def LB_Keogh(self, s1, s2, r):
# 	'''
# 	Calculates LB_Keough lower bound to dynamic time warping. Linear
# 	complexity compared to quadratic complexity of dtw.
# 	'''
# 	LB_sum = 0
# 	for ind, i in enumerate(s1):
#
# 		lower_bound = min(s2[(ind-r if ind-r >= 0 else 0):(ind+r)])
# 		upper_bound = max(s2[(ind-r if ind-r >= 0 else 0):(ind+r)])
#
# 		if i > upper_bound:
# 			LB_sum = LB_sum+(i-upper_bound)**2
# 		elif i < lower_bound:
# 			LB_sum = LB_sum+(i-lower_bound)**2
#
# 	return np.sqrt(LB_sum)
