import asyncio as asy
# from distance_class import DistaceMatrix
# from clustering_class import Stellar_dataset_clustering
from classes.kmeans_clustering import KMeans_clustering

async def distance_matrix_and_hierachical_clustering():
       distance_class = DistaceMatrix()
       distance_class.eth_btc_clustering()
# print("hello")

async def clustering():
        stellar_clustering = Stellar_dataset_clustering()
        stellar_clustering.process_handler()

async def kmeans():
    num_workers = 4
    num_iter = 5
    normalization_method = ["z_score", "min_max", "log", "log_10", "tanh", "MaxAbsScaler"]
    kmeans_ = KMeans_clustering("dtw", num_workers, normalization_method[5])
    kmeans_.load_dataset()
    for num_cluster in range(1, 3):
        kmeans_.k_means_and_plot_results(num_cluster, num_iter)
    kmeans_.plot_elbow_method("inertia")
    kmeans_.plot_elbow_method("distortion")
    # kmeans_.plot_kmeans_result(kmeans_.series[0], result["centroids"], result["closest"], 4)

loop = asy.get_event_loop()
#loop.run_until_complete(distance_matrix_and_hierachical_clustering())
# loop.run_until_complete(clustering())
loop.run_until_complete(kmeans())
loop.close()
