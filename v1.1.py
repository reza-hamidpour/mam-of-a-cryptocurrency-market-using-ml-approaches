import asyncio as asy
from distance_class import DistaceMatrix
from clustering_class import Stellar_dataset_clustering
from classes.kmeans_clustering import KMeans_clustering

async def distance_matrix_and_hierachical_clustering():
       distance_class = DistaceMatrix()
       distance_class.eth_btc_clustering()
# print("hello")

async def clustering():
        stellar_clustering = Stellar_dataset_clustering()
        stellar_clustering.process_handler()

async def kmeans():
    kmeans_ = KMeans_clustering()
    kmeans_.load_dataset()
    kmeans_.k_means(kmeans_.series[0], 5, 1000)

loop = asy.get_event_loop()
#loop.run_until_complete(distance_matrix_and_hierachical_clustering())
# loop.run_until_complete(clustering())
loop.run_until_complete(kmeans())
loop.close()
