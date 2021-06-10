import asyncio as asy
#from distance_class import DistaceMatrix
from clustering_class import Stellar_dataset_clustering

async def distance_matrics_and_hierachical_clustering():
#        distance_class = DistaceMatrix()
 #       distance_class.eth_btc_clustering()
	print("hello")

async def clustering():
        stellar_clustering = Stellar_dataset_clustering()
        stellar_clustering.process_handler()

loop = asy.get_event_loop()
loop.run_until_complete(clustering())
loop.close()
