## Data preprocessing
## In this file we did some preprocessing operations on ower gathered data.

## 1- Extracting 3 buckets from our data ( operations ) 
    1-1 BTC bucket : All transactions which have btc asset on at least one side of offers.
    1-2 ETH bucket : All transactions which have eth asset on at least one side of offers.
    1-3 native bucket: All transactions which have native asset on at least one side of offers.
## 2- Computing these attributes from our buckets:
    1- Number of transactions per each user on each time window.
    2- Volume of transactions per each user on each time window.
    3- Change of inventory : The difference between opening offers and
    closing offers of each user on each time window.
    4- Cumulative net inventory : *
## 3- Data Normalization :
    x' = {
            x > 0       log(x),
            x < 0       -log(-x)
         }
## 4- Matrix creation per user: 
    Getting all data from our attributes collection per user and creating a csv file per user.
