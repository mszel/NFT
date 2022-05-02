# NFT Asset Price Prediction and Analysis with Graphs

Laszlo Albert Barabási wrote in one of his books ([The Formula, 2018](https://www.goodreads.com/book/show/39088545-the-formula)): 

> “Performance drives success, but when performance can’t be measured, networks drive success”.
> 

So let’s use this idea on [OpenSea’](https://opensea.io/)s NFT dataset, where all the transactions are public. So even if we cannot decide the value of an NFT at the first sight, we might be able to predict its future value from its earlier owners, creators, etc. We will try out several further techniques, and at the very end, suggest an optimal portfolio for a given capital (considering risk, etc.). 

# Content

This GitHub page contains notebooks in the main folder that substitute the usual “main” function: you can call all the major functions from there, like downloading NFT data using OpenSea API, running the ETLs, running the analysis, or optimizing the investments.

The majority of the functions are written both in PANDAS and PYSPARK (this can be set at the beginning of the notebooks - so the right functions will be imported).

The codes are W.I.P. - so none of the notebooks are final, as well as many of them are missing. Currently, only the PANDAS mode can be used (but it’ll be changed soon).

Thanks to [OpenSea](https://opensea.io/) for sharing their data and providing a [detailed API reference doc](https://docs.opensea.io/reference/api-overview).

![https://storage.googleapis.com/opensea-static/Logomark/OpenSea-Full-Logo%20(dark).png](https://storage.googleapis.com/opensea-static/Logomark/OpenSea-Full-Logo%20(dark).png)

## Notebooks on the Main Folder

As above mentioned, these notebooks are running all of the functions. Some of them are cores (downloading data, running ETLs, executing analysis, and optimizing portfolio), while others are exploring the data.

The notebooks are numbered in the order of execution. The notebooks themselves contain the instructions about how to run them (markdown notes).

If you are using your own notebook (and do not have pyspark there), you can use the “PANDAS” running mode. It will run slower - as you can run it in smaller batches. If you’re installing a GCP environment, then it is better to select the “PYSPARK” mode at the beginning.

## ./src subfolder

This folder contains all the codes which are used by the main notebooks:

- **config.py**: contains the paths of the input/output folders + some runtime parameters (in the development phase, the values are mainly hardcoded to the notebook, later on these will be cleansed)
- **config_API.py**: the [OpenSea API keys](https://docs.opensea.io/reference/request-an-api-key) should be updated here. If a user has several API keys, more can be added (and later on the loaders will parallelize the API calling).
- **util.py**: contains functions that are used by several loaders.
- **api_dl.py**: downloading Raw NFT data from OpenSea (calling the API). It saves the JSON files to pickle files.
- **ETL_00_rawToStage.py**: wrapping out the downloaded JSON files (saved to pickle) and saving them to partitioned parquet files to the stage area (the tokens and the collections are held redundantly - as only the next loader unify them).
- **ETL_01_stageToNDS.py**: loading the token, collection, and the transaction tables to the Normalized Data Store (NDS). It solves some of the data issues (duplicated transactions, seller/buyer address related anomalies) as well.
- **ETL_02_analyticsDM.py:** creating an analytics data mart from the normalized data store (which will be the base of all the codes). It contains multiple in-between layers, as some of the DM tables are depending on each other.
- **EXPL_00_visualization.py**: contains functions used for visualization.
- **other**: the analytics/kite codes will be held here as well, the first versions will be shared soon.

# Single Server vs. GCP Running mode

As mentioned above, in some of the notebooks, the way of running can be selected: PANDAS - for a single server without pyspark, and PYSPARK for running the codes on GCP.

Some of the functions (like ETL_01_stageToNDS.py) exist in 2 versions (one with a _pyspark postfix), and during the import, the notebook selects the right source file for the running mode. As the function names are the same (just the implementation is different), nothing should be changed in the notebook while switching from a single server to GCP.

# How to Use

You can clone the repository and “download” (pull) the notebooks and the ./src subfolder. After you can run the notebooks one by one, following the instructions there (currently W.I.P., so just some of the notebooks are working).

# Methodology (brief summary of the ideas behind)

A detailed methodology document will be provided later. Here you can read a list of ideas that are implemented (or under implementation) in the notebooks and in [LynxKite](https://lynxkite.com/) (a GCN-able graph analytics tool with a comfortable UI). These ideas will be the base of future price prediction.

[https://repository-images.githubusercontent.com/251277949/d0add180-7294-11ea-8ee3-6ca69fddb879](https://repository-images.githubusercontent.com/251277949/d0add180-7294-11ea-8ee3-6ca69fddb879)

## Trader Statistics - basics

Success KPIs can be created for the traders by their past transactions (e.g., realized profit till date w/wo holding, etc.). The impact on the NFT’s future price should be examined if a “successful” trader buys in from the same collection or from the same minter.

## Trader Statistics - galleries and influencers

The traders can be handled as galleries, as the OpenSea users can browse the holdings of each collector. The question is if there is any gallery that guarantees success? (Barabási-Albert Laszlo’s idea).

Also, an influence graph can be made across traders (if a trader buys from a collection/minter, and another follows within a time frame, we can add a directed influence edge between the two traders). Influence metrics can be calculated in this graph. The price change of NFTs, after a big influencer buys from the same collection/minter should be examined.

## Minter Statistics - basics

Success KPIs can be created for the minters as well. The impact of a recent success should be examined for the minter’s earlier work. Also, past success should affect the price of future works (which will be checked as well). 

## Graph-based metrics

Trader graphs (buyer/seller) based on transactions could be made. It can be both used for adding some further features to the trader KPI table, and for identifying some fraud networks (when a group of traders makes an artificial hype, or when the NFT is used for money laundering - after cracking some wallets). Using this information can be useful for all the other analyses. 

On a trader - NFT bipartite graph, some community-based features could be derived for the NFTs which could be important for their future value prediction.

An NFT similarity graph (e.g., had common traders or minters, belongs to the same collection, sharing several features - derived by computer vision, etc.) could be used as a base of GCN for estimating the real value of each NFT (and for identifying possible arbitrage events).

## Collection/Minter Trajectories

Typical patterns can be identified by analyzing the time series of the average (median, top/bottom decile, etc.) value of a collection. After the first months of movements, predictions can be made for future values.

## Unifying Analyses, Identifying the Real Value Drivers

Applying Machine Learning to all features for estimating the future price of an asset (NFT) could also show which drivers are the most important ones (SHAP analysis for instance). 

Also, this analysis would use all the mentioned ideas above for predicting the future value (with different timeframes) of an NFT. The analysis should also give some lower value where the token can be sold with a given confidence.

# Ideas for later

In a second phase, the following can be discovered:

- **Pre-launching price prediction of collections**: analyzing the collection’s social network activity (Twitter, Instagram, Discord, etc.) and their minters/creators’ earlier projects.