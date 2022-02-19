# NFT Asset Price Prediction and Analysis with Graphs

Laszlo Albert Barabási wrote in one of his books ([The Formula, 2018](https://www.goodreads.com/book/show/39088545-the-formula)): 

> “Performance drives success, but when performance can’t be measured, networks drive success”.
> 

So let’s use this idea on [OpenSea’](https://opensea.io/)s NFT dataset, where all the transactions are public. So even if we cannot decide the value of an NFT for the first sight, we might be able to predict its future value from its earlier owners, creators, etc. 

# Content

This GitHub page contains the following folders.

## Notebooks on the Main Folder

These notebooks can be used for running all the ETL codes, exploring the collections, selecting the NFTs you would like to buy, etc. (will be uploaded soon, the development has just started). At the beginning of the file, you can select your environment (single server or distributed), so it will choose the right functions (from the src folder to run).

## Data Ingestion - API related codes

This folder contains all the codes which download the transactions, and the asset/collection descriptions to the staging area (will be uploaded soon, the development has just started). Thanks to [OpenSea](https://opensea.io/) for sharing all of it. Also, they have a [detailed API reference doc](https://docs.opensea.io/reference/api-overview).

![https://storage.googleapis.com/opensea-static/Logomark/OpenSea-Full-Logo%20(dark).png](https://storage.googleapis.com/opensea-static/Logomark/OpenSea-Full-Logo%20(dark).png)

## Single Server Solution

If you would like to try out these codes on a single server (probably in your PC/notebook), you can use these codes. These are not always the most effective ones, but you are able to run them with lower memory usage - in a low-performance environment, using just Anaconda for instance (neither pyspark knowledge nor access to GCP is necessary).

## GCP version

If you would like to run the codes on a distributed system (AWS or GCP), you can use these libraries. In function, these are totally the same as the “single server” ones, but these are faster, and you can process the full dataset at once. 

# How to Use

You can download the notebooks to the folder you’d like to use them, and create an src subfolder to the same place. Download all the .py files to the src folder (from the single server solution or the GCP version), and you are ready to go. Some tutorial videos will be shared soon with the details.

# Questions to Answer (with the analyses)

A detailed methodology document will arrive. Till then, here is the list of ideas that are implemented in the notebooks or in [LynxKite](https://lynxkite.com/) (most of the graph-related or GCN-based parts).

[https://repository-images.githubusercontent.com/251277949/d0add180-7294-11ea-8ee3-6ca69fddb879](https://repository-images.githubusercontent.com/251277949/d0add180-7294-11ea-8ee3-6ca69fddb879)

## Following Successful Traders

Analyzing traders and creating success KPIs (e.g., realized profit till date w/wo holding). Examine the collections or minters: which are those, which are popular among the successful traders. Will these tokens be worth a lot in the future?

Some of the traders can be handled as galleries. The question is if there is any gallery that guarantees success? (Barabási-Albert Laszlo’s idea)

## Following the Influencers

Are there any traders who affect the price of the NFT (or the price of the other NFTs in the collection)? Basically in this analysis, we are checking the price-level increments among the collections (with some filter, like not only 1-time jump) and if there is a verified jump, we mark the traders who bought from them a bit before.

Summarising these flags over time could lead us to the influencers. The analysis is the same from here: does it worth buying from the same collections/minters as these influencers?

The influencer score could be also made on finding jumps on the minters’ historical data.

## Earlier Pieces of Recently Discovered Minters

Check how the earlier pieces’ prices are changing of the recently discovered minters. Does it worth buying the recently discovered minters’ earlier collections? (Also, is it possible to predict, which minters will be hyped?)

## New Pieces of Famous Artists (minters)

If an artist is already accepted, will their later work be also worth a lot? Does it worth buying them?

## Trader - NFT graph

Visualize the trader NFT graph (till time), and also add the future values of some NFTs. Try to create some graph community-based models. This graph can be also the base of the NFT similarity graph, where graph convolutional networks can be tried out (for current/future value estimation).

## Buyer - Seller graph

The transactional graph could help to identify the fraud networks (some traders with huge losses make some minters rich). Also, some other interesting patterns might be found. By using the ether-tracker / the Google Cloud data, some full fraud chains could be explored from the cracked wallets to the minters. (However, I am not sure of their existence, there are easier ways for money laundering in the blockchain world). The found suspicious minters/traders can be handled in the data for increasing the accuracy.

## NFT similarity graph

The nodes would be the NFTs and the edges would represent the similarity (filtered over a value). Using a Graph Convolutional Network on this graph can help to estimate the value of the NFTs (current and future). If an estimated value is much higher than the original one, the NFT might worth to be bought.

## Drivers of Asset Value

Applying Machine Learning to all features for estimating the future price of an asset (NFT) could also show which drivers are the most important ones (SHAP analysis for instance). 

Also, this analysis would use all the mentioned ideas above for predicting the future value (with different timeframes) of an NFT. The analysis should also give some lower value where the token can be sold with a given confidence.

## How Social Networks Influences Prices

Checking galleries (web-page), social network communities/platforms (like Twitter or Reddit) that influence the price of the future mints. Try to find out which future NFT to buy. Rarity scores should be checked as well.