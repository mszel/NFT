##################################################################
#            ETL: Creating some DM tables and reports            #
##################################################################
#                                                                #
# Questions to marton.szel@lynxanalytics.com                     #
# Version: 2022-01-31                                            #
##################################################################

# Ideas: DM tables
# - coin prices, transactions, etc. up to a certain date (and maybe add a future date as well)
# - to the coins: until xxxxx date how many transactions they had --> how is it refletc to the future value?
#   (probably more transaction shows better future performance)

# The tables can be built from the coin table as a base, left join with the transaction table
# (up to a certain date)

# Ideas: collection and author tables (compare these metrics on collections, 
# also, create some similar to authors) - these can be also added to the analysis
# - how many % of the collection is sold (at least once)
# - how fast the first 1000 transactions happened
# - how fast the first 1000 tokens were sold
# - AVG price increment after first sales


# before export (maybe before table creation): REMOVE those NFT tokens, authors and transactions which only appears once
# - only the single cases: authors with only 1 NFT transactions where the buyer also only had that transactions 
# (and the NFT as well and the NFT's collection is not famous?)
# be careful, not to affect the simulation results (shitty collection with 1 famous piece)

# somehow calculate influence score ... somebody buys from a collection and popularity increasing?
# csinalni egy moving average idosort a collection salesekre, es ahol van egy peak, az azelotti trx-eket nezni

##################################################################
#                        Import libraries                        #
##################################################################

# loading ETL related libraries
import pandas as pd
import numpy as np
import csv

# core libraries
from datetime import datetime, timedelta
import imp
import time

# OS related
from os import listdir, makedirs, remove, path

# utility functions
from .util import *


##################################################################
#                        Helper Functions                        #
##################################################################

def _readTrx_betweenMonats(path_folder_in, load_interval_intmonat=[], load_max_intmonat=0):
    """
    Loading transaction files from a folder: 
     - in an interval: if the load_interval_intmonat is filled with 2 integers: [yyyymm, yyyymm]
     - up to a date: if the load_max_intmonat is filled with an integer: yyyymm
    """

    if (len(load_interval_intmonat) > 0):
        _loaded = [pd.read_pickle(path_folder_in + "trx_{}.pickle".format(_item)) for _item in _monat_list_creator(load_interval_intmonat, 0)]
    else:
        _loaded = [pd.read_pickle(path_folder_in + "trx_{}.pickle".format(_item)) for _item in _collectMonatsFromFilenames(path_folder_in, load_max_intmonat)]
    
    return pd.concat(_loaded, axis=0).reset_index(drop=True)


##################################################################
#                           Table Loaders                        #
##################################################################

def _tempL_NFT_User_pairs_mthly(pathfolder_trx, pathfolder_nftuser, actmonat):
    """
    Reads the previous month's table, and the actual month's transactions, and calcualtes 
    the month-end state of NFT holding. Contains all the NFT - user pairs which were existing 
    during the period. At the first month, it does not append to the previous mth-end table.

    """

    # reading transactions from the checked month
    pdf_trx_mth = pd.read_pickle(pathfolder_trx + "trx_{}.pickle".format(actmonat))
    pdf_trx_mth = pdf_trx_mth[((pdf_trx_mth.trx_duplication_flg == 0) & 
                               (~pdf_trx_mth.trx_value_usd.isnull()))].sort_values(by=['token_category', 'collection_ID', 'token_ID', 'trx_time_sec'])
    pdf_trx_mth['nft_first_sell_dt'] = pdf_trx_mth.groupby(['token_category', 'collection_ID', 'token_ID'])['trx_time_sec'].transform('first')
    pdf_trx_mth['possible_mint'] = np.select([pdf_trx_mth.nft_first_sell_dt == pdf_trx_mth.trx_time_sec], [1], default=0)
    pdf_trx_mth['possible_mint_usd'] = np.select([pdf_trx_mth.nft_first_sell_dt == pdf_trx_mth.trx_time_sec], [pdf_trx_mth.trx_value_usd], default=0)

    # separating buyers and sellers, flagging the first transactions (maybe a mint)
    pdf_tmp_sellers = pdf_trx_mth.sort_values(by=['token_category', 'collection_ID', 'token_ID', 'trx_seller_address', 'trx_time_sec']).groupby(
        ['token_category', 'collection_ID', 'token_ID', 'trx_seller_address']).agg(
        nftu_sell_date_latest = ('trx_time_sec', 'last'), nftu_sell_price_usd_latest = ('trx_value_usd', 'last'), 
        nftu_sell_amt_usd_sum = ('trx_value_usd', 'sum'), nftu_sell_cnt = ('trx_value_usd', 'count'), 
        nftu_possible_mint_flg = ('possible_mint', 'sum'), nftu_possible_mint_usd = ('possible_mint_usd', 'sum')).reset_index().rename(columns={'trx_seller_address':'trader_ID'})
    
    pdf_tmp_buyers = pdf_trx_mth.sort_values(by=['token_category', 'collection_ID', 'token_ID', 'trx_buyer_address', 'trx_time_sec']).groupby(
        ['token_category', 'collection_ID', 'token_ID', 'trx_buyer_address']).agg(
        nftu_buy_date_latest = ('trx_time_sec', 'last'), nftu_buy_price_usd_latest = ('trx_value_usd', 'last'), 
        nftu_buy_amt_usd_sum = ('trx_value_usd', 'sum'), nftu_buy_cnt = ('trx_value_usd', 'count')).reset_index().rename(
        columns={'trx_buyer_address':'trader_ID'})
    
    # joining aggregated tables, forming the extension table
    pdf_traderbase = pd.merge(pdf_tmp_sellers, pdf_tmp_buyers, how='outer', 
                              on=['token_category', 'collection_ID', 'token_ID', 'trader_ID'])
    pdf_traderbase['nftu_hold_flg'] = np.select([(pdf_traderbase.nftu_sell_date_latest.isnull()), 
                                                 (pdf_traderbase.nftu_buy_date_latest > pdf_traderbase.nftu_sell_date_latest)], 
                                                [1, 1], default=0)
    pdf_traderbase = pdf_traderbase.fillna({'nftu_possible_mint_flg':0, 'nftu_possible_mint_usd':0, 'nftu_sell_cnt':0, 'nftu_buy_cnt':0,
                                            'nftu_sell_amt_usd_sum':0, 'nftu_buy_amt_usd_sum':0})
    
    # defining the structure of the new table
    _kcid = ['token_category', 'collection_ID', 'token_ID', 'trader_ID']
    _kc = ['nftu_hold_flg', 'nftu_sell_date_latest', 'nftu_sell_cnt', 'nftu_sell_amt_usd_sum', 
           'nftu_sell_price_usd_latest', 'nftu_buy_date_latest', 'nftu_buy_price_usd_latest', 'nftu_buy_cnt', 
           'nftu_buy_amt_usd_sum', 'nftu_mint_flg', 'nftu_mint_usd']
    _feature_rename_dict = dict([(_item, _item + "_n") for _item in _kc])
    
    # checking if there is a previous table
    if path.exists(pathfolder_nftuser + "nftuser_{}.pickle".format(_monat_add(actmonat, -1))):

        # reading the old table and merge with the new one
        old_nftu_df = pd.read_pickle(pathfolder_nftuser + "nftuser_{}.pickle".format(_monat_add(actmonat, -1)))
        old_nftu_df = old_nftu_df[old_nftu_df.nftu_hold_flg > 0].copy() # keeping only the holders
        old_nftu_df['old_flg'] = 1
        merged_nftu = pd.merge(old_nftu_df, pdf_traderbase.rename(columns=_feature_rename_dict), 
                               how='outer', on=_kcid).fillna({'old_flg':0})
        
        # if there are records only in the new one
        for _varis in list(set(_kc) - set(['nftu_mint_flg', 'nftu_mint_usd'])):
            merged_nftu[_varis] = np.select([merged_nftu.old_flg == 0], [merged_nftu[_varis + "_n"]], default=merged_nftu[_varis])
        merged_nftu['nftu_mint_flg'] = np.select([merged_nftu.old_flg == 0], [merged_nftu.nftu_possible_mint_flg], default=0)
        merged_nftu['nftu_mint_usd'] = np.select([merged_nftu.old_flg == 0], [merged_nftu.nftu_possible_mint_usd], default=0)

        # handle the cases when both are filled (when keeping the new)
        for _varis in ['nftu_hold_flg', 'nftu_sell_date_latest', 'nftu_sell_price_usd_latest', 'nftu_buy_date_latest', 'nftu_buy_price_usd_latest']:
            merged_nftu[_varis] = np.select([(~merged_nftu[_varis + "_n"].isnull())], [merged_nftu[_varis + "_n"]], default=merged_nftu[_varis])
        
        # handle the cases when both are filled (when filling with sum)
        for _varis in ['nftu_sell_cnt', 'nftu_sell_amt_usd_sum', 'nftu_buy_cnt', 'nftu_buy_amt_usd_sum']:
            merged_nftu[_varis] = merged_nftu[_varis].fillna(0) + merged_nftu[_varis + "_n"].fillna(0)
        
        # writing out the table
        merged_nftu[_kcid + _kc].to_pickle(pathfolder_nftuser + "nftuser_{}.pickle".format(actmonat))
    
    else:

        # writing out the new file
        if not path.exists(pathfolder_nftuser):
            makedirs(pathfolder_nftuser)
        pdf_traderbase['nftu_mint_flg'] = pdf_traderbase.nftu_possible_mint_flg
        pdf_traderbase['nftu_mint_usd'] = pdf_traderbase.nftu_possible_mint_usd
        pdf_traderbase[_kcid + _kc].to_pickle(pathfolder_nftuser + "nftuser_{}.pickle".format(actmonat))
    
    
    pass


def _tempL_NFT_mthly(pathfolder_trx, pathfolder_nftuser, pathfolder_nft, pathfolder_nftkpi, actmonat, _tokcat):
    """
    Creates an NFT summary from transactions, calculating the cumulative counts and trading values (merging with previous mth). 
    For this reason, at least in month start, all NFT will appear at once.

    It will be the base of the later collection and mint stats. It'll aggregate the stats to the minter and
    collection level as well. It also contains the # of parallel owners (some NFT exists in multiple instances!!!). 

    Notes: the latest trx price will not be accurate everywhere, as we cannot see the quantity of sold NFTs
           in the case of token NFTs (which is made in x pieces). From later analyses, many of these will be 
           excluded. It could be also interesting to create a cumulated NFT-Owner-mindt table (excluding minters), 
           which can be used for counting the distinct owners of a given NFT. But it needs to much resource
           (maybe in pyspark)
    
    It should run after the nft-user loader
    """
    
    # reading transactions
    pdf_trx_mth = pd.read_pickle(pathfolder_trx + "trx_{}.pickle".format(actmonat))
    pdf_trx_mth = pdf_trx_mth[((pdf_trx_mth.trx_duplication_flg == 0) & (~pdf_trx_mth.trx_value_usd.isnull()))]

    # reading NFT (from same category)
    _kc_nft = ['token_category', 'collection_ID', 'token_ID', 'token_mint_address_est', 'token_mint_date_est']
    pdf_nft_master = pd.read_pickle(pathfolder_nft + 'token_master_{}.pickle'.format(_tokcat))

    # create NFT related tables
    # aggregating trx count for the period (by NFT) + adding the parallel-owner-count
    _kc = ['token_category', 'collection_ID', 'token_ID', 'trx_time_sec', 'trx_seller_address', 
           'trx_buyer_address', 'trx_value_usd']
    pdf_nft_cumulative = pd.merge(pdf_trx_mth[_kc], pdf_nft_master[_kc_nft], on=['token_category', 'collection_ID', 'token_ID'])
    # print(pdf_trx_mth.shape[0], pdf_nft_cumulative.shape[0])
    pdf_nft_cumulative['trx_cnt'] = 1
    pdf_nft_cumulative['token_tenure_days'] = (pdf_nft_cumulative.trx_time_sec - pdf_nft_cumulative.token_mint_date_est).apply(lambda x: int(x.days))

    # r1: token counts
    pdf_nft_cumulative = pdf_nft_cumulative.sort_values(by=['token_category', 'collection_ID', 'token_ID', 'trx_time_sec'])
    pdf_nft_cumulative['token_trx_cnt_cumsum'] = pdf_nft_cumulative.groupby(['token_category', 'collection_ID', 'token_ID'])['trx_cnt'].cumsum()
    pdf_nft_cumulative['token_trx_usd_cumsum'] = pdf_nft_cumulative.groupby(['token_category', 'collection_ID', 'token_ID'])['trx_value_usd'].cumsum()
    
    # r2: collection counts
    pdf_nft_cumulative = pdf_nft_cumulative.sort_values(by=['token_category', 'collection_ID', 'trx_time_sec'])
    pdf_nft_cumulative['collection_trx_cnt_cumsum'] = pdf_nft_cumulative.groupby(['token_category', 'collection_ID'])['trx_cnt'].cumsum()
    pdf_nft_cumulative['collection_trx_usd_cumsum'] = pdf_nft_cumulative.groupby(['token_category', 'collection_ID'])['trx_value_usd'].cumsum()

    # r3: minter counts
    pdf_nft_cumulative = pdf_nft_cumulative.sort_values(by=['token_category', 'token_mint_address_est', 'trx_time_sec'])
    pdf_nft_cumulative['minter_trx_cnt_cumsum'] = pdf_nft_cumulative.groupby(['token_category', 'token_mint_address_est'])['trx_cnt'].cumsum()
    pdf_nft_cumulative['minter_trx_usd_cumsum'] = pdf_nft_cumulative.groupby(['token_category', 'token_mint_address_est'])['trx_value_usd'].cumsum()

    # calculate the parallel owners (for later exclusions)
    # as we are reading the nft-user table, it should run after it.
    pdf_nft_user = pd.read_pickle(pathfolder_nftuser + "nftuser_{}.pickle".format(actmonat))
    _helper_multiuser = pdf_nft_user.groupby(['token_category', 'collection_ID', 'token_ID']).agg(
        token_owners_mecnt = ('nftu_hold_flg', 'sum')).reset_index()
    pdf_nft_cumulative = pd.merge(pdf_nft_cumulative, _helper_multiuser, on=['token_category', 'collection_ID', 'token_ID'] ,
                                  how='left').fillna({'token_owners_mecnt':0})
    pdf_nft_cumulative['nftkpi_inmonth_trx_flg'] = 1

    # writing out the results (appending with previous month, adding up the sums)
    _kc_tok_sum = ['token_trx_cnt_cumsum', 'token_trx_usd_cumsum']
    _kc_tok = ['trx_value_usd', 'trx_cnt', 'token_tenure_days']
    _kc_tok_max = ['token_owners_mecnt']
    _kc_coll_sum = ['collection_trx_cnt_cumsum', 'collection_trx_usd_cumsum']
    _kc_mint_sum = ['minter_trx_cnt_cumsum', 'minter_trx_usd_cumsum']
    _kc_id = ['token_category', 'collection_ID', 'token_ID', 'token_mint_address_est']
    _rename_dict = {'trx_time_sec':'nftkpi_datetime_stamp', 'token_trx_cnt_cumsum':'nftkpi_token_trx_cnt_csum', 
                    'token_trx_usd_cumsum':'nftkpi_token_trx_usd_csum', 'token_owners_mecnt':'nftkpi_token_parallel_own_max', 
                    'collection_trx_cnt_cumsum':'nftkpi_coll_trx_cnt_csum', 'collection_trx_usd_cumsum':'nftkpi_coll_trx_usd_csum', 
                    'minter_trx_cnt_cumsum':'nftkpi_mint_trx_cnt_csum', 'minter_trx_usd_cumsum':'nftkpi_mint_trx_usd_csum'}

    # checking if there is an earlier file:
    if path.exists(pathfolder_nftkpi + "nftkpi_{}.pickle".format(_monat_add(actmonat, -1))):

        pdf_oldnftkpi = pd.read_pickle(pathfolder_nftkpi + "nftkpi_{}.pickle".format(_monat_add(actmonat, -1)))
        _kcold = list(pdf_oldnftkpi.columns)

        # reading latest token counts, and join it back to the fresh table (new will be 0)
        pdf_oldnftkpi = pdf_oldnftkpi.sort_values(by=['token_category', 'collection_ID', 'token_ID', 'nftkpi_datetime_stamp', 'nftkpi_token_trx_cnt_csum'])
        _helper_tokenmax = pdf_oldnftkpi.groupby(['token_category', 'collection_ID', 'token_ID']).agg(
            tcnt_last = ('nftkpi_token_trx_cnt_csum', 'last'), tusd_last = ('nftkpi_token_trx_usd_csum', 'last'), 
            towner_last = ('nftkpi_token_parallel_own_max', 'last')).reset_index()
        pdf_nft_cumulative = pd.merge(pdf_nft_cumulative, _helper_tokenmax, how='left', 
                                      on=['token_category', 'collection_ID', 'token_ID']).fillna({'tcnt_last':0, 'tusd_last':0, 'towner_last':0})
        pdf_nft_cumulative['token_trx_cnt_cumsum'] = pdf_nft_cumulative.token_trx_cnt_cumsum + pdf_nft_cumulative.tcnt_last
        pdf_nft_cumulative['token_trx_usd_cumsum'] = pdf_nft_cumulative.token_trx_usd_cumsum + pdf_nft_cumulative.tusd_last
        pdf_nft_cumulative['token_owners_mecnt'] = np.select([pdf_nft_cumulative.towner_last > pdf_nft_cumulative.token_owners_mecnt], [pdf_nft_cumulative.towner_last], 
                                                             default=pdf_nft_cumulative.token_owners_mecnt)
        
        # reading latest collection counts, and join it back to the fresh table (new will be 0)
        pdf_oldnftkpi = pdf_oldnftkpi.sort_values(by=['token_category', 'collection_ID', 'nftkpi_datetime_stamp', 'nftkpi_coll_trx_cnt_csum'])
        _helper_collmax = pdf_oldnftkpi.groupby(['token_category', 'collection_ID']).agg(
            ccnt_last = ('nftkpi_coll_trx_cnt_csum', 'last'), cusd_last = ('nftkpi_coll_trx_usd_csum', 'last')).reset_index()
        pdf_nft_cumulative = pd.merge(pdf_nft_cumulative, _helper_collmax, how='left', 
                                      on=['token_category', 'collection_ID']).fillna({'ccnt_last':0, 'cusd_last':0})
        pdf_nft_cumulative['collection_trx_cnt_cumsum'] = pdf_nft_cumulative.collection_trx_cnt_cumsum + pdf_nft_cumulative.ccnt_last
        pdf_nft_cumulative['collection_trx_usd_cumsum'] = pdf_nft_cumulative.collection_trx_usd_cumsum + pdf_nft_cumulative.cusd_last
        
        # reading latest minter counts, and join it back to the fresh table (new will be 0)
        pdf_oldnftkpi = pdf_oldnftkpi.sort_values(by=['token_category', 'token_mint_address_est', 'nftkpi_datetime_stamp', 'nftkpi_mint_trx_cnt_csum'])
        _helper_mintmax = pdf_oldnftkpi.groupby(['token_category', 'token_mint_address_est']).agg(
            mcnt_last = ('nftkpi_mint_trx_cnt_csum', 'last'), musd_last = ('nftkpi_mint_trx_usd_csum', 'last')).reset_index()
        pdf_nft_cumulative = pd.merge(pdf_nft_cumulative, _helper_mintmax, how='left', 
                                      on=['token_category', 'token_mint_address_est']).fillna({'mcnt_last':0, 'musd_last':0})
        pdf_nft_cumulative['minter_trx_cnt_cumsum'] = pdf_nft_cumulative.minter_trx_cnt_cumsum + pdf_nft_cumulative.mcnt_last
        pdf_nft_cumulative['minter_trx_usd_cumsum'] = pdf_nft_cumulative.minter_trx_usd_cumsum + pdf_nft_cumulative.musd_last
        # print(pdf_nft_cumulative.shape[0])
        
        # collect those tokens which are not appeared in the new count and keep their latest value
        _newtokens = pdf_nft_cumulative[_kc_id].drop_duplicates()
        _newtokens['new_flg'] = 1
        pdf_oldnftkpi = pd.merge(pdf_oldnftkpi, _newtokens, on=_kc_id, how='left').fillna({'new_flg':0})
        pdf_oldnftkpi = pdf_oldnftkpi.sort_values(by=['token_category', 'collection_ID', 'token_ID', 'nftkpi_datetime_stamp', 'nftkpi_token_trx_cnt_csum'])
        pdf_oldnftkpi = pdf_oldnftkpi.groupby(_kc_id).last().reset_index()[_kcold]
        pdf_oldnftkpi['nftkpi_inmonth_trx_flg'] = 0 # nulling out the flag for those, which came from the prev mth

        # merge tables and writing out results
        _export_df = pdf_oldnftkpi.append(pdf_nft_cumulative.rename(columns=_rename_dict)[_kcold])
        _export_df.reset_index(drop=True)
        _export_df.to_pickle(pathfolder_nftkpi + "nftkpi_{}.pickle".format(actmonat))

    else:

        # writing out the new file
        if not path.exists(pathfolder_nftkpi):
            makedirs(pathfolder_nftkpi)
        
        _kc = _kc_id + ['trx_time_sec'] + _kc_tok_sum + _kc_tok_max + _kc_tok + _kc_coll_sum + _kc_mint_sum + ['nftkpi_inmonth_trx_flg']
        pdf_nft_cumulative[_kc].rename(columns=_rename_dict).to_pickle(pathfolder_nftkpi + "nftkpi_{}.pickle".format(actmonat))

    pass

# maybe add a collection_n_minter field, and do the aggregation for that field as well...
# that can be even more precise

def _DML_collectionTimeSeries(pathfolder_nftkpi, pathfolder_trx, pathfolder_colldistuser, pathfolder_collection_ts, actmonat, malist=[7, 14, 28], _estcalc=False):
    """
    Creates a collection x day (28/29, 30, 31 days x all collections till date) time series report table for all the 
    collections, stored monthly. It contains the cumulative sums, the moving averages (and std-s) of the latest
    transaction values, etc. 

    It also contains the NFT distinct count (till date), and an owner distinc count (till date, act). 
    Later, a collection master will be pulled (for getting additional fields, like the # of tokens, etc.)

    If the _estcalc is True, the Moving Averages will be calculated on the daily base 
    (the std and quantile features will be not accurate, but can run with lower memory...)

    The malist list should be in ascending order (later maybe add a .sort()).

    """

    # calculate the days of the monat
    _daylist = _monatToDateList(actmonat)
    _firstday = _daylist[0]
    _prevday = _firstday - timedelta(days=1)

    # reading the latest 2 months of NFT KPI tables (if there are 2 months)
    pdf_nftkpi = pd.read_pickle(pathfolder_nftkpi + "nftkpi_{}.pickle".format(actmonat))
    if path.exists(pathfolder_nftkpi + "nftkpi_{}.pickle".format(_monat_add(actmonat, -1))):
        pdf_nftkpi = pd.read_pickle(pathfolder_nftkpi + "nftkpi_{}.pickle".format(_monat_add(actmonat, -1))).append(
            pdf_nftkpi).reset_index(drop=True)
    
    # reading the transaction table (for filling collection - owner features / temp tables)
    pdf_trx = pd.read_pickle(pathfolder_trx + "trx_{}.pickle".format(actmonat))
    pdf_trx = pdf_trx[((pdf_trx.trx_duplication_flg == 0) & (~pdf_trx.trx_value_usd.isnull()))]
    
    # loading the cumulative collection-owner table
    pdf_coll_buyers = pdf_trx.groupby(['token_category', 'collection_ID', 'trx_buyer_address']).agg(
        dt_first_record = ('trx_time_sec', 'min')).reset_index().rename(columns={'trx_buyer_address':'trader_ID'})
    pdf_coll_sellers = pdf_trx.groupby(['token_category', 'collection_ID', 'trx_seller_address']).agg(
        dt_first_record = ('trx_time_sec', 'min')).reset_index().rename(columns={'trx_seller_address':'trader_ID'})
    pdf_coll_owners_cum = pdf_coll_buyers.append(pdf_coll_sellers)
    pdf_coll_owners_cum = pdf_coll_owners_cum[(~pdf_coll_owners_cum.dt_first_record.isnull())].groupby(
        ['token_category', 'collection_ID', 'trader_ID']).agg({'dt_first_record':'min'}).reset_index()
    if path.exists(pathfolder_colldistuser + "distcolluser_{}.pickle".format(_monat_add(actmonat, -1))):
        old_ownercoll = pd.read_pickle(pathfolder_colldistuser + "distcolluser_{}.pickle".format(_monat_add(actmonat, -1)))
        pdf_coll_owners_cum = old_ownercoll.append(pdf_coll_owners_cum).groupby(
            ['token_category', 'collection_ID', 'trader_ID']).agg({'dt_first_record':'min'}).reset_index()
        pdf_coll_owners_cum.to_pickle(pathfolder_colldistuser + "distcolluser_{}.pickle".format(actmonat))
    else:
        if not path.exists(pathfolder_colldistuser):
            makedirs(pathfolder_colldistuser)
        pdf_coll_owners_cum.to_pickle(pathfolder_colldistuser + "distcolluser_{}.pickle".format(actmonat))
    
    # Collection Mint dates
    _helper_mintdates = pdf_coll_owners_cum.groupby(['token_category', 'collection_ID']).agg(coll_mint_dt = ('dt_first_record', 'min')).reset_index()
    _helper_mintdates['coll_mint_dt'] = _helper_mintdates.coll_mint_dt.dt.date
    
    # Moving Average alike KPIs (all collections from the mid tables = all up to date minted collections)

    # - preparing date/collection table (structure)
    _collections = np.array(pdf_nftkpi.collection_ID.unique())
    _dates = np.array(_daylist)
    pdf_coll_kpi = pd.DataFrame(np.transpose([np.tile(_dates, len(_collections)), np.repeat(_collections, len(_dates))]), columns=['report_dt', 'collection_ID'])
    pdf_coll_kpi['report_dt'] = pd.to_datetime(pdf_coll_kpi['report_dt'])
    pdf_coll_kpi['token_category'] = pdf_nftkpi.iloc[0].token_category # not the nicest solution, later on, use meshgrid!
    # print(pdf_coll_kpi.shape[0])
    pdf_coll_kpi = pd.merge(pdf_coll_kpi, _helper_mintdates, on=['token_category', 'collection_ID'])
    # print(pdf_coll_kpi.shape[0]) 
    pdf_coll_kpi = pdf_coll_kpi[pdf_coll_kpi.report_dt >= pdf_coll_kpi.coll_mint_dt].copy()
    # print(pdf_coll_kpi.shape[0])

    # - adding date intervals (for moving average alike features)
    for _madays in malist:
        pdf_coll_kpi['ma_{}_start_dt'.format(_madays)] = pdf_coll_kpi.report_dt - timedelta(days=_madays)
    
    # - joining the table wiht the KPI transactions (after aggregating it by day and by collection)
    pdf_nftkpi_agg = pdf_nftkpi[pdf_nftkpi.nftkpi_inmonth_trx_flg == 1].copy() # for avoiding duplications
    pdf_nftkpi_agg['nftkpi_datetime_stamp'] = pdf_nftkpi_agg.nftkpi_datetime_stamp.dt.date
    if _estcalc:
        # in this low-memory mode, pre-aggregating everything daily
        pdf_nftkpi_agg = pdf_nftkpi_agg.groupby(['token_category', 'collection_ID', 'nftkpi_datetime_stamp']).agg(
            coll_trx_cnt_dly = ('trx_cnt', 'sum'), coll_trx_usd_dly = ('trx_value_usd', 'sum'), coll_trx_maxval_usd = ('trx_value_usd', 'max'), 
            coll_trx_cnt_csum = ('nftkpi_coll_trx_cnt_csum', 'max'), coll_trx_usd_csum = ('nftkpi_coll_trx_usd_csum', 'max')).reset_index()
    else:
        # here, we keep all 2mth transactions (maybe not running with low performance PCs)
        _kc = ['token_category', 'collection_ID', 'nftkpi_datetime_stamp', 'coll_trx_cnt_dly', 'coll_trx_usd_dly', 
               'coll_trx_maxval_usd', 'coll_trx_cnt_csum', 'coll_trx_usd_csum']
        pdf_nftkpi_agg['coll_trx_maxval_usd'] = pdf_nftkpi_agg.trx_value_usd
        pdf_nftkpi_agg = pdf_nftkpi_agg.rename(columns={'trx_cnt':'coll_trx_cnt_dly', 'trx_value_usd':'coll_trx_usd_dly', 
                                                        'nftkpi_coll_trx_cnt_csum':'coll_trx_cnt_csum', 'nftkpi_coll_trx_usd_csum':'coll_trx_usd_csum'})[_kc]
    pdf_coll_kpi_20 = pd.merge(pdf_coll_kpi, pdf_nftkpi_agg, on=['token_category', 'collection_ID'], how='left')

    # it can help in the performance, but the result table should be joined w the original table
    pdf_coll_kpi_20 = pdf_coll_kpi_20[((pdf_coll_kpi_20.nftkpi_datetime_stamp > pdf_coll_kpi_20['ma_{}_start_dt'.format(malist[-1])]) & 
                                       (pdf_coll_kpi_20.nftkpi_datetime_stamp <= pdf_coll_kpi_20.report_dt))].copy()
    # print(pdf_nftkpi_agg.shape[0], pdf_coll_kpi_20.shape[0])

    # - recalculating the values (check the intervals)
    for _madays in malist:
        pdf_coll_kpi_20['coll_trx_cnt_dly_ma{}'.format(_madays)] = np.select([((pdf_coll_kpi_20.nftkpi_datetime_stamp > pdf_coll_kpi_20['ma_{}_start_dt'.format(_madays)]) & 
                                                                               (pdf_coll_kpi_20.nftkpi_datetime_stamp <= pdf_coll_kpi_20.report_dt))], 
                                                                             [pdf_coll_kpi_20.coll_trx_cnt_dly], default=np.NaN)
        pdf_coll_kpi_20['coll_trx_usd_dly_ma{}'.format(_madays)] = np.select([((pdf_coll_kpi_20.nftkpi_datetime_stamp > pdf_coll_kpi_20['ma_{}_start_dt'.format(_madays)]) & 
                                                                               (pdf_coll_kpi_20.nftkpi_datetime_stamp <= pdf_coll_kpi_20.report_dt))], 
                                                                             [pdf_coll_kpi_20.coll_trx_usd_dly], default=np.NaN)
    pdf_coll_kpi_20['coll_trx_maxval_usd'] = np.select([(pdf_coll_kpi_20.nftkpi_datetime_stamp <= pdf_coll_kpi_20.report_dt)], [pdf_coll_kpi_20.coll_trx_maxval_usd], default=np.NaN)
    pdf_coll_kpi_20['coll_trx_cnt_csum'] = np.select([(pdf_coll_kpi_20.nftkpi_datetime_stamp <= pdf_coll_kpi_20.report_dt)], [pdf_coll_kpi_20.coll_trx_cnt_csum], default=np.NaN)
    pdf_coll_kpi_20['coll_trx_usd_csum'] = np.select([(pdf_coll_kpi_20.nftkpi_datetime_stamp <= pdf_coll_kpi_20.report_dt)], [pdf_coll_kpi_20.coll_trx_usd_csum], default=np.NaN)

    # - aggregating the KPIs daily (np.percentile is faster, but sometimes not working - not nan)
    def p25(_row):
        return _row.quantile(0.25)
    def p75(_row):
        return _row.quantile(0.75)
    
    # - defining the aggregations (0/1 = needs moving average, the others are the functions to execute)
    _aggdefs = {'coll_trx_cnt_dly':[1, ['sum'], ['sum']], 'coll_trx_usd_dly':[1, ['min', 'max', 'sum', 'std', p25, p75], ['sum']], 
                'coll_trx_maxval_usd':[0, ['max']], 'coll_trx_cnt_csum':[0, ['max']], 'coll_trx_usd_csum':[0, ['max']]}
    _aggdefs_str = {'coll_trx_cnt_dly':[1, ['sum'], ['sum']], 'coll_trx_usd_dly':[1, ['min', 'max', 'sum', 'std', 'p25', 'p75'], ['sum']], 
                    'coll_trx_maxval_usd':[0, ['max']], 'coll_trx_cnt_csum':[0, ['max']], 'coll_trx_usd_csum':[0, ['max']]}
    _aggdict_ma = {}
    _aggdict_ma_str = {}
    for _keys in _aggdefs.keys():
        if (_aggdefs[_keys][0] > 0):
            for _madays in malist:
                _aggdict_ma.update({_keys + '_ma{}'.format(_madays):_aggdefs[_keys][1]})
                _aggdict_ma_str.update({_keys + '_ma{}'.format(_madays):_aggdefs_str[_keys][1]})
            if (len(_aggdefs[_keys]) > 2):
                _aggdict_ma.update({_keys:_aggdefs[_keys][2]})
                _aggdict_ma_str.update({_keys:_aggdefs_str[_keys][2]})
        else:
            _aggdict_ma.update({_keys:_aggdefs[_keys][1]})
            _aggdict_ma_str.update({_keys:_aggdefs_str[_keys][1]})
    
    # - aggregating tables, and solving the indexes
    pdf_coll_kpi_dly = pdf_coll_kpi_20.groupby(['token_category', 'collection_ID', 'report_dt']).agg(_aggdict_ma).reset_index()

    _kc = ['token_category', 'collection_ID', 'report_dt']
    for _cols in _aggdict_ma_str.keys():
        for _postfixes in _aggdict_ma_str[_cols]:
            _kc = _kc + [_cols + '_' + _postfixes]
            pdf_coll_kpi_dly[_cols + '_' + _postfixes] = pdf_coll_kpi_dly[_cols][_postfixes].values
    pdf_coll_kpi_dly = pdf_coll_kpi_dly[_kc].copy()
    
    # resolving indexes
    _cols = pdf_coll_kpi_dly.columns
    pdf_coll_kpi_dly.columns = _colsimpler(_cols)

    # calculated fields (AVG price = USD / CNT)
    pdf_coll_kpi_dly['coll_avg_price_dly'] = pdf_coll_kpi_dly.coll_trx_usd_dly_sum / pdf_coll_kpi_dly.coll_trx_cnt_dly_sum
    for _madays in malist:
        pdf_coll_kpi_dly['coll_avg_price_dly_ma{}'.format(_madays)] = pdf_coll_kpi_dly['coll_trx_usd_dly_ma{}_sum'.format(_madays)] / pdf_coll_kpi_dly['coll_trx_cnt_dly_ma{}_sum'.format(_madays)]

    # distinct owners cumulative - per month
    pdf_coll_owners_cum_dly = pdf_coll_owners_cum.copy()
    pdf_coll_owners_cum_dly['dt_first_record'] = pdf_coll_owners_cum_dly.dt_first_record.dt.date
    pdf_coll_owners_cum_dly['dt_first_record'] = np.select([pdf_coll_owners_cum_dly.dt_first_record < _firstday], 
                                                           [_firstday], default=pdf_coll_owners_cum_dly.dt_first_record)
    pdf_coll_owners_cum_dly = pdf_coll_owners_cum_dly.groupby(['token_category', 'collection_ID', 'dt_first_record']).agg(
        coll_owner_cumdcnt = ('trader_ID', 'count')).reset_index()
    pdf_coll_owners_cum_dly['coll_owner_cumdcnt'] = pdf_coll_owners_cum_dly.groupby(['token_category', 'collection_ID'])['coll_owner_cumdcnt'].cumsum()
    
    pdf_coll_kpi_out = pd.merge(pdf_coll_kpi[['token_category', 'collection_ID', 'report_dt']], pdf_coll_owners_cum_dly, 
                                on=['token_category', 'collection_ID'], how='left')
    pdf_coll_kpi_out['coll_owner_cumdcnt'] = np.select([pdf_coll_kpi_out.dt_first_record <= pdf_coll_kpi_out.report_dt], 
                                                       [pdf_coll_kpi_out.coll_owner_cumdcnt], default=np.NaN)
    pdf_coll_kpi_out = pdf_coll_kpi_out.groupby(['token_category', 'collection_ID', 'report_dt']).agg({'coll_owner_cumdcnt':'max'}).reset_index()
    

    # calculating the sum value of the collections (up to date) and the dist NFTs (for the AVG value)
    
    # - transforming the current NFT transaction table
    _kctr = ['token_category', 'collection_ID', 'token_ID', 'trx_time_sec', 'trx_value_usd']
    _kcv = ['token_category', 'collection_ID', 'token_ID', 'coll_agg_date', 'coll_latest_value', 'coll_latest_trx_dt']
    pdf_valuechecker = pdf_trx[_kctr].copy()
    pdf_valuechecker['coll_agg_date'] = pdf_valuechecker.trx_time_sec.dt.date
    pdf_valuechecker = pdf_valuechecker.sort_values(
        by=['token_category', 'collection_ID', 'token_ID', 'coll_agg_date', 'trx_time_sec']).groupby(
            ['token_category', 'collection_ID', 'token_ID', 'coll_agg_date']).agg(
                coll_latest_value = ('trx_value_usd', 'last'), coll_latest_trx_dt = ('trx_time_sec', 'last')).reset_index()
    pdf_valuechecker['coll_latest_trx_dt'] = pdf_valuechecker.coll_latest_trx_dt.dt.date # it is basically always coll_agg_date
    pdf_valuechecker = pdf_valuechecker[_kcv]

    # - loading the prev mth coin table (if exist) for getting all coins latest values, and merge w the daily aggs
    if path.exists(pathfolder_nftkpi + "nftkpi_{}.pickle".format(_monat_add(actmonat, -1))):
        _kc_kpi = ['token_category', 'collection_ID', 'token_ID', 'nftkpi_datetime_stamp', 'trx_value_usd']
        pdf_nftkpi_prevmth = pd.read_pickle(pathfolder_nftkpi + 'nftkpi_{}.pickle'.format(_monat_add(actmonat, -1)))[_kc_kpi].sort_values(
            by=['token_category', 'collection_ID', 'token_ID', 'nftkpi_datetime_stamp']).groupby(
                ['token_category', 'collection_ID', 'token_ID']).agg(
                    coll_latest_value = ('trx_value_usd', 'last'), coll_latest_trx_dt = ('nftkpi_datetime_stamp', 'last')).reset_index()
        pdf_nftkpi_prevmth['coll_latest_trx_dt'] = pdf_nftkpi_prevmth.coll_latest_trx_dt.dt.date
        pdf_nftkpi_prevmth['coll_agg_date'] = _prevday
        pdf_valuechecker = pdf_nftkpi_prevmth[_kcv].append(pdf_valuechecker)
    
    # - finding the previous transaction's value and date (fillna(0)) for each token and calculate deltas
    pdf_valuechecker = pdf_valuechecker.sort_values(
        by=['token_category', 'collection_ID', 'token_ID', 'coll_agg_date', 'coll_latest_trx_dt']).reset_index(drop=True)
    pdf_valuechecker['coll_token_latest_trx_daycnt'] = pdf_valuechecker.coll_agg_date - pdf_valuechecker.coll_latest_trx_dt
    pdf_valuechecker['coll_token_latest_trx_daycnt'] = pdf_valuechecker['coll_token_latest_trx_daycnt'].apply(lambda x: int(x.days))
    pdf_valuechecker['coll_prev_value'] = pdf_valuechecker.groupby(
        ['token_category', 'collection_ID', 'token_ID'])['coll_latest_value'].shift(1).fillna(0)
    pdf_valuechecker['coll_prev_daycnt'] = pdf_valuechecker.groupby(
        ['token_category', 'collection_ID', 'token_ID'])['coll_token_latest_trx_daycnt'].shift(1).fillna(0)
    pdf_valuechecker['coll_added_value'] = pdf_valuechecker.coll_latest_value - pdf_valuechecker.coll_prev_value
    pdf_valuechecker['coll_added_daycnt'] = pdf_valuechecker.coll_token_latest_trx_daycnt - pdf_valuechecker.coll_prev_daycnt 
    pdf_valuechecker['coll_added_daycnt'] = np.select([pdf_valuechecker.coll_agg_date >= _firstday], 
                                                      [pdf_valuechecker.coll_added_daycnt - pdf_valuechecker.coll_agg_date.apply(lambda x:int(x.day)) + 1], 
                                                      default=pdf_valuechecker['coll_added_daycnt'])
    # coll_added_daycnt is not perfect yet... some further design is necessary (although, very close)
    
    # - intermezzo: creating a daily dcnt NFT/collection table before aggregating its source
    pdf_dnftcounter = pdf_valuechecker.sort_values(by=['token_category', 'collection_ID', 'token_ID', 'coll_agg_date']).groupby(
        ['token_category', 'collection_ID', 'token_ID']).agg(first_rec_dt = ('coll_agg_date', 'first')).reset_index()
    pdf_dnftcounter['coll_token_dcnt'] = 1
    pdf_dnftcounter = pdf_dnftcounter.groupby(['token_category', 'collection_ID', 'first_rec_dt']).agg(
        {'coll_token_dcnt':'sum'}).reset_index()
    pdf_dnftcounter['coll_token_dcnt'] = pdf_dnftcounter.groupby(['token_category', 'collection_ID'])['coll_token_dcnt'].cumsum()

    # - aggregate the delta values to date, and calculate the cumulative value to date
    pdf_valuechecker = pdf_valuechecker.groupby(['token_category', 'collection_ID', 'coll_agg_date']).agg(
        coll_token_sum_usd = ('coll_added_value', 'sum'), 
        coll_token_sum_lastdays = ('coll_added_daycnt', 'sum')).reset_index()
    pdf_valuechecker['coll_token_sum_usd'] = pdf_valuechecker.groupby(['token_category', 'collection_ID'])['coll_token_sum_usd'].cumsum()
    pdf_valuechecker['coll_token_sum_lastdays'] = pdf_valuechecker.groupby(['token_category', 'collection_ID'])['coll_token_sum_lastdays'].cumsum()
    
    # - join the value table to the date table
    pdf_coll_kpi_out_nftv = pd.merge(pdf_coll_kpi[['token_category', 'collection_ID', 'report_dt']], pdf_valuechecker, 
                                     on=['token_category', 'collection_ID'], how='left')
    pdf_coll_kpi_out_nftv['coll_token_sum_usd'] = np.select([(pdf_coll_kpi_out_nftv.coll_agg_date <= pdf_coll_kpi_out_nftv.report_dt)], 
                                                            [pdf_coll_kpi_out_nftv.coll_token_sum_usd], default=np.NaN)
    pdf_coll_kpi_out_nftv['coll_token_sum_lastdays'] = np.select([(pdf_coll_kpi_out_nftv.coll_agg_date <= pdf_coll_kpi_out_nftv.report_dt)], 
                                                                 [pdf_coll_kpi_out_nftv.coll_token_sum_lastdays], default=np.NaN)
    pdf_coll_kpi_out_nftv['coll_last_trx_dt'] = np.select([(pdf_coll_kpi_out_nftv.coll_agg_date <= pdf_coll_kpi_out_nftv.report_dt)], 
                                                            [pdf_coll_kpi_out_nftv.coll_agg_date], default=np.NaN)
    pdf_coll_kpi_out_nftv = pdf_coll_kpi_out_nftv.sort_values(by=['token_category', 'collection_ID', 'report_dt', 'coll_agg_date'])
    pdf_coll_kpi_out_nftv = pdf_coll_kpi_out_nftv.groupby(['token_category', 'collection_ID', 'report_dt']).last().reset_index()

    # - join the NFT count table to the date table
    pdf_coll_kpi_out_nftc = pd.merge(pdf_coll_kpi[['token_category', 'collection_ID', 'report_dt']], pdf_dnftcounter, 
                                     on=['token_category', 'collection_ID'], how='left')
    pdf_coll_kpi_out_nftc['coll_token_dcnt'] = np.select([(pdf_coll_kpi_out_nftc.first_rec_dt <= pdf_coll_kpi_out_nftc.report_dt)], 
                                                         [pdf_coll_kpi_out_nftc.coll_token_dcnt], default=np.NaN)
    pdf_coll_kpi_out_nftc = pdf_coll_kpi_out_nftc.sort_values(by=['token_category', 'collection_ID', 'report_dt', 'first_rec_dt'])
    pdf_coll_kpi_out_nftc = pdf_coll_kpi_out_nftc.groupby(['token_category', 'collection_ID', 'report_dt']).last()[['coll_token_dcnt']].reset_index()
    # print(pdf_coll_kpi_out_nftc.shape[0])

    # join partial tables (user dcnt, token dcnt, overall values, trx MAs), calculate features, finalize structure
    pdf_coll_kpi_out = pd.merge(pdf_coll_kpi_out, pdf_coll_kpi_dly, on=['token_category', 'collection_ID', 'report_dt'], how='left')
    pdf_coll_kpi_out = pd.merge(pdf_coll_kpi_out, pdf_coll_kpi_out_nftv, on=['token_category', 'collection_ID', 'report_dt'], how='left')
    pdf_coll_kpi_out = pd.merge(pdf_coll_kpi_out, pdf_coll_kpi_out_nftc, on=['token_category', 'collection_ID', 'report_dt'], how='left')
    # print(pdf_coll_kpi_out.shape[0])
    pdf_coll_kpi_out['coll_token_avg_latest_val_usd'] = pdf_coll_kpi_out.coll_token_sum_usd / pdf_coll_kpi_out.coll_token_dcnt
    pdf_coll_kpi_out['coll_token_avg_latest_trx_day'] = pdf_coll_kpi_out.coll_token_sum_lastdays / pdf_coll_kpi_out.coll_token_dcnt + pdf_coll_kpi_out.report_dt.dt.day

    _kc_final = ['token_category', 'collection_ID', 'report_dt', 'coll_owner_cumdcnt', 'coll_token_dcnt', 
                 'coll_token_sum_usd', 'coll_token_avg_latest_val_usd', 'coll_token_avg_latest_trx_day', 
                 'coll_trx_maxval_usd_max', 'coll_trx_usd_csum_max', 'coll_trx_cnt_csum_max', 
                 'coll_trx_cnt_dly_sum', 'coll_trx_usd_dly_sum', 'coll_avg_price_dly']
    
    for _madays in malist:
        _kc_final = _kc_final  + ['coll_avg_price_dly_ma{}'.format(_madays)]
    
    for _madays in malist:
        _kc_final = _kc_final  + ['coll_trx_cnt_dly_ma{}_sum'.format(_madays)]
        _kc_final = _kc_final  + ['coll_trx_usd_dly_ma{}_sum'.format(_madays)]
        _kc_final = _kc_final  + ['coll_trx_usd_dly_ma{}_min'.format(_madays)]
        _kc_final = _kc_final  + ['coll_trx_usd_dly_ma{}_max'.format(_madays)]
        _kc_final = _kc_final  + ['coll_trx_usd_dly_ma{}_std'.format(_madays)]
        _kc_final = _kc_final  + ['coll_trx_usd_dly_ma{}_p25'.format(_madays)]
        _kc_final = _kc_final  + ['coll_trx_usd_dly_ma{}_p75'.format(_madays)]
    
    pdf_coll_kpi_out = pdf_coll_kpi_out[_kc_final]


    # appending w previous month (for filling in missing values)
    if path.exists(pathfolder_collection_ts + "coll_ts_{}.pickle".format(_monat_add(actmonat, -1))):
        old_df = pd.read_pickle(pathfolder_collection_ts + "coll_ts_{}.pickle".format(_monat_add(actmonat, -1)))
        pdf_coll_kpi_out = old_df.append(pdf_coll_kpi_out).sort_values(by=['token_category', 'collection_ID', 'report_dt'])
        pdf_coll_kpi_out['report_dt'] = pd.to_datetime(pdf_coll_kpi_out['report_dt'])
        pdf_coll_kpi_out['report_dt'] = pdf_coll_kpi_out['report_dt'].dt.date

        # fillna previous values
        _ffill_cols = ['coll_owner_cumdcnt', 'coll_token_dcnt', 'coll_token_sum_usd', 'coll_token_avg_latest_val_usd', 
                       'coll_trx_maxval_usd_max', 'coll_trx_usd_csum_max', 'coll_trx_cnt_csum_max']
        pdf_coll_kpi_out[_ffill_cols] = pdf_coll_kpi_out[_ffill_cols].fillna(pdf_coll_kpi_out.groupby(['token_category', 'collection_ID'])[_ffill_cols].ffill())

        # apply a cumulative max on the max token value
        pdf_coll_kpi_out['coll_trx_maxval_usd_max'] = pdf_coll_kpi_out.groupby(['token_category', 'collection_ID'])['coll_trx_maxval_usd_max'].cummax()

        # filtering table and writing out
        pdf_coll_kpi_out = pdf_coll_kpi_out[pdf_coll_kpi_out.report_dt >= _firstday]
        # print(pdf_coll_kpi_out.shape[0])
        pdf_coll_kpi_out.to_pickle(pathfolder_collection_ts + "coll_ts_{}.pickle".format(actmonat))

    # writing out the current file if not
    else:
        if not path.exists(pathfolder_collection_ts):
            makedirs(pathfolder_collection_ts)
        pdf_coll_kpi_out.to_pickle(pathfolder_collection_ts + "coll_ts_{}.pickle".format(actmonat))
    
    # Future development ideas
    #  - adding the current number of owners (not just the cumulative one)
    #  - adding a minter count (distinct cumulative)
    #  - adding the latest transaction date of the collection itself
    #  - correcting the coll_token_avg_latest_trx_day value (it cannot be negative :))

    pass



##################################################################
#                         Main  Functions                        #
##################################################################

def TrxDuplicationMarker(path_folder_in, load_interval_intmonat=[]):
    """
    Reads the transactions from the input folder (2 monthes onto 1 table) and mark those transactions which
    are probably duplicated (there are 2 consequtive A --> B sales w/o any other sales in between).

    Inputs: 
     * path_folder_in: input folder of transaction files
     * load_interval_intmonat: between these months the loader will clean the transactions (loading 2 by 2).
       If missing, then cleansing all the months.
    """

    # generating month list
    if (len(load_interval_intmonat) > 0):
        _basic_mth_list = _monat_list_creator(load_interval_intmonat, 0)
    else:
        _basic_mth_list = listdir(path_folder_in)
        _basic_mth_list = [int(_item.split('.')[0].split('_')[-1]) for _item in _basic_mth_list]

    # pairing the months (running pla)
    _paired_mths = []
    for i in range(len(_basic_mth_list) - 1):
        _paired_mths = _paired_mths + [[_basic_mth_list[i], _basic_mth_list[i+1]]]
    
    # reading the months by the plan (and marking the duplicates)
    for _mthtuples in _paired_mths:

        _tmp_trdf = _readTrx_betweenMonats(path_folder_in, _mthtuples)

        # finding duplicates (w/o the date, just by checking the pairs)
        # if we do not want to build in a date control (<= 3 days) to the duplication flagging, than this part is not necessary
        _helper_dup = _tmp_trdf.groupby(['collection_ID', 'token_ID', 'trx_seller_address', 'trx_buyer_address', 'trx_value_crypto']).agg(
            trx_count = ('trx_time_sec', 'count'), trx_mindt = ('trx_time_sec', 'min'), trx_maxdt = ('trx_time_sec', 'max')).reset_index()
        _helper_dup = _helper_dup[_helper_dup.trx_count > 0].copy()
        _helper_dup['datediff'] = (_helper_dup.trx_maxdt - _helper_dup.trx_mindt).apply(lambda x: int(x.days))
        _helper_dup = _helper_dup[_helper_dup.datediff <= 3][['collection_ID', 'token_ID', 'trx_seller_address', 'trx_buyer_address', 'trx_value_crypto']].copy()
        _helper_dup['possible_dup'] = 1
        _tmp_trdf = pd.merge(_tmp_trdf, _helper_dup, how='left', on=['collection_ID', 'token_ID', 'trx_seller_address', 'trx_buyer_address', 'trx_value_crypto']).fillna({'possible_dup':0})

        # round 1: ordering the sales by token, and flag those, where the seller are the same (in 2 cons. sales event)
        # as it is possible to sell to multiple buyers (token NFT), this step makes less sense
        _tmp_trdf = _tmp_trdf.sort_values(by=['collection_ID', 'token_ID', 'trx_time_sec'])
        _tmp_trdf['seller_address_prev'] = _tmp_trdf.groupby(['collection_ID', 'token_ID'])['trx_seller_address'].shift(1)
        # _tmp_trdf['trx_duplication_flg'] = np.select([_tmp_trdf.trx_seller_address == _tmp_trdf.seller_address_prev], 
        #                                              [1], default=_tmp_trdf.trx_duplication_flg)
        
        # round 2: from the marked ones, select those which are not the first...
        # note: would be nice to mark A-->B , A-->B w/o B-->A, but the join could take long, and only a few cases / million...
        _tmp_trdf = _tmp_trdf.sort_values(by=['collection_ID', 'token_ID', 'trx_seller_address', 'trx_buyer_address', 'trx_value_crypto', 'trx_time_sec'])
        _tmp_trdf['possible_dup'] = _tmp_trdf.groupby(['collection_ID', 'token_ID', 'trx_seller_address', 'trx_buyer_address', 'trx_value_crypto'])['possible_dup'].cumsum()
        _tmp_trdf['trx_duplication_flg'] = np.select([_tmp_trdf.possible_dup > 1], [1], default=_tmp_trdf.trx_duplication_flg)

        # writing out the months
        _tmp_trdf['tr_monat'] = _tmp_trdf.trx_time_sec.dt.year * 100 + _tmp_trdf.trx_time_sec.dt.month
        for _mths in _mthtuples:
            _tmp_trdf[_tmp_trdf.tr_monat == _mths].drop(['seller_address_prev', 'tr_monat', 'possible_dup'], axis=1).to_pickle(path_folder_in + 'trx_{}.pickle'.format(int(_mths)))

    pass


def NFTUserPairs_runner(trx_folder_in, nftu_folder_out, load_interval_intmonat=[], _verbose=False):
    """
    Reads the transactions from the input folder and creates an NFT-user temp table. 
    Currently, the in folder should show the transactions/<category> folder - later, it will be changed.

    Inputs: 
     * trx_folder_in: input folder of transaction files
     * nftu_folder_out: path of the NFT - user table's folder
     * load_interval_intmonat: between these months the loader will load the DM table.
       If empty list is given, it will run for all the transaction files.
    """

    if _verbose:
        log("NFT - User Montly Pair Table started running ...")
    
    # generating month list
    if (len(load_interval_intmonat) > 0):
        _basic_mth_list = _monat_list_creator(load_interval_intmonat, 0)
    else:
        _basic_mth_list = listdir(trx_folder_in)
        _basic_mth_list = [int(_item.split('.')[0].split('_')[-1]) for _item in _basic_mth_list]
    
    if _verbose:
        log("Running {} month(s) from {} to {}".format(len(_basic_mth_list), _basic_mth_list[0], _basic_mth_list[-1]))
    
    # run (serial) the DM loader
    for _mths in _basic_mth_list:
        _tempL_NFT_User_pairs_mthly(trx_folder_in, nftu_folder_out, _mths)
    
    if _verbose:
        log("NFT - User Montly Pair Table finished running ...")
    
    pass


def NFTKPI_runner(trx_folder, nftu_folder, nftkpi_folder, nftfolder, tokencats, load_interval_intmonat=[], _verbose=False):
    """
    Reads the transactions, the NFT master, the transaction - user temp layer table from the input folders
    and creates an NFT KPI temp table. It runs for all the categories from the tokencats list (no automatic value).

    Inputs: 
     * trx_folder: input folder of transaction files
     * nftu_folder: path of the NFTU-user table's folder
     * nftkpi_folder: path of the newly created NFT KPI table's folder
     * nftfolder: path of the nft master folder
     * load_interval_intmonat: between these months the loader will load the DM table. If empty list is given, 
                               it will run for all the transaction files.
     * tokencats: the categories of tokens the loader is running for (list)
    """
    
    if _verbose:
        log("NFT - NFT KPI monthly loader started running ...")
    
    for _cats in tokencats:

        # generating month list
        if (len(load_interval_intmonat) > 0):
            _basic_mth_list = _monat_list_creator(load_interval_intmonat, 0)
        else:
            _basic_mth_list = listdir(trx_folder + '{}/'.format(_cats.lower()))
            _basic_mth_list = [int(_item.split('.')[0].split('_')[-1]) for _item in _basic_mth_list]
        
        if _verbose:
            log("{} category is running {} month(s) from {} to {}".format(_cats, len(_basic_mth_list), _basic_mth_list[0], _basic_mth_list[-1]))
        
        # run (serial) the DM loader
        for _mths in _basic_mth_list:
            _tempL_NFT_mthly(pathfolder_trx = trx_folder + '{}/'.format(_cats.lower()), 
                             pathfolder_nftuser = nftu_folder + '{}/'.format(_cats.lower()), 
                             pathfolder_nft = nftfolder,
                             pathfolder_nftkpi = nftkpi_folder + '{}/'.format(_cats.lower()), 
                             actmonat = _mths, _tokcat = _cats)
    
    if _verbose:
        log("NFT - User Montly Pair Table finished running ...")
    
    pass


def collection_ts_runner(trx_folder, nftkpi_folder, colldistu_folder, collts_folder, tokencats, load_interval_intmonat=[], 
                         malist=[7, 14, 28], _estcalc=False, _verbose=False):
    """
    Reads the transactions, the NFT KPIs, and the collection reports from the previous month, and create a collection time series
    (daily KPIs for each collection for each day in a month).

    Inputs: 
     * trx_folder: input folder of transaction files
     * nftkpi_folder: path of the NFT KPI table's folder
     * colldistu_folder: path of the collection/owner distinct historical table (basically a helper table)
     * collts_folder: path of the newly created collection time series table's folder
     * load_interval_intmonat: between these months the loader will load the DM table. If empty list is given, 
                               it will run for all the transaction files.
     * tokencats: the categories of tokens the loader is running for (list)
     * malist: the list of days which are used for moving averages (default 7, 14, 28 - 1 week / 2 weeks / 1 month)
     * _estcalc: if True, the p75, p25, std and average values will be not accurate, but it needs less resource.
                 Keep it False, if possible (later on, some other solutions can be introduced if there are too many trxs).
    """
    
    if _verbose:
        log("NFT - NFT KPI monthly loader started running ...")
    
    for _cats in tokencats:

        # generating month list
        if (len(load_interval_intmonat) > 0):
            _basic_mth_list = _monat_list_creator(load_interval_intmonat, 0)
        else:
            _basic_mth_list = listdir(trx_folder + '{}/'.format(_cats.lower()))
            _basic_mth_list = [int(_item.split('.')[0].split('_')[-1]) for _item in _basic_mth_list]
        
        if _verbose:
            log("{} category is running {} month(s) from {} to {}".format(_cats, len(_basic_mth_list), _basic_mth_list[0], _basic_mth_list[-1]))
        
        # run (serial) the DM loader
        for _mths in _basic_mth_list:
            _DML_collectionTimeSeries(pathfolder_nftkpi=nftkpi_folder + '{}/'.format(_cats.lower()),
                                      pathfolder_trx=trx_folder + '{}/'.format(_cats.lower()),
                                      pathfolder_colldistuser=colldistu_folder + '{}/'.format(_cats.lower()),
                                      pathfolder_collection_ts=collts_folder + '{}/'.format(_cats.lower()),
                                      actmonat=_mths, malist=malist, _estcalc=_estcalc)
    
    if _verbose:
        log("NFT - User Montly Pair Table finished running ...")
    
    pass