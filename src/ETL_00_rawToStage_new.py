##################################################################
#            ETL: Loading raw NFT tables to the stage            #
##################################################################
#                                                                #
# Questions to marton.szel@lynxanalytics.com                     #
# Version: 2022-04-17, pandas version                            #
##################################################################

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
import shutil

# parallel programming related
from multiprocessing import Pool
import subprocess

# utility functions
from .util import log

##################################################################
#                        Helper Functions                        #
##################################################################

# ToDo: writing functions to util: write out parquet (by date), read in parquet to pandas (from date interval)

def _wrapOut_innerTables(in_df_orig, ext_col_nm, _prefix=''):
    """
    Wrapping out the "tables" which are "packed" to a column (usual JSON solution) and add the original column 
    name as a prefix (ar any added one).
    """
    
    if len(_prefix) > 0:
        _prefix_used = _prefix + '_'
    else:
        _prefix_used = ext_col_nm + '_'

    out_df = in_df_orig[ext_col_nm].apply(pd.Series)
    _rename_dict = dict([(_c, _prefix_used + _c) for _c in list(out_df.columns)])

    return out_df.rename(columns=_rename_dict)


def _processOneFile(path_in_file_pkl, path_out_trx_folder_pq, path_out_token_folder_pq, 
                    path_out_collection_folder_pq, _verbose=False):
    """
    Read and preprocess a given file from the "dump" area, and load it to the stage as daily partitioned parquet
    files. It generates 3 files from each input: a transaction, a token and a collection file.

    It handles the bundled transactions (filling a trx_value_usd_bundle column with the value and leave 
    empty the trx_value_usd field - later on adding a weighted price by the latest sales price).
    """

    pdf_event_00 = pd.read_pickle(path_in_file_pkl)
    _kc = list(pdf_event_00.columns) + ['asset_num', 'total_price_bundle']

    # Handling some numeric values
    pdf_event_00['quantity'] = pdf_event_00['quantity'].fillna(0).astype('float')
    pdf_event_00['total_price'] = pdf_event_00['total_price'].fillna(0).astype('float')

    # Handling the bundles (if any)
    if pdf_event_00[(~pdf_event_00.asset_bundle.isnull())].shape[0] > 0:
        pdf_event_nb = pdf_event_00[pdf_event_00.asset_bundle.isnull()].copy() # keep it as it is
        pdf_event_nb['asset_num'] = 1
        pdf_event_nb['total_price_bundle'] = pdf_event_nb.total_price
        
        # adding the bundle table
        pdf_event_b = pdf_event_00[(~pdf_event_00.asset_bundle.isnull())].reset_index(drop=True)
        pdf_bundle = _wrapOut_innerTables(pdf_event_b, 'asset_bundle', 'bundle')
        pdf_bundle['asset_num'] = pdf_bundle.bundle_assets.apply(lambda x: len(x))
        pdf_event_b = pd.concat([pdf_event_b, pdf_bundle[['bundle_assets', 'asset_num']]], axis=1)
        pdf_event_b['total_price_bundle'] = pdf_event_b.total_price

        # wrapping out the bundle part
        pdf_event_b_20 = pd.DataFrame()
        for _col in pdf_event_b.columns.values:
            pdf_event_b_20[_col] = pdf_event_b[_col].repeat(pdf_event_b.asset_num)
        pdf_event_b_20['asset'] = np.hstack(pdf_event_b.bundle_assets)
        pdf_event_b_20['quantity'] = 1 # the original field matches with the asset_num
        pdf_event_b_20['total_price'] = pdf_event_b_20.total_price / pdf_event_b_20.asset_num # it is an average, later might be changed

        pdf_event_10 = pdf_event_nb[_kc].append(pdf_event_b_20[_kc]).reset_index(drop=True)
    else:
        pdf_event_10 = pdf_event_00.copy() # nothing to handle, just add the same columns
        pdf_event_10['asset_num'] = 1
        pdf_event_10['total_price_bundle'] = pdf_event_10.total_price

    # Create partial tables
    pdf_asset = _wrapOut_innerTables(pdf_event_10, 'asset').rename(
        columns={'asset_asset_contract':'asset_contract'})
    pdf_asset_ctr = _wrapOut_innerTables(pdf_asset, 'asset_contract', 'assetdet')
    pdf_collection = _wrapOut_innerTables(pdf_asset, 'asset_collection', 'collection')

    pdf_trxdetails = _wrapOut_innerTables(pdf_event_10, 'transaction', 'trx_det')
    pdf_token = _wrapOut_innerTables(pdf_event_10, 'payment_token')

    pdf_fromacc = _wrapOut_innerTables(pdf_event_10, 'from_account')
    pdf_toacc = _wrapOut_innerTables(pdf_event_10, 'to_account')
    pdf_seller = _wrapOut_innerTables(pdf_event_10, 'seller')
    pdf_winner = _wrapOut_innerTables(pdf_event_10, 'winner_account', 'winner')
    pdf_fromacc_tr = _wrapOut_innerTables(pdf_trxdetails, 'trx_det_from_account', 'from_account_tr')
    pdf_toacc_tr = _wrapOut_innerTables(pdf_trxdetails, 'trx_det_to_account', 'to_account_tr')

    # Handling sellers
    pdf_seller_all = pd.concat([pdf_seller, pdf_fromacc, pdf_fromacc_tr], axis=1)
    if ('seller_address' not in list(pdf_seller_all.columns)) and ('from_account_address' not in list(pdf_seller_all.columns)):
        pdf_seller_all['seller_address'] = ''
        pdf_seller_all['from_account_address'] = ''
    if 'seller_address' not in list(pdf_seller_all.columns):
        pdf_seller_all['seller_address'] = pdf_seller_all.from_account_address
    if 'from_account_address' not in list(pdf_seller_all.columns):
        pdf_seller_all['from_account_address'] = pdf_seller_all.seller_address
    pdf_seller_all['seller_address'] = np.select([pdf_seller_all.seller_address.isnull()],
                                                 [pdf_seller_all.from_account_address], 
                                                 default=pdf_seller_all.seller_address)
    pdf_seller_all = pdf_seller_all[['seller_address', 'from_account_tr_address']].copy()

    # Handling buyers
    pdf_buyer_all = pd.concat([pdf_winner, pdf_toacc, pdf_toacc_tr], axis=1)
    if ('winner_address' not in list(pdf_seller_all.columns)) and ('to_account_address' not in list(pdf_seller_all.columns)):
        pdf_seller_all['winner_address'] = ''
        pdf_seller_all['to_account_address'] = ''
    if 'winner_address' not in list(pdf_buyer_all.columns):
        pdf_buyer_all['winner_address'] = pdf_buyer_all.to_account_address
    if 'to_account_address' not in list(pdf_buyer_all.columns):
        pdf_buyer_all['to_account_address'] = pdf_buyer_all.winner_address
    pdf_buyer_all['buyer_address'] = np.select([pdf_buyer_all.winner_address.isnull()],
                                               [pdf_buyer_all.to_account_address], 
                                               default=pdf_buyer_all.winner_address)
    pdf_buyer_all = pdf_buyer_all[['buyer_address', 'to_account_tr_address']].copy()

    # Handling transaction amount, calculating transaction price
    pdf_trx_amt = pd.concat([pdf_event_10[['total_price', 'total_price_bundle']], pdf_token], axis=1)
    pdf_trx_amt['wrong_flg'] = np.select([pdf_trx_amt.payment_token_decimals.isnull()], [1], default=0)
    pdf_trx_amt['payment_token_usd_price'] = pdf_trx_amt['payment_token_usd_price'].fillna(0).astype('float')
    pdf_trx_amt['payment_token_eth_price'] = pdf_trx_amt['payment_token_eth_price'].fillna(0).astype('float')
    pdf_trx_amt['payment_token_decimals'] = pdf_trx_amt['payment_token_decimals'].fillna(0).astype('float')
    pdf_trx_amt['total_price'] = pdf_trx_amt['total_price'].fillna(0).astype('float')
    pdf_trx_amt['total_price_bundle'] = pdf_trx_amt['total_price_bundle'].fillna(0).astype('float')

    pdf_trx_amt['trx_value_crypto'] = pdf_trx_amt.total_price / 10 ** pdf_trx_amt.payment_token_decimals
    pdf_trx_amt['trx_value_usd'] = pdf_trx_amt.trx_value_crypto * pdf_trx_amt.payment_token_usd_price
    pdf_trx_amt['trx_value_eth'] = pdf_trx_amt.trx_value_crypto * pdf_trx_amt.payment_token_eth_price
    pdf_trx_amt['trx_value_crypto_bundle'] = pdf_trx_amt.total_price_bundle / 10 ** pdf_trx_amt.payment_token_decimals
    pdf_trx_amt['trx_value_usd_bundle'] = pdf_trx_amt.trx_value_crypto_bundle * pdf_trx_amt.payment_token_usd_price
    pdf_trx_amt['trx_value_eth_bundle'] = pdf_trx_amt.trx_value_crypto_bundle * pdf_trx_amt.payment_token_eth_price
    
    pdf_trx_amt['payment_token_symbol'] = np.select([pdf_trx_amt.wrong_flg > 0], ['xx_NA'], default=pdf_trx_amt.payment_token_symbol)
    pdf_trx_amt = pdf_trx_amt[['payment_token_symbol', 'trx_value_crypto', 'trx_value_usd', 'trx_value_eth', 
                               'trx_value_crypto_bundle', 'trx_value_usd_bundle', 'trx_value_eth_bundle']].rename(
        columns={'payment_token_symbol':'trx_token_symbol'})

    # Append the partial tables (and filter/rename columns) --> trx table
    _kc_trx = ['asset_num', 'quantity', 'auction_type']
    _kc_trx_coll = ['collection_slug', 'collection_name']
    _kc_trx_asset = ['asset_id', 'asset_token_id', 'asset_name', 'asset_permalink']
    _kc_trx_asset_det = ['assetdet_symbol', 'assetdet_asset_contract_type', 'assetdet_address']
    _kc_trx_paym = ['trx_token_symbol', 'trx_value_crypto', 'trx_value_usd', 'trx_value_eth', 
                   'trx_value_crypto_bundle', 'trx_value_usd_bundle', 'trx_value_eth_bundle']
    _kc_trx_det = ['trx_det_timestamp']
    _kc = (_kc_trx_coll + _kc_trx_asset + _kc_trx_asset_det + ['seller_address', 'from_account_tr_address', 'buyer_address', 'to_account_tr_address'] + 
           _kc_trx_det + _kc_trx + _kc_trx_paym)

    _kc_coll = _kc_trx_coll + ['collection_created_date', 'collection_description', 'collection_external_url', 'collection_twitter_username',
                               'collection_instagram_username', 'collection_wiki_url', 'collection_safelist_request_status', 'collection_image_url', 
                               'collection_banner_image_url', 'collection_is_nsfw', 'collection_require_email', 'collection_only_proxied_transfers', 
                               'collection_is_subject_to_whitelist', 'collection_hidden', 'collection_featured', 'collection_default_to_fiat', 
                               'collection_dev_buyer_fee_basis_points', 'collection_dev_seller_fee_basis_points', 'collection_payout_address',
                               'collection_opensea_buyer_fee_basis_points', 'collection_opensea_seller_fee_basis_points', 'collection_discord_url']

    _kc_token = _kc_trx_coll + _kc_trx_asset + _kc_trx_asset_det + ['asset_description', 'assetdet_total_supply', 'asset_token_metadata', 'asset_is_nsfw', 'asset_background_color', 
                                                                    'asset_image_url', 'asset_image_original_url', 'asset_animation_url', 'asset_animation_original_url', 
                                                                    'assetdet_payout_address', 'assetdet_description', 'assetdet_external_link', 'trx_det_timestamp']

    # Writing out the trx table as a partitioned parquet
    # Appending subtables, fixing data types, creating partitioning fields, writing out the table
    pdf_trx_final = pd.concat([pdf_collection[_kc_trx_coll], pdf_asset[_kc_trx_asset], pdf_asset_ctr[_kc_trx_asset_det], 
                               pdf_seller_all, pdf_buyer_all, pdf_event_10[_kc_trx], pdf_trxdetails[_kc_trx_det], 
                               pdf_trx_amt[_kc_trx_paym]], axis=1)[_kc].reset_index(drop=True)
    _rename_dict = {'asset_num':'trx_asset_num', 'quantity':'trx_quantity', 'auction_type':'trx_auction_type', 'trx_det_timestamp':'trx_timestamp', 
                    'assetdet_symbol':'asset_symbol', 'assetdet_asset_contract_type':'asset_contract_type', 'assetdet_address':'asset_token_address'}
    pdf_trx_final = pdf_trx_final.rename(columns=_rename_dict)
    pdf_trx_final['trx_timestamp'] = pd.to_datetime(pdf_trx_final['trx_timestamp'])
    pdf_trx_final['year'] = pdf_trx_final.trx_timestamp.dt.year
    pdf_trx_final['month'] = pdf_trx_final.trx_timestamp.dt.month
    pdf_trx_final['day'] = pdf_trx_final.trx_timestamp.dt.day
    
    if not path.exists(path_out_trx_folder_pq):
        makedirs(path_out_trx_folder_pq)
    pdf_trx_final.to_parquet(path_out_trx_folder_pq, partition_cols=['year', 'month', 'day'])
    # the default is append ... the out-function will handle the overwrite case (with deleting files)

    # Saving the partial token table
    pdf_asset_20 = pd.concat([pdf_collection[_kc_trx_coll], pdf_asset, pdf_asset_ctr, pdf_trxdetails[_kc_trx_det]], axis=1)[_kc_token].reset_index(drop=True)
    pdf_asset_20['asset_first_trx_dt'] = pd.to_datetime(pdf_asset_20['trx_det_timestamp']).dt.date
    pdf_asset_20 = pdf_asset_20.drop(['trx_det_timestamp'], axis=1)
    pdf_asset_20 = pdf_asset_20.sort_values(by='asset_first_trx_dt', ascending=False)
    pdf_asset_20 = pdf_asset_20.groupby(['asset_id', 'collection_slug', 'asset_token_id']).last().reset_index()

    pdf_asset_20['collection_category'] = 'all' # later on, we might find the good one
    if not path.exists(path_out_token_folder_pq):
        makedirs(path_out_token_folder_pq)
    pdf_asset_20.to_parquet(path_out_token_folder_pq, partition_cols=['collection_category', 'collection_slug'])


    # Saving the partial collection table
    # Appending subtables, aggregating table, fixing data types, creating partitioning fields, writing out the table
    pdf_collection = pdf_collection[_kc_coll]
    pdf_collection['collection_created_date'] = pd.to_datetime(pdf_collection['collection_created_date']).dt.date
    pdf_collection = pdf_collection.groupby(['collection_slug', 'collection_name', 'collection_created_date']).first().reset_index()
    pdf_collection['collection_category'] = 'all' # later on, we might find the good one
    if not path.exists(path_out_collection_folder_pq):
        makedirs(path_out_collection_folder_pq)
    pdf_collection.to_parquet(path_out_collection_folder_pq, partition_cols=['collection_category'])

    pass

def _processOneFile_threadWrapper(in_dict_params):
    """
    Running the _processOneFile() function from an input dictionary - for parallel running.
    """

    _processOneFile(**in_dict_params) # later on, some error handling might be needed here

    pass


def StageLoader(path_in_folder_list, path_out_trx_folder_pq, path_out_token_folder_pq, path_out_collection_folder_pq, 
                path_io_meta_folder, _mode='append', _njobs=2, _verbose=False):
    """
    Pre-processing the files (with the prefix of eventresponse_) within the list of folder names given.
    It uses the _processOneFile() function, running it parallel (on njobs=x CPUs).

    In the append mode, it considers only those files, which has not been processed yet, while the 
    overwrite mode deletes the existing content and processing all inputs (using a csv file in a _meta 
    subfolder).
    """

    # Calculating which files to process
    # - loading the list and creating a df

    _list_of_files = []
    for _dirs in path_in_folder_list:
        _list_of_files = _list_of_files + [_dirs + i for i in listdir(_dirs) if ((i.count('.pickle') > 0) and (i.count('eventresponse_') > 0))]
    df_meta_toLoad = pd.DataFrame({'path_in_files':_list_of_files})
    df_meta_toLoad['file_id'] = df_meta_toLoad.path_in_files.apply(lambda x: '_'.join(x.split('.pickle')[0].split('_')[-2:]))
    
    # - checking if the file is processed already - depending on the mode (if overwrite, preprocess anyway / delete folders)
    if _mode == 'overwrite':
        pdf_meta_final = df_meta_toLoad[['file_id']].copy()
        _runList = _list_of_files
        if path.exists(path_out_trx_folder_pq):
            shutil.rmtree(path_out_trx_folder_pq)
        if path.exists(path_out_token_folder_pq):
            shutil.rmtree(path_out_token_folder_pq)
        if path.exists(path_out_collection_folder_pq):
            shutil.rmtree(path_out_collection_folder_pq)
    
    else: # mode append
        if path.exists(path_io_meta_folder + '01_stage_loader.csv'):
            pdf_oldmeta = pd.read_csv(path_io_meta_folder + '01_stage_loader.csv')
            pdf_oldmeta['exist_flg'] = 1
            pdf_meta_final = pd.merge(pdf_oldmeta, df_meta_toLoad, how='outer', on=['file_id']).fillna({'exist_flg':0})
            _runList = list(pdf_meta_final[(pdf_meta_final.exist_flg <= 0) & 
                                           (~pdf_meta_final.path_in_files.isnull())].path_in_files.unique())
            pdf_meta_final = pdf_meta_final[['file_id']]
        else:
            pdf_meta_final = df_meta_toLoad[['file_id']].copy()
            _runList = _list_of_files
    
    # Preprocessing files from the list
    # - create the list of input dictionaries
    _rundictlist = []
    for _file in _runList:
        _rundictlist = _rundictlist + [{'path_in_file_pkl':_file, 'path_out_trx_folder_pq':path_out_trx_folder_pq, 
                                        'path_out_token_folder_pq':path_out_token_folder_pq, 'path_out_collection_folder_pq':path_out_collection_folder_pq, 
                                        '_verbose':_verbose}]

    # - run the preprocessing on multi threads
    with Pool(_njobs) as p:
        p.map(_processOneFile_threadWrapper, _rundictlist)
    p.close()
    
    # writing out the new meta file
    if not path.exists(path_io_meta_folder):
        makedirs(path_io_meta_folder)
    pdf_meta_final.to_csv(path_io_meta_folder + '01_stage_loader.csv', index=False)
    
    pass



# ToDo: in the NDS loader, handle the "duplicated" transactions (where 1st item is the sales and the
# 2nd is some internal transfer - from accounts are different in transaction column / from account)
# Example: https://opensea.io/assets/0xdd0d63220265927b29646cb7d4684d5fa6df4cb8/57
# Also, correct the related trx addresses (buyer/seller)

# ToDo: check if the ETH-USD time series is correct