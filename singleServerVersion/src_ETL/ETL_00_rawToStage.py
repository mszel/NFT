##################################################################
#            ETL: Loading raw NFT tables to the stage            #
##################################################################
#                                                                #
# Questions to marton.szel@lynxanalytics.com                     #
# Version: 2022-01-31                                            #
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

# parallel programming related
from multiprocessing import Pool
import subprocess

# utility functions
from .util import log


##################################################################
#                        Helper Functions                        #
##################################################################

def _runPlanner(batchsize, input_folder, input_file=''):
    """
    Check the size of the input file(s) and give back a list of parameter dictionaries 
    (filename, from_row, rownum). These dictionaries can be extended with further parameters
    by another function.

    Inputs: 
     * batchsize: maximum rows to be read
     * input_folder: the folder where the file(s) are stored
     * input_file: the name of the file to be read (if empty, all to read)
    """

    paramlist_prun = []
    if len(input_file) > 0:
        _tocheck_files = [input_file]
    else:
        _tocheck_files = listdir(input_folder)
    
    for _files in _tocheck_files:
        _size = len(open(input_folder + _files, encoding="utf8").readlines())
        _start_size = 0
        while (_start_size < _size):
            _toread = min(batchsize, _size - _start_size)
            paramlist_prun = paramlist_prun + [{"path":input_folder + _files, "start_row":_start_size, "to_read":_toread}]
            _start_size = _start_size + batchsize
    return paramlist_prun


def _addParams(list_of_dictionaries, additional_dictionary):
    """
    Updating the dictionaries (in a list) with additional parameters
    """
    for _d in list_of_dictionaries:
        _d.update(additional_dictionary)
    
    return list_of_dictionaries


def _ID_correction(in_df_chunk):
    """
    Correcting the ID field of the given partial transaction table. 
    The ID will be the category + unique collection ID + token ID.
    The ID will be a string field (a bit slower of processing, but handles all the size issues + duplications)
    """

    _tmp_df = in_df_chunk.copy()
    _tmp_df['collection_ID'] = _tmp_df.Unique_id_collection.apply(lambda x: eval(x)[0]).astype(str)
    _tmp_df['ID_check'] = _tmp_df.Unique_id_collection.apply(lambda x: str(eval(x)[1]).split('.')[0]).astype(str)
    _tmp_df['ID_token'] = _tmp_df.ID_token.apply(lambda x: x.split('.')[0])
    _tmp_df['error_ID'] = np.select([_tmp_df.ID_check != _tmp_df.ID_token], [1], default=0)
    _tmp_df['token_ID'] = _tmp_df.collection_ID + '_' + _tmp_df.ID_token
    
    return _tmp_df.drop(columns=['Unique_id_collection', 'ID_token', 'ID_check'])


def _collectTokenFeatures(in_df_chunk):
    """
    Creates the base of the token feature table, including:
     - collection_id, token id, contract (K)
     - collection_id, collection_nm_cleaned, latest_URL
     - first transaction datetime (in the chunk, later aggregated)
     - last transaction datetime (in the chunk, later aggregated)
     - first seller (in this dataset, most of the time they are the minters as the mint 
       transactions are not recorder) and buyer (a kind of gallery / collector)
     - first price USD (in the chunk, later aggregated)
     - last price USD (in the chunk, later aggregated)
     - category
    Note: one token can appear on several markets, so market is removed among keys.
    """

    # adding features
    _tmp_df = in_df_chunk.copy()
    _tmp_df['last_URL'] = np.select([(_tmp_df.Image_url_4.str.count('http') > 0), (_tmp_df.Image_url_3.str.count('http') > 0), 
                                     (_tmp_df.Image_url_2.str.count('http') > 0)],
                                    [_tmp_df.Image_url_4, _tmp_df.Image_url_3, _tmp_df.Image_url_2], 
                                    default=_tmp_df.Image_url_1)

    # sort values for aggregation
    _tmp_df = _tmp_df.sort_values(by=['collection_ID', 'token_ID', 'Datetime_updated_seconds'])

    # adding the first transactions (and all other features - trx. related to the first sales)
    # only first/last is used, as max runs for too long (e.g., on name and description)
    
    # _keyrename = {'Market':'Market_ID'}
    pdf_tokenbase = _tmp_df.groupby(['collection_ID', 'token_ID']).agg(
        token_URL_latest = ('last_URL', 'last'), 
        token_permanent_link = ('Permanent_link', 'last'),
        token_mint_crypto = ('Crypto', 'first'), 
        token_mint_price_USD = ('Price_USD', 'first'), 
        token_mint_price_crypto = ('Price_Crypto', 'first'),
        token_mint_address_est = ('Seller_address', 'first'), 
        token_mint_date_est = ('Datetime_updated_seconds', 'first'),
        token_latest_sales_crypto = ('Crypto', 'last'), 
        token_latest_sales_date = ('Datetime_updated_seconds', 'last'), 
        token_latest_price_USD = ('Price_USD', 'last'), 
        token_latest_price_crypto = ('Price_Crypto', 'last'), 
        token_collection_nm = ('Collection', 'last'), 
        token_name = ('Name', 'last'), 
        token_description = ('Description', 'last'), 
        token_category = ('Category', 'first'),
        token_ID_error = ('error_ID', 'sum')
    ).reset_index() #.rename(columns=_keyrename)
    
    return pdf_tokenbase


def _collectTrxFeatures(in_df_chunk):
    """
    Creating a base table for transactions, containing the:
     - token ID
     - seller, buyer, date, price, crypto
     - does not contain the trx hash etc., neither the token info (it would make the table too big for analysis)
    """
    
    # keeping the necessary columns, renaming columns
    _kc = ['Market', 'collection_ID', 'token_ID', 'Category', 'Datetime_updated_seconds', 'Seller_address', 
           'Buyer_address', 'Crypto', 'Price_Crypto', 'Price_USD']
    _rename_dict = {'Market':'Market_ID', 'Category':'token_category', 'Datetime_updated_seconds':'trx_time_sec', 
                    'Seller_address':'trx_seller_address', 'Buyer_address':'trx_buyer_address', 
                    'Crypto':'trx_crypto', 'Price_Crypto':'trx_value_crypto', 'Price_USD':'trx_value_usd'}
    pdf_trx = in_df_chunk[_kc].rename(columns=_rename_dict)

    # handling duplicates (if any), and return - next layers will add some further filters
    return pdf_trx.drop_duplicates()


##################################################################
#                         Main  Functions                        #
##################################################################

def ReadOneChunk(in_dict):
    """
    Read one part of the input file (from start_row, readng the first to_read lines). After preprocess 
    the table and save them to the different temporary tables. The input parameters are stored in a 
    dictionary, so the parallel computation can be applied.

    Inputs: 
     * path: file path to read
     * start_row: readng the file from start_row
     * to_read: reading the first to_read lines from start_row
    
    There is no output file (all are written out to the stage folder)
    """

    _infile = in_dict['path'].split('/')[-1].split('.')[0]
    
    _dtypes = {'Smart_contract': str, 'ID_token': str, 'Transaction_hash': str,
               'Seller_address': str, 'Seller_username': str, 'Buyer_address': str,
               'Buyer_username': str, 'Image_url_1': str, 'Image_url_2': str,
               'Image_url_3': str, 'Image_url_4': str, 'Price_Crypto': np.float32,
               'Crypto': str, 'Price_USD': np.float32, 'Name': str,
               'Description': str, 'Collection': str, 'Market': str,
               'Datetime_updated': str, 'Datetime_updated_seconds': str, 'Permanent_link': str, 
               'Unique_id_collection': str, 'Collection_cleaned': str, 'Category': str}
        
    # reading the csv file between the limits
    _header =  list(pd.read_csv(in_dict['path'], skiprows=0, nrows=1).columns)   # <fixme: header as input, if missing use this>
    pdf_rawtable_00 = pd.read_csv(in_dict['path'], skiprows = in_dict['start_row'] + 1, nrows = in_dict['to_read'], 
                                  names=_header, encoding='utf8', dtype=_dtypes)
    
    # filtering the table (if necessary)
    if (len(in_dict['categories']) > 0):
        pdf_rawtable_00 = pdf_rawtable_00[pdf_rawtable_00.Category.isin(in_dict['categories'])].copy()
    
    # correcting IDs
    pdf_rawtable_00 = _ID_correction(pdf_rawtable_00)
    
    # casting / cleansing some variables
    pdf_rawtable_00['Datetime_updated_seconds'] = pd.to_datetime(pdf_rawtable_00['Datetime_updated_seconds'])

    # creating and writing out the token table
    pdf_token_features = _collectTokenFeatures(pdf_rawtable_00)
    if not path.exists(in_dict['out_path_base'] + 'token/'):
        makedirs(in_dict['out_path_base'] + 'token/')
    pdf_token_features.to_pickle(in_dict['out_path_base'] + 'token/{}_{}_{}.pickle'.format(
        _infile, in_dict['start_row'], in_dict['to_read']))
    
    # creating and writing out the transaction table
    pdf_trx_details = _collectTrxFeatures(pdf_rawtable_00)
    if not path.exists(in_dict['out_path_base'] + 'trx/'):
        makedirs(in_dict['out_path_base'] + 'trx/')
    pdf_trx_details.to_pickle(in_dict['out_path_base'] + 'trx/{}_{}_{}.pickle'.format(
        _infile, in_dict['start_row'], in_dict['to_read']))
    
    pass


def ReadAllFiles_parallel(input_folder, input_file, batchsize, category_list, output_folder, cln_flg, njobs, _verbose=False):
    """
    Reads all raw files from a given dictionary, and load them to the staging area (in chunks).
    The ETL can run parallel (number of executors are marked with the "njobs" input parameter).

    The following inputs are used:
     * batchsize: maximum rows to be read in one chunk (default value is 500k)
     * input_folder: the folder where the file(s) are stored
     * input_file: the name of the file to be read (if empty, all to read)
     * category_list: list of categories to be load from the input file (ike Art, Games, Metaverse, Collectibles, etc.)
     * output_folder: the main output folder where the results should be stored
     * cln_flg: if it is True, the output folder will be deleted at the beginning of the code 
       (so there will be no duplicates caused by leftover old files)
    """

    if _verbose:
        log("Data preprocessing started running (raw to stage)...")

    # calculating execution plan
    if _verbose:
        log("Planning the parallel execution...")
    _paramlist = _runPlanner(batchsize=batchsize, input_folder=input_folder, input_file=input_file)
    
    # adding the further necessary parameters to the given parameter dictionary
    _paramlist = _addParams(_paramlist, {'out_path_base':output_folder, 'categories':category_list})

    # cleaning the directory if necessary
    _clrFolders = ['trx', 'token']
    if cln_flg:
        if _verbose:
            log("Deleting temporary files from the target folder...")
        for _delFolders in _clrFolders:
            if path.exists(output_folder + _delFolders + '/'):
                for _delFiles in listdir(output_folder + _delFolders + '/'):
                    if _delFiles.count('.pickle') > 0:
                        remove(output_folder + _delFolders + '/' + _delFiles)
    
    # calling the chunk procesor (parallel)
    if _verbose:
        log("Starting parallel execution...")
    with Pool(njobs) as p:
        p.map(ReadOneChunk, _paramlist)
    p.close()

    if _verbose:
        log("Finished running...")

    pass