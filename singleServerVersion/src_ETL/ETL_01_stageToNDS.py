##################################################################
# ETL: Loading stage chunks, unify them, and load the            #
# results to NDS                                                 #
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

# utility functions
from .util import log


##################################################################
#                        Helper Functions                        #
##################################################################


def _addTrxToNDS(path_partial_file, path_output_main):
    """
    Adding transactions to the monthly transaction files. 
    (this is a slow, non-parallel version, but it can handle bigger input files)

    The output path contains the transactions in subfolders by category (e.g. art, games, etc.) 
    and in separate picke files by month ..._yyyymm. (in the pyspark version, these will be the partitions)
    """

    in_df_trx = pd.read_pickle(path_partial_file)
    in_df_trx['tr_monat'] = in_df_trx.trx_time_sec.dt.year * 100 + in_df_trx.trx_time_sec.dt.month
    in_df_trx['trx_duplication_flg'] = 0 # later it will be filled (next layer)

    # splitting the table by category
    for _cats in list(in_df_trx.token_category.unique()):

        _tmp_df = in_df_trx[in_df_trx.token_category == _cats]

        # splitting the table by month
        for _monats in list(_tmp_df.tr_monat.unique()):

            # checking if there is a table already: if yes, appending, if no, writing out - with dropping duplicates
            if not path.exists(path_output_main + 'transactions/' + _cats.lower() + '/'):
                makedirs(path_output_main + 'transactions/' + _cats.lower() + '/')
            if not path.exists(path_output_main + 'transactions/' + _cats.lower() + '/trx_{}.pickle'.format(int(_monats))):
                _exp_df = _tmp_df[_tmp_df.tr_monat == _monats].drop(columns=['tr_monat']).reset_index(drop=True)
                _exp_df = _exp_df.drop_duplicates(subset=['collection_ID', 'token_ID', 'trx_seller_address', 'trx_buyer_address', 'trx_time_sec'])
                _exp_df.to_pickle(path_output_main + 'transactions/' + _cats.lower() + '/trx_{}.pickle'.format(int(_monats)))
            else:
                _old_df = pd.read_pickle(path_output_main + 'transactions/' + _cats.lower() + '/trx_{}.pickle'.format(int(_monats)))
                _old_df = _old_df.append(_tmp_df[_tmp_df.tr_monat == _monats].drop(columns=['tr_monat'])).drop_duplicates().reset_index(drop=True)
                _old_df = _old_df.drop_duplicates(subset=['collection_ID', 'token_ID', 'trx_seller_address', 'trx_buyer_address', 'trx_time_sec'])
                _old_df.to_pickle(path_output_main + 'transactions/' + _cats.lower() + '/trx_{}.pickle'.format(int(_monats)))
    pass


def _addTokensToNDS(path_partial_file, path_output_main):
    """
    Adding tokens to the token master files (separated by token category). 
    (this is a slow, non-parallel version, but it can handle bigger input files)
    """

    in_df_token = pd.read_pickle(path_partial_file)
    _colmap = list(in_df_token.columns)

    # splitting the table by category
    for _cats in list(in_df_token.token_category.unique()):
        
        # checking if there is a table already: if yes, appending, if no, writing out
        if not path.exists(path_output_main + 'tokens/'):
            makedirs(path_output_main + 'tokens/')
        if not path.exists(path_output_main + 'tokens/token_master_' + _cats.lower() + '.pickle'):
            in_df_token[in_df_token.token_category == _cats][_colmap].to_pickle(path_output_main + 'tokens/token_master_' + _cats.lower() + '.pickle')
        
        else:

            # reading the existing file and check the common tokens (the others can be written out)
            _idc = ['collection_ID', 'token_ID']
            old_token_df = pd.read_pickle(path_output_main + 'tokens/token_master_' + _cats.lower() + '.pickle')
            _id_old = old_token_df[_idc].drop_duplicates()
            _id_old['flg_old'] = 1
            _id_new = in_df_token[in_df_token.token_category == _cats][_idc].drop_duplicates()
            _id_new['flg_new'] = 1
            _planmaster = pd.merge(_id_old, _id_new, on=_idc, how='outer').fillna({'flg_old':0, 'flg_new':0})
            _planmaster['role_nm'] = np.select([((_planmaster.flg_old == 1) & (_planmaster.flg_new == 1)), 
                                                (_planmaster.flg_old == 1), (_planmaster.flg_new == 1)], 
                                               ['common', 'only_old', 'only_new'], default='xx_NA')
            
            # handling the situation if there is overlap (selecting the first/last transactions)
            if (_planmaster[_planmaster.role_nm == 'common'].shape[0] > 0):
                pdf_keepold = pd.merge(old_token_df, _planmaster[_planmaster.role_nm == 'only_old'][_idc], on=_idc)
                pdf_keepnew = pd.merge(in_df_token[in_df_token.token_category == _cats], 
                                    _planmaster[_planmaster.role_nm == 'only_new'][_idc], on=_idc)
                pdf_common_old = pd.merge(old_token_df, _planmaster[_planmaster.role_nm == 'common'][_idc], on=_idc)

                _new_kc = ['token_mint_crypto', 'token_mint_price_USD', 'token_mint_price_crypto', 'token_mint_address_est', 
                           'token_mint_date_est', 'token_latest_sales_crypto', 'token_latest_sales_date', 
                           'token_latest_price_USD', 'token_latest_price_crypto']
                _new_rename = dict([(_item, _item + '_n') for _item in _new_kc])
                _new_kc = _idc + _new_kc
                pdf_common_new = pd.merge(in_df_token[in_df_token.token_category == _cats][_new_kc].rename(columns=_new_rename), 
                                        _planmaster[_planmaster.role_nm == 'common'][_idc], on=_idc)

                # deciding the date dependent features (in the common table)
                pdf_common_old = pd.merge(pdf_common_old, pdf_common_new, on=_idc)
                if (pdf_common_old.shape[0] != pdf_common_new.shape[0]):
                    print("ID error: duplications in the source tables!")
                
                for _vars in ['token_mint_crypto', 'token_mint_price_USD', 'token_mint_price_crypto', 'token_mint_address_est', 'token_mint_date_est']:
                    # with this solution, the date should be the last to rewrite :)
                    pdf_common_old[_vars] = np.select([pdf_common_old.token_mint_date_est > pdf_common_old.token_mint_date_est_n], 
                                                      [pdf_common_old[_vars + '_n']], default=pdf_common_old[_vars])
                for _vars in ['token_latest_sales_crypto', 'token_latest_price_USD', 'token_latest_price_crypto', 'token_latest_sales_date']:
                    # with this solution, the date should be the last to rewrite :)
                    pdf_common_old[_vars] = np.select([pdf_common_old.token_latest_sales_date_n > pdf_common_old.token_latest_sales_date], 
                                                      [pdf_common_old[_vars + '_n']], default=pdf_common_old[_vars])

                _export_df = pdf_common_old[_colmap].append(pdf_keepold).append(pdf_keepnew).reset_index(drop=True)
            
            else:
                _export_df = old_token_df.append(in_df_token[in_df_token.token_category == _cats]).reset_index(drop=True)

            # writing out the non-overlapping and overlapping tables
            _export_df[_colmap].to_pickle(path_output_main + 'tokens/token_master_' + _cats.lower() + '.pickle')
    pass




##################################################################
#                         Main  Functions                        #
##################################################################

def stageToNDS(in_path_stage, out_path_NDS, _verbose=False):
    """
    Running the token and trx functions for all the files in the staging area in series.
    """

    if _verbose:
        log("Transaction data loader started running (stage to NDS)...")
    for _files in listdir(in_path_stage + 'trx/'):
        _addTrxToNDS(path_partial_file=in_path_stage + 'trx/' + _files, path_output_main=out_path_NDS)
    
    if _verbose:
        log("Token master loader started running (stage to NDS)...")
    for _files in listdir(in_path_stage + 'token/'):
        _addTokensToNDS(path_partial_file=in_path_stage + 'token/' + _files, path_output_main=out_path_NDS)
    
    if _verbose:
        log("Finished running...")
    
    pass
