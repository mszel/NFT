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

# core libraries
from datetime import datetime, timedelta
import imp
import time

# OS related
from os import listdir, makedirs, remove, path


##################################################################
#                Commonly used utility functions                 #
##################################################################

def log(s):
    """ 
    Writing out the time, and print the given string
    """
    print('[' + time.strftime('%a %H:%M:%S') + '] ' + s)
    pass

def _monat_add(_int_monat, _months_to_add):
    """
    Add _months_to_add monats to the original monat (_int_monat). 
    The addition can be both positive / negative
    """
    _add_yearpart = _months_to_add // 12
    _add_monthpart = _months_to_add - 12 * _add_yearpart
    out_monat = _int_monat + 100 * _add_yearpart
    if (out_monat % 100) + _add_monthpart > 12:
        return out_monat + 88 + _add_monthpart
    elif (out_monat % 100) + _add_monthpart < 1:
        return out_monat - 89 + _add_monthpart
    else:
        return out_monat + _add_monthpart

def _monat_list_creator(load_dt_list_str, monat_add=1):
    """
    Input: a list of the first / last dates which we should read from s3 (monat partitioned)
    Output: list of monats we should read (decreasing the first monat with the monat_add parameter)
    """
    _start_monat = _monat_add(_int_monat=int(str(load_dt_list_str[0])[:6]), _months_to_add = -monat_add)
    _end_monat = int(str(load_dt_list_str[1])[:6])
    _out_list = []
    while (_start_monat <= _end_monat):
        _out_list = _out_list + [_start_monat]
        _start_monat = _monat_add(_start_monat, 1)
    return _out_list 

def _collectMonatsFromFilenames(path_folder_chk, int_max_monat):
    """
    Checking the files (trx pickle files) in a given folder, 
    and collects the available months (up to a monat, if it is given). 
    """

    _monats = listdir(path_folder_chk)
    _monats = [int(_item.split('.')[0].split('_')[-1]) for _item in _monats]

    if int_max_monat > 0:
        _monats = [_item for _item in _monats if _item <= int_max_monat]
    
    return _monats

def _monatToDateList(int_monat):
    """
    Gives back a list with all the dates in a month - of the given yyyymm integer format monat.
    """

    _first_date = datetime.strptime(str(int_monat) + '01', '%Y%m%d')
    _first_date_nm = datetime.strptime(str(_monat_add(int_monat, 1)) + '01', '%Y%m%d')
    _dt_diff = _first_date_nm - _first_date
    _dt_diff = int(_dt_diff.days)

    return [(_first_date + timedelta(days=i)).date() for i in range(0, _dt_diff)]

def _colsimpler(colnames):

    _newcol = []
    for _items in colnames:
        if len(_items[0]) > 1:
            _newcol = _newcol + [_items[0]]
        else:
            _newcol = _newcol + [_items]
    
    return _newcol

def _findingLatestFile(path_folder):
    """
    Finding the highest yyyymm postfix in a folder, and returns back with the full file path.
    """

    if str(path_folder)[-1] == '/':
        _pf = path_folder
    else:
        _pf = path_folder + '/'

    _monat_df = pd.DataFrame({'file_rel_path':listdir(_pf)})
    _monat_df['file_path'] = _pf + _monat_df.file_rel_path
    _monat_df['monat'] = _monat_df.file_rel_path.apply(lambda x: int(x.split('.')[0].split('_')[-1]))

    return _monat_df[_monat_df.monat == _monat_df.monat.max()].iloc[0].file_path

def _firstNLastDt(path_folder):
    """
    Getting the first / last report dt from a given folder (usually time series).
    Not the exact one, just form the filenames (which are monthly)
    """

    _monats = listdir(path_folder)
    _monats = [int(_item.split('.')[0].split('_')[-1]) for _item in _monats]
    _monats.sort()

    _first_dt = datetime.strptime(str(_monats[0]) + '01', '%Y%m%d')
    _last_dt = datetime.strptime(str(_monat_add(_monats[-1], 1)) + '01', '%Y%m%d') - timedelta(days=1)

    return [_first_dt.date(), _last_dt.date()]


def FileCollector(path_folder, date_interval=[], filters={}, _dtvar='report_dt'):
    """
    Collecting and appending all files from a folder, which are in between the date interval. The code
    also applies the filter for the given columns. If the date interval is empty, loading all files.
    """

    if str(path_folder)[-1] == '/':
        _pf = path_folder
    else:
        _pf = path_folder + '/'

    _monat_df = pd.DataFrame({'file_rel_path':listdir(_pf)})
    _monat_df['file_path'] = _pf + _monat_df.file_rel_path
    _monat_df['monat'] = _monat_df.file_rel_path.apply(lambda x: int(x.split('.')[0].split('_')[-1]))

    if len(date_interval) > 0:
        _minmonat = date_interval[0].year * 100 + date_interval[0].month
        _maxmonat = date_interval[1].year * 100 + date_interval[1].month
        _monat_df = _monat_df[_monat_df.monat.between(_minmonat, _maxmonat)]
    
    _filelist = list(_monat_df.file_path.tolist())
    _read_pdfs = []

    for _files in _filelist:
        _tmp_df = pd.read_pickle(_files)
        _tmp_df[_dtvar] = pd.to_datetime(_tmp_df[_dtvar])
        for _cols in filters.keys():
            _tmp_df = _tmp_df[_tmp_df[_cols].isin(filters[_cols])]
        _tmp_df = _tmp_df[(_tmp_df[_dtvar].dt.date >= date_interval[0]) & 
                          (_tmp_df[_dtvar].dt.date <= date_interval[1])]
        _read_pdfs = _read_pdfs + [_tmp_df]
    
    return pd.concat(_read_pdfs, axis=0).reset_index(drop=True)
