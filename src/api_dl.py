##################################################################
#           API: Downloading Raw NFT data from Opensea           #
##################################################################
#                                                                #
# Questions to marton.szel@lynxanalytics.com                     #
# Version: 2022-01-31                                            #
##################################################################

##################################################################
#                        Import libraries                        #
##################################################################

# data processing related
import pandas as pd
import numpy as np
import json

# core libraries
from datetime import datetime, timedelta
import time

# OS related
from os import listdir, makedirs, remove, path

# parallel programming related
from multiprocessing import Pool
import subprocess

# downloading related
import requests

# loading some own libraries
from .util import log, _periodFilter


##################################################################
#                        Transactions DL                         #
##################################################################

def _timeInterval_eventDL_oneThread(API_key, filter_dict, time_interval, path_dumpdir, batchsize=300, 
                                    time_interval_unixts=[], _verbose=False, _timeout_limit=0, _existchk=True):
    """
    Downloading the Event Table within the given time range, using the added filters from the 
    given dictionary (should be same as the API).

    Inputs:
     * API_key: the API key the thread can use
     * filter_dict: filter dictionary with the possible filter values
     * time_interval: ['yyyy-mm-dd hh:mm:ss', 'yyyy-mm-dd hh:mm:ss'] strings of the starting and end time
     * batchsize=300: the chunks we're downloding one time (default is opensea API allowed max)
     * path_dumpdir: the directory we're dumping the files
     * time_interval_unixts: the time inteval can be added in unix timestamps. 
       It'll overwrite the time_interval input!
     * _existchk=True: it is not loading the file if it is already available
    """
    
    # dictionary of saving the errors
    _err_dict = {'event_name':[], 'event_input_url':[], 'event_response_dict':[]}

    _runstart = time.time()
    if len(time_interval_unixts) == 0:
        msg_interval=time_interval
    else:
        msg_interval=time_interval_unixts
    
    if _verbose:
        log('Downloading events from {} to {}...'.format(msg_interval[0], msg_interval[1]))
    
    if len(time_interval_unixts) > 0:
        _end_dt = time_interval_unixts[1]
        _start_dt = time_interval_unixts[0]
    else:
        _end_dt = pd.to_datetime(time_interval[1]).timestamp()
        _start_dt = pd.to_datetime(time_interval[0]).timestamp()
    
    if (path.exists(path_dumpdir + 'eventresponse_{}_{}.pickle'.format(int(_start_dt), int(_end_dt)))) and (_existchk):
        if _verbose:
            log("File already exists...")
    else:

        # Downloading and Writing Out the file if does not exist

        _out_df_list = []

        _base_URL = 'https://api.opensea.io/api/v1/events?'

        # adding the filters (creating the base URL)
        for _filter in filter_dict.keys():
            _base_URL = _base_URL + _filter + '=' + filter_dict[_filter] + '&'
        
        # adding the timestamp and the batch size
        _base_URL = _base_URL + 'occurred_before={}&limit={}'.format(int(_end_dt), batchsize)
        _url = _base_URL
        
        # creating a header
        headers = {"Accept": "application/json", "X-API-KEY": API_key}

        _keepRunning = True
        _emptycnt = 0 # counting the empty responses
        
        # the cycle of data requests
        while _keepRunning:

            # request the data
            _response = requests.request("GET", _url, headers=headers)
            if _response.status_code == 200:
                _answer = json.loads(_response.content.decode('utf-8'))
                # check if data is arrived (and not empty)
                if 'asset_events' in list(_answer.keys()):
                    if (_answer['asset_events'] != []) and (_answer['asset_events'] != None):
                        _tmp_df = pd.DataFrame(_answer['asset_events'])
                        # cleansing events with missing trx time (those are anyway non-sucessful)
                        _tmp_df = _tmp_df[(~_tmp_df.transaction.apply(pd.Series).timestamp.isnull())]

                        # checking the min time in the DataFrame (for stopping the cycle)
                        # also, cutting out the part of the DataFrame which is not in the asked time interval
                        _mindt = pd.to_datetime(_tmp_df.iloc[-1].transaction['timestamp']).timestamp()
                        if _mindt < _start_dt:
                            _keepRunning = False
                            print(_tmp_df[(_tmp_df.transaction.apply(pd.Series).timestamp.isnull())].head())
                            _tmp_df = _tmp_df[_tmp_df.transaction.apply(pd.Series).timestamp.apply(lambda x: pd.to_datetime(x).timestamp()) >= _start_dt]
                    else:
                        _tmp_df = pd.DataFrame()
                        _err_dict['event_name'] = _err_dict['event_name'] + ['Empty Asset Event Table']
                        _err_dict['event_input_url'] = _err_dict['event_input_url'] + [_url]
                        _err_dict['event_response_dict'] = _err_dict['event_response_dict'] + [_answer]
                else:
                    _tmp_df = pd.DataFrame()
                    log('Warning: within the {} - {} interval there were no response dataset. The answer dictionary was the following: {}. Used base URL: {}'.format(
                        msg_interval[0], msg_interval[1], str(_answer), _base_URL))
                    _err_dict['event_name'] = _err_dict['event_name'] + ['Response w/o Asset Event Table']
                    _err_dict['event_input_url'] = _err_dict['event_input_url'] + [_url]
                    _err_dict['event_response_dict'] = _err_dict['event_response_dict'] + [_answer]
                
                # check if there is a "next" cursor value
                if 'next' in list(_answer.keys()):
                    if _answer['next'] != None:
                        _url = _base_URL + '&cursor={}'.format(_answer['next'])
                    else:
                        _keepRunning = False
                else:
                    _keepRunning = False
            
            else:
                _tmp_df = pd.DataFrame()
                _emptycnt = _emptycnt + 1
                time.sleep(1)
                if _emptycnt > 3:
                    _err_dict['event_name'] = _err_dict['event_name'] + ['Empty response over 5 times']
                    _err_dict['event_input_url'] = _err_dict['event_input_url'] + [_url]
                    _err_dict['event_response_dict'] = _err_dict['event_response_dict'] + ['Status code: {}'.format(_response.status_code)]
                    _keepRunning = False
            
            _out_df_list = _out_df_list + [_tmp_df]
            
            # checking for timeout event
            if (_timeout_limit > 0) and (time.time() - _runstart > _timeout_limit):
                _keepRunning = False
                log('Error: within the {} - {} interval time out event occurred. Used base URL: {}. Use higher limit, or smaller interval'.format(
                    msg_interval[0], msg_interval[1], _base_URL))
                _err_dict['event_name'] = _err_dict['event_name'] + ['Timeout Event']
                _err_dict['event_input_url'] = _err_dict['event_input_url'] + ['Not Relevant']
                _err_dict['event_response_dict'] = _err_dict['event_response_dict'] + ['Not relevant']
        
        # Handling the errors if any is collected
        if len(_err_dict['event_name']) > 0:
            if not path.exists(path_dumpdir + 'errors/'):
                makedirs(path_dumpdir + 'errors/')
            pd.DataFrame(_err_dict).to_pickle(path_dumpdir + 'errors/runtimeerror_{}_{}.pickle'.format(int(_start_dt), int(_end_dt)))
            if _verbose:
                log('Errors occured at file from {} to {}.'.format(msg_interval[0], msg_interval[1]))
        else:
        # appending and writing out the results to the dumping directory
            out_df = pd.concat(_out_df_list, axis=0).reset_index(drop=True)
            if not path.exists(path_dumpdir):
                makedirs(path_dumpdir)
            out_df.to_pickle(path_dumpdir + 'eventresponse_{}_{}.pickle'.format(int(_start_dt), int(_end_dt)))

            if _verbose:
                log('Downloading finished from {} to {} with creating {} records'.format(msg_interval[0], msg_interval[1], out_df.shape[0]))

    pass


def _timeInterval_eventDL_oneThreadWrapper(in_dict_params):
    """
    Running the _timeInterval_eventDL_oneThread() function from an input dictionary.
    """

    API_key = in_dict_params['API_key']
    filter_dict = in_dict_params['filter_dict']
    time_interval = in_dict_params['time_interval']
    path_dumpdir = in_dict_params['path_dumpdir']
    batchsize = in_dict_params['batchsize']
    time_interval_unixts = in_dict_params['time_interval_unixts']
    _verbose = in_dict_params['_verbose']
    _timeout_limit = in_dict_params['_timeout_limit']
    _existchk=in_dict_params['_existchk']
    
    # _timeInterval_eventDL_oneThread(API_key=API_key, filter_dict=filter_dict, time_interval=time_interval, 
    #                                     path_dumpdir=path_dumpdir, batchsize=batchsize, time_interval_unixts=time_interval_unixts,
    #                                     _verbose=_verbose, _timeout_limit=_timeout_limit, _existchk=_existchk)
    
    try:
        _timeInterval_eventDL_oneThread(API_key=API_key, filter_dict=filter_dict, time_interval=time_interval, 
                                        path_dumpdir=path_dumpdir, batchsize=batchsize, time_interval_unixts=time_interval_unixts,
                                        _verbose=_verbose, _timeout_limit=_timeout_limit, _existchk=_existchk)
    except:
        _err_dict = {'event_name':['Other Runtime Error'], 'event_input_url':['NA'], 'event_response_dict':['NA']}
        if not path.exists(path_dumpdir + 'errors/'):
            makedirs(path_dumpdir + 'errors/')
        if len(time_interval_unixts) > 0:
            _end_dt = time_interval_unixts[1]
            _start_dt = time_interval_unixts[0]
        else:
            _end_dt = pd.to_datetime(time_interval[1]).timestamp()
            _start_dt = pd.to_datetime(time_interval[0]).timestamp()
        pd.DataFrame(_err_dict).to_pickle(path_dumpdir + 'errors/runtimeerror_{}_{}.pickle'.format(int(_start_dt), int(_end_dt)))
    
    pass


def _listrunner_timeInterval_eventDL(list_of_in_dict_params):
    """
    Running Events in a Loop (Serial). As one key can initiate one thread at the time, their requests should
    run serial.
    """

    for in_dict_params in list_of_in_dict_params:
        _timeInterval_eventDL_oneThreadWrapper(in_dict_params)
    pass


def TimeIntervalEventDL(list_of_API_keys, filter_dict, time_interval, time_batch_sec, path_out_dumpdir, 
                        batchsize=300, _timeout_limit=0, _verbose=False, _existchk=True):
    """
    Downloading events within a date period, using multiple API keys (parallel computing). It can handle 
    added filters from the given dictionary (keys should be same as the API's inputs).

    Inputs: 
     * list_of_API_keys: list of the API keys (as many API keys we have, as many parallel threads can run)
     * filter_dict: filter dictionary with the possible filter values (like collection_slug, event_type)
     * time_interval: ['yyyy-mm-dd hh:mm:ss', 'yyyy-mm-dd hh:mm:ss'] strings of the starting and end time
     * time_batch_sec: the time period we're maximum handling with one thread (at one DL session). Before 
       2019, it can be even a week, while after 2022 it should be a few hours maximum.
     * path_out_dumpdir: the main directory we're collecting the files. The program will create a subfolder 
       with the given days in yyyymmdd format and saving the files there.
     * batchsize=300: the chunks we're downloding one time (default is opensea API allowed max)
     * _timeout_limit=0: maximum time in seconds we're allowing a thread running (with one date period). 
       0=infinite.
    """

    # converting dates, calculates the pathnames
    _start_dt = pd.to_datetime(time_interval[0]).timestamp()
    _end_dt = pd.to_datetime(time_interval[1]).timestamp()
    _start_yyyymmdd_hhmmss = time_interval[0].replace(' ', '_').replace('-', '').replace(':', '')
    _end_yyyymmdd_hhmmss = time_interval[1].replace(' ', '_').replace('-', '').replace(':', '')
    _outhpath = path_out_dumpdir + 'events_{}_{}/'.format(_start_yyyymmdd_hhmmss, _end_yyyymmdd_hhmmss)
    
    # calculate the number of time intervals
    _intervals = []
    _ct = _start_dt
    while _ct < _end_dt:
        _intervals = _intervals + [[_ct, _ct + time_batch_sec]]
        _ct = _ct + time_batch_sec
        
    # collecting existing intervals
    _existing_intervals = []
    if path.exists(_outhpath):
        _filepathes = listdir(_outhpath)
        _filepathes = [_f for _f in _filepathes if _f.endswith('.pickle')]
        if len(_filepathes) > 0:
            _existing_intervals = [[int(_f.split('.')[0].split('_')[1]), 
                                    int(_f.split('.')[0].split('_')[2])] for _f in _filepathes]
    
    # calculating the necessary intervals to download
    _intervals = _periodFilter(_intervals, _existing_intervals)

    # planning the download: create inputs for the threads
    _rundict = {}
    _njobs = len(list_of_API_keys)
    for _keys in range(_njobs):
        _rundict.update({_keys:[]})
        
    i = 0
    for _interval in _intervals:
        _key = i % _njobs           # deciding which list we are adding the input
        _rundict[_key] = _rundict[_key] + [{'API_key':list_of_API_keys[_key], 'filter_dict':filter_dict, 'time_interval':[], 
                                            'path_dumpdir':_outhpath, 'batchsize':batchsize,
                                            'time_interval_unixts':_interval, '_verbose':_verbose, 
                                            '_timeout_limit':_timeout_limit, '_existchk':_existchk}]
        i = i + 1
    
    # converting back the dictionary to a list
    list_of_rundict = []
    for _key in _rundict.keys():
        list_of_rundict = list_of_rundict + [_rundict[_key]]
    
    # Download the data based on the running dictionary
    with Pool(_njobs) as p:
        p.map(_listrunner_timeInterval_eventDL, list_of_rundict)
    p.close()

    pass


