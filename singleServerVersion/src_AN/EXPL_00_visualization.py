##################################################################
#   Exploration: Visualization & Analytics related functions     #
##################################################################
#                                                                #
# Questions to marton.szel@lynxanalytics.com                     #
# Version: 2022-02-13                                            #
##################################################################

##################################################################
#                        Import libraries                        #
##################################################################

# loading ETL related libraries
import pandas as pd
import numpy as np

# core libraries
from datetime import datetime, timedelta
import time

# OS related
from os import listdir, makedirs, remove, path

# Visualization
import ipywidgets as widgets
import seaborn as sns
import matplotlib.pyplot as plt

# utility functions
from .util import _findingLatestFile, _firstNLastDt, FileCollector

##################################################################
#                       Helper Functions                         #
##################################################################

def _paramPrepTSVisu(pathfolder_collection_ts, _orderby_cols=['collection_ID'], _asc_cols=[True], _rowshow=10):
    """
    Preparing the widget box selection for the Collection TS Viewer:
     * Multiselect Box for the Collections (order by name, or trx volume)
     * Date Selection (from/to)
     * ... adding more later on (e.g., which reports we want to see...)
    """

    pdf_latest_collts = pd.read_pickle(_findingLatestFile(pathfolder_collection_ts))
    pdf_latest_collts = pdf_latest_collts[pdf_latest_collts.report_dt == pdf_latest_collts.report_dt.max()]
    pdf_latest_collts = pdf_latest_collts.sort_values(by=_orderby_cols, ascending=_asc_cols)
    _coll_col = list(pdf_latest_collts.drop_duplicates(subset=['collection_ID']).collection_ID.to_list())

    _dates = _firstNLastDt(pathfolder_collection_ts)

    collection_filter = widgets.Dropdown(
        options=_coll_col,
        rows=_rowshow,
        description='Collection Names',
        disabled=False
    )
    
    dtbox_from = widgets.DatePicker(
        description='From (date):',
        value=_dates[0],
        disabled=False
    )

    dtbox_to = widgets.DatePicker(
        description='To (date):',
        value=_dates[1],
        disabled=False
    )

    return [collection_filter, dtbox_from, dtbox_to]



##################################################################
#                       Visual Functions                         #
##################################################################

def CollTSViewer(pathfolder_collection_ts, selected_coll, date_interval=[], fig_width=24, 
                 _dtvar='report_dt'):
    """
    Create a Collection report from the given collection within the given date range.
    If empty, reads all the dates.
    """

    # loading time series table
    pdf_collts = FileCollector(pathfolder_collection_ts, date_interval=date_interval, 
                               filters={'collection_ID':[selected_coll]}, _dtvar=_dtvar)
    
    _plotvars_1 = ['coll_avg_price_dly_ma7', 'coll_token_avg_latest_val_usd', 
                   'coll_trx_usd_dly_ma7_p25', 'coll_trx_usd_dly_ma7_p75']    # later selection
    
    _visu_1 = pd.DataFrame()
    for _vars in _plotvars_1:
        _tmp_df = pdf_collts[[_dtvar, _vars]].rename(columns={_vars:'feature_value'})
        _tmp_df['feature_name'] = _vars
        _visu_1 = _visu_1.append(_tmp_df[[_dtvar, 'feature_name', 'feature_value']])
    _visu_1 = _visu_1.reset_index(drop=True)
    
    # creating plotting area
    fig, axes = plt.subplots(1, 1, figsize=(fig_width, 6))

    sns.lineplot(data=_visu_1, x=_dtvar, y='feature_value', hue='feature_name', 
                 palette=['blue', 'orange', 'blue', 'blue'], ax=axes)
    
    axes.lines[2].set_linestyle("--")
    axes.lines[3].set_linestyle("--")





##################################################################
#                     Analytical Functions                       #
##################################################################


