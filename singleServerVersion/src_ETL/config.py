##################################################################
#                      Pathes and Parameters                     #
##################################################################

##################################################################
# Runtime Parameters                                             #
##################################################################

PARAM_batchsize = 500000      # size of read rows at once
PARAM_njobs = 2               # of processors used at parallel computation
PARAM_categorylist = ['Art']  # kept categories (empty=all)
PARAM_raw_cleanup = False     # if True, delete all temp tables before start
PARAM_raw_append = False      # if True, will not calculate the already existing tables (checing the rownums - not an active function as of now)


##################################################################
# Data Sources: Raw Data Layer                                   #
##################################################################

PATH_00RAW_API_input_folder = './data/00_raw/'
PATH_00RAW_API_input_filename = ''


##################################################################
# Stage Layer Pathes                                             #
##################################################################

PATH_01STAGE_tmp_main = './data/01_stage/'


##################################################################
# NDS Layer Pathes                                               #
##################################################################

PATH_02NDS_main = './data/02_NDS/'


##################################################################
# DM Layer Pathes                                                #
##################################################################

PATH_03DM_main = './data/03_DM/'
PATH_03DM_01tempL = PATH_03DM_main + 'templayer/'


