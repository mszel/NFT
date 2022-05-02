##################################################################
#                      Pathes and Parameters                     #
##################################################################

##################################################################
# Runtime Parameters                                             #
##################################################################

# API parameters
PARAM_API_batch = 300             # load lines by API request (max possible is 300)

# ETL parameters
PARAM_ETL_njobs = 2               # of processors used at parallel computation
PARAM_ETL_raw_cleanup = False     # if True, delete all temp tables before start
PARAM_ETL_raw_append = False      # if True, will not calculate the already existing tables (checing the rownums - not an active function as of now)


##################################################################
# Data Sources: Raw Data Layer                                   #
##################################################################

PATH_00RAW_API_dump_main = './data/00_dump/' # the folder the API load the data


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


