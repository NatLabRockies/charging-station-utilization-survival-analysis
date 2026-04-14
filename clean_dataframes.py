"""
@Author: Robin Steuteville
Objective: Script for cleaning and combining AFDC and EVW data. This sample script
is not meant to be run, since the data it cleans is confidential. Cleaned sample data
with confidential information removed is available in the data folder, and can be
used to run the survival_analysis_time_of_day_and_day_type.R script.
Date: April 13, 2026
Contact: Robin.Steuteville@nlr.gov
Corresponding Author: Ranjit Desai (Ranjit.Desai@nlr.gov)
Note: Anywhere the string "confidential" is used, this is replacing
confidential information.
"""

#%%
from afdc_cleaning_utilities import *
from evw_cleaning_utilities import *
from combined_df_utilities import *
from pathlib import Path
import os
import datetime as dt

#viewing all output rows in panda
pd.set_option("display.max_rows", None)
pd.set_option('display.max_columns', None)

#%%

# constants
DATE = dt.datetime.today().strftime("%m_%d_%Y")
DOWNLOAD_AFDC_DATA = False # if True, download AFDC data rather than accessing locally
TEST = False # set to true to create smaller test datasets
UNCLEANED_AFDC_DIR = "data_cleaning_and_analysis/uncleaned_afdc_data" #local uncleaned afdc data location
if not os.path.exists(Path(UNCLEANED_AFDC_DIR)) and not DOWNLOAD_AFDC_DATA:
    raise ValueError("UNCLEANED_AFDC_DIR {} does not exist. Either specify a different directory or set DOWNLOAD_AFDC_DATA to True.".format(UNCLEANED_AFDC_DIR))
elif not os.path.exists(Path(UNCLEANED_AFDC_DIR)):
    os.makedirs(UNCLEANED_AFDC_DIR)
UNCLEANED_AFDC_FILE = "afdc_data.parquet" #name of local uncleaned afdc data file
UNCLEANED_EVW_DIR = "confidential" # local uncleaned evw data location
UNCLEANED_EVW_SESSION_FILE = "session.csv"
UNCLEANED_EVW_STATION_FILE = "station.csv"
if not os.path.exists(Path(UNCLEANED_EVW_DIR)):
    raise ValueError("UNCLEANED_EVW_DIR {} does not exist.".format(UNCLEANED_EVW_DIR))
OUTPUT_DIR = "cleaned_dfs/" + DATE
ADD_TEST = "" #empty string if TEST is false
if TEST:
    ADD_TEST = "_test"
    OUTPUT_DIR = OUTPUT_DIR + ADD_TEST
if not os.path.exists(Path(OUTPUT_DIR)):
    os.makedirs(OUTPUT_DIR)
AFDC_CLEANED_FILE = "AFDC_cleaned_without_added_location_types_" + DATE + ADD_TEST + ".feather"
AFDC_WITH_LOCATIONS_L2_FILE = "AFDC_with_location_types_L2_" + DATE + ADD_TEST + ".feather"
AFDC_WITH_LOCATIONS_DC_FILE = "AFDC_with_location_types_DC_" + DATE + ADD_TEST + ".feather"
EVW_BASIC_CLEANED_FILE_L2 = "EVW_basic_cleaned_L2_" + DATE + ADD_TEST + ".feather"
EVW_BASIC_CLEANED_FILE_DC = "EVW_basic_cleaned_DC_" + DATE + ADD_TEST + ".feather"
EVW_SURVIVAL_CLEANED_FILE_L2 = "EVW_survival_cleaned_L2_" + DATE + ADD_TEST + ".feather"
EVW_SURVIVAL_CLEANED_FILE_DC = "EVW_survival_cleaned_DC_" + DATE + ADD_TEST + ".feather"
COMBINED_BASIC_L2_FILE = "combined_basic_L2_" + DATE + ADD_TEST + ".feather"
COMBINED_BASIC_DC_FILE = "combined_basic_DC_" + DATE + ADD_TEST + ".feather"
COMBINED_SURVIVAL_L2_FILE = "combined_survival_L2_" + DATE + ADD_TEST + ".feather"
COMBINED_SURVIVAL_DC_FILE = "combined_survival_DC_" + DATE + ADD_TEST + ".feather"
COMBINED_BASIC_L2_AFDC_FILE = "basic_AFDC_cleaned_L2_station_metrics_" + DATE + ADD_TEST + ".feather"
COMBINED_BASIC_DC_AFDC_FILE = "basic_AFDC_cleaned_DC_station_metrics_" + DATE + ADD_TEST + ".feather"
COMBINED_BASIC_L2_EVW_FILE = "basic_EVW_cleaned_L2_station_metrics_" + DATE + ADD_TEST + ".feather"
COMBINED_BASIC_DC_EVW_FILE = "basic_EVW_cleaned_DC_station_metrics_" + DATE + ADD_TEST + ".feather"
COMBINED_SURVIVAL_L2_AFDC_FILE = "basic_AFDC_cleaned_L2_station_metrics_" + DATE + ADD_TEST + ".feather"
COMBINED_SURVIVAL_DC_AFDC_FILE = "basic_AFDC_cleaned_DC_station_metrics_" + DATE + ADD_TEST + ".feather"
COMBINED_SURVIVAL_L2_EVW_FILE = "basic_EVW_cleaned_L2_station_metrics_" + DATE + ADD_TEST + ".feather"
COMBINED_SURVIVAL_DC_EVW_FILE = "basic_EVW_cleaned_DC_station_metrics_" + DATE + ADD_TEST + ".feather"


#%%
# clean AFDC data
if DOWNLOAD_AFDC_DATA:
    access_afdc_data(UNCLEANED_AFDC_DIR, UNCLEANED_AFDC_FILE)
if UNCLEANED_AFDC_FILE.endswith(".feather"):
    afdc_data = pd.read_feather(UNCLEANED_AFDC_DIR + "/" + UNCLEANED_AFDC_FILE)
elif UNCLEANED_AFDC_FILE.endswith(".parquet"):
    afdc_data = pd.read_parquet(UNCLEANED_AFDC_DIR + "/" + UNCLEANED_AFDC_FILE, engine="pyarrow")
else:
    raise ValueError("UNCLEANED_AFDC_FILE {} must either be a .feather or a .parquet file.", UNCLEANED_AFDC_FILE)
if TEST:
    afdc_data = afdc_data.head(100)
cleaned_afdc_df = clean_afdc_data(afdc_data, OUTPUT_DIR, AFDC_CLEANED_FILE)
clean_afdc_df_L2, clean_afdc_df_DC = add_afdc_location_types(cleaned_afdc_df, OUTPUT_DIR, AFDC_WITH_LOCATIONS_L2_FILE, AFDC_WITH_LOCATIONS_DC_FILE)
print("Cleaned afdc data!")

#%%
# clean EVW data
EVW_session_table = pd.read_csv(UNCLEANED_EVW_DIR + "/" + UNCLEANED_EVW_SESSION_FILE, low_memory=False, header=None)
EVW_station_table = pd.read_csv(UNCLEANED_EVW_DIR + "/" + UNCLEANED_EVW_STATION_FILE, low_memory=False, header=None)
if TEST:
    EVW_session_table = EVW_session_table.head(100)
    EVW_station_table = EVW_station_table.head(100)
EVW_basic_df_L2, EVW_basic_df_DC = clean_basic_evwatts(EVW_session_table, EVW_station_table, OUTPUT_DIR, EVW_BASIC_CLEANED_FILE_L2, EVW_BASIC_CLEANED_FILE_DC)
EVW_survival_df_L2, EVW_survival_df_DC = clean_survival_evwatts(EVW_session_table, EVW_station_table, OUTPUT_DIR, EVW_SURVIVAL_CLEANED_FILE_L2, EVW_SURVIVAL_CLEANED_FILE_DC)
print("Cleaned evw data!")

# combine data
combine_basic_df(clean_afdc_df_L2, clean_afdc_df_DC, EVW_basic_df_L2, EVW_basic_df_DC, OUTPUT_DIR, COMBINED_BASIC_L2_FILE, COMBINED_BASIC_DC_FILE)
print("Cleaned combined basic data!")
combine_survival_df(clean_afdc_df_L2, clean_afdc_df_DC, EVW_survival_df_L2, EVW_survival_df_DC, OUTPUT_DIR, COMBINED_SURVIVAL_L2_FILE, COMBINED_SURVIVAL_DC_FILE)
print("Cleaned combined survival data!")
print("Finished cleaning data!")
