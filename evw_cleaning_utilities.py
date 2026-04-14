"""
@Author: Robin Steuteville
Objective: Functions to use when accessing and cleaning EVW data
Date: October 31, 2025
Contact: Robin.Steuteville@nlr.gov
Corresponding Author: Ranjit Desai (Ranjit.Desai@nlr.gov)
Notes: 
1. Anywhere the string "confidential" is used, this is replacing 
confidential information.
2. Since EVW data contained data from public, private, and limited access
stations, it had to be cleaned separately for the yearly analysis
(which only included public charging stations), and the venue analysis
(which included public, private, and limited access stations).
3. In the data's original format, session and station information
were stored in separate tables. In order to create a dataset containing
all the information we needed for analysis, we had to combine these two tables.
"""

import pandas as pd

def convert_to_mins(hour):
    if pd.isna(hour):
        return hour
    else:
        return hour * 60.
    
def add_evw(id):
    new_id = 'EVW' + id
    return new_id

# cleaning EVW data for yearly analysis (only including public data)
def clean_basic_evwatts(EVW_session_table: pd.DataFrame, EVW_station_table: pd.DataFrame, output_dir, l2_output_file, dc_output_file, include_2023 = False) -> pd.DataFrame:
    #adding column names
    EVW_session_table.columns = ['id', 'station_id', 'port_id', 'start_time', 'start_time_zone', 'end_datetime', 'end_time_zone', 'connected_mins', 
                                'charging_mins', 'kwh', 'account_id', 'charging_level', 'fee', 'currency', 'ended_by', 'start_soc', 'end_soc', 'flag_id_session']
    EVW_station_table.columns = ['station_id', 'address', 'city', 'state', 'postal_code', 'local_time_zone', 'country', 
                                'lat', 'lon', 'no_ports', 'charging_level', 'station_location_id', 'rucc_id', 
                                'venue', 'access_type', 'coordinate_info_id', 'curbside', 'afdc_corridor_designation', 'afdc_ev_pricing_designation', 
                                'ev_pricing_category', 'flag_id_station']

    #getting rid of column names already at top of station table
    EVW_station_table = EVW_station_table[EVW_station_table['station_id']!='id']
    EVW_session_table = EVW_session_table[EVW_session_table['station_id']!='station_id']

    #changing station_id to integer so two tables can be combined
    EVW_station_table['station_id'] = EVW_station_table['station_id'].apply(pd.to_numeric)
    EVW_session_table['station_id'] = EVW_session_table['station_id'].apply(pd.to_numeric)

    #merging the two data tables
    EVW_data = EVW_session_table.merge(EVW_station_table, left_on='station_id', right_on='station_id', how='outer')

    # getting rid of any sessions where the charging time is greater that the connected time
    EVW_data = EVW_data[((EVW_data['connected_mins'] >= EVW_data['charging_mins']) | (EVW_data['charging_mins'].isnull()))]

    #throwing out data points that have no id
    EVW_data = EVW_data[EVW_data['id'].notnull()]

    #cutting out data points for which the two charging levels don't match
    EVW_data = EVW_data[EVW_data['charging_level_x']==EVW_data['charging_level_y']]

    #changing charging_level_x to charging_level and getting rid of charging_level_y
    EVW_data.columns = ['id', 'station_id', 'port_id', 'start_time', 'start_time_zone',
        'end_time', 'end_time_zone', 'connected_mins', 'charging_mins',
        'kwh', 'account_id', 'charging_level', 'fee', 'currency', 'ended_by',
        'start_soc', 'end_soc', 'flag_id_session', 'address', 'city', 'state',
        'postal_code', 'local_time_zone', 'country', 'lat', 'lon', 'no_ports',
        'charging_level_y', 'station_location_id', 'rucc_id', 'venue',
        'access_type', 'coordinate_info_id', 'curbside',
        'afdc_corridor_designation', 'afdc_ev_pricing_designation',
        'ev_pricing_category', 'flag_id_station']
    EVW_data = EVW_data.loc[:, ~EVW_data.columns.isin(['charging_level_y'])]

    # get rid of non-public datapoints
    EVW_data = EVW_data[(EVW_data['access_type'].isnull()) | (EVW_data['access_type']=='Public')]

    #changing DCFC to DC to match other dataset
    EVW_data = EVW_data.replace({'charging_level': {'DCFC': 'DC'}})

    # getting rid of null data points for required fields
    EVW_data = EVW_data[EVW_data['state'].notnull()]
    EVW_data = EVW_data[EVW_data['connected_mins'].notnull()]
    EVW_data = EVW_data[EVW_data['start_time'].notnull()]
    EVW_data = EVW_data[EVW_data['kwh'].notnull()]
    EVW_data = EVW_data[EVW_data['charging_level'].notnull()]
    EVW_data=EVW_data[EVW_data['station_id'].notnull()]

    #add date column
    EVW_data['start_date'] = pd.to_datetime(EVW_data['start_time']).dt.date.astype("string")
    EVW_data['end_date'] = pd.to_datetime(EVW_data['end_time']).dt.date.astype("string")

    #creating year column
    EVW_data['year'] = pd.to_datetime(EVW_data['start_time']).dt.year

    #exclude 2023 due to small sample size
    if not include_2023:
        EVW_data=EVW_data[EVW_data['year']!=2023]

    #cutting down based on minutes connected and kwh like for afdc data
    EVW_data['connected_mins'] = EVW_data['connected_mins'].apply(pd.to_numeric)
    EVW_data['charging_mins'] = EVW_data['charging_mins'].apply(pd.to_numeric)
    EVW_data = EVW_data[EVW_data['connected_mins']>=0.08333]
    EVW_data = EVW_data[EVW_data['connected_mins']<=48.0]
    EVW_data['kwh'] = EVW_data['kwh'].apply(pd.to_numeric)
    EVW_data = EVW_data[EVW_data['kwh']>=0.5]
    EVW_data = EVW_data[EVW_data['kwh']<= 212.7]
    EVW_data = EVW_data.drop(EVW_data[(EVW_data.charging_level=='DC') & (EVW_data.connected_mins>10.)].index)

    #fixing inconsistent city names
    EVW_data = EVW_data.replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}})

    EVW_data = EVW_data.replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})

    EVW_data = EVW_data.replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})

    EVW_data = EVW_data.replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})

    EVW_data = EVW_data[EVW_data['city']!="confidential"]

    #changing to title case
    EVW_data['city']=EVW_data['city'].str.title()

    #fixing city names that don't use title case
    EVW_data = EVW_data.replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})

    #list of columns
    columns = EVW_data.columns

    #creating uniform column types
    EVW_data.loc[:, 'station_location_id'] = EVW_data['station_location_id'].astype('float64')
    EVW_data.loc[:, 'rucc_id'] = EVW_data['rucc_id'].astype('float64')
    EVW_data.loc[:, 'coordinate_info_id'] = EVW_data['coordinate_info_id'].astype('float64')
    EVW_data.loc[:, 'account_id'] = EVW_data['account_id'].astype('float64')
    EVW_data.loc[:, 'flag_id_session'] = EVW_data['flag_id_session'].astype('float64')

    EVW_data = EVW_data.astype(dtype= {columns[0]:"Int64", columns[1]:"Int64", columns[2]:"Int64", columns[3]:"datetime64[ns]", columns[4]:"string", columns[5]:"datetime64[ns]", 
                                            columns[6]:"string", columns[7]:"float64", columns[8]:"float64", columns[9]:"float64", columns[10]:"Int64", columns[11]:"string", columns[12]: "float64",
                                            columns[13]:"string", columns[14]: "string", columns[15]: "float64", columns[16]: "float64", columns[17]: "Int64",
                                            columns[18]:"string", columns[19]:"string", columns[20]:"string", columns[21]:"string", columns[22]:"string", columns[23]:"string",
                                            columns[24]:"float64", columns[25]:"float64", columns[26]:"Int64",
                                            columns[27]:"Int64", columns[28]:"Int64", columns[29]:"string", columns[30]:"string", columns[31]:"Int64", 
                                            columns[32]:"object", columns[33]:"object", columns[34]:"string", columns[35]:"string", columns[36]:"Int64",
                                            columns[37]:"string", columns[38]:"string", columns[39]:"Int32"})

    #converting connected_mins and charging_mins to mins from hours
    EVW_data.loc[:,'connected_mins']=EVW_data['connected_mins'].apply(convert_to_mins)
    EVW_data.loc[:,'charging_mins']=EVW_data['charging_mins'].apply(convert_to_mins)

    #getting rid of any data points where charging minutes are nonzero/non-null and less than 5
    EVW_data = EVW_data[~((EVW_data['charging_mins']!=0.) & (EVW_data['charging_mins'].notnull()) & (EVW_data['charging_mins']<5.))]

    #adding EVW to end of id values
    #first, must make id values into strings
    EVW_data = EVW_data.astype(dtype= {columns[0]:"string", columns[1]:"string", columns[2]:"Int64", columns[3]:"datetime64[ns]", columns[4]:"string", columns[5]:"datetime64[ns]", 
                                            columns[6]:"string", columns[7]:"float64", columns[8]:"float64", columns[9]:"float64", columns[10]:"Int64", columns[11]:"string", columns[12]: "float64",
                                            columns[13]:"string", columns[14]: "string", columns[15]: "float64", columns[16]: "float64", columns[17]: "Int64",
                                            columns[18]:"string", columns[19]:"string", columns[20]:"string", columns[21]:"string", columns[22]:"string", columns[23]:"string",
                                            columns[24]:"float64", columns[25]:"float64", columns[26]:"Int64",
                                            columns[27]:"Int64", columns[28]:"Int64", columns[29]:"string", columns[30]:"string", columns[31]:"Int64", 
                                            columns[32]:"object", columns[33]:"object", columns[34]:"string", columns[35]:"string", columns[36]:"Int64"})

    EVW_data.loc[:,'id']=EVW_data['id'].apply(add_evw)
    EVW_data.loc[:,'station_id']=EVW_data['station_id'].apply(add_evw)

    # get rid of rows with legitimate flags
    EVW_data = EVW_data[(EVW_data['flag_id_session']!=2) & (EVW_data['flag_id_session']!=1) & (EVW_data['flag_id_session']!=3) & (EVW_data['flag_id_session']!=66) & (EVW_data['flag_id_session']!=33) & (EVW_data['flag_id_session']!=4) & (EVW_data['flag_id_session']!=36) & (EVW_data['flag_id_session']!=6) & (EVW_data['flag_id_session']!=5)]

    #storing L2 datapoints in feather format
    EVW_data_L2 = EVW_data[EVW_data['charging_level']=='L2']
    EVW_data_L2.to_feather(output_dir + "/" + l2_output_file)

    #storing DC datapoints in feather format
    EVW_data_DC = EVW_data[EVW_data['charging_level']=='DC']
    EVW_data_DC.to_feather(output_dir + "/" + dc_output_file)
    
    return EVW_data_L2, EVW_data_DC

# cleaning EVW data for venue type analysis and time of day analysis (including public, limited, and private data)
def clean_survival_evwatts(EVW_session_table: pd.DataFrame, EVW_station_table: pd.DataFrame, output_dir, l2_output_file, dc_output_file, include_2023 = False) -> pd.DataFrame:
    #adding column names
    EVW_session_table.columns = ['id', 'station_id', 'port_id', 'start_time', 'start_time_zone', 'end_datetime', 'end_time_zone', 'connected_mins', 
                                'charging_mins', 'kwh', 'account_id', 'charging_level', 'fee', 'currency', 'ended_by', 'start_soc', 'end_soc', 'flag_id_session']
    EVW_station_table.columns = ['station_id', 'address', 'city', 'state', 'postal_code', 'local_time_zone', 'country', 
                                'lat', 'lon', 'no_ports', 'charging_level', 'station_location_id', 'rucc_id', 
                                'venue', 'access_type', 'coordinate_info_id', 'curbside', 'afdc_corridor_designation', 'afdc_ev_pricing_designation', 
                                'ev_pricing_category', 'flag_id_station']

    #getting rid of column names already at top of station table
    EVW_station_table = EVW_station_table[EVW_station_table['station_id']!='id']
    EVW_session_table = EVW_session_table[EVW_session_table['station_id']!='station_id']
    #changing station_id of station table to integer so two tables can be combined
    EVW_station_table['station_id'] = EVW_station_table['station_id'].astype(float)
    EVW_session_table['station_id'] = EVW_session_table['station_id'].apply(pd.to_numeric)

    #merging the two data tables
    EVW_data = EVW_session_table.merge(EVW_station_table, left_on='station_id', right_on='station_id', how='outer')

    #throwing out data points that have no id
    EVW_data = EVW_data[EVW_data['id'].notnull()]

    #cutting out data points for which the two charging levels don't match
    EVW_data = EVW_data[EVW_data['charging_level_x']==EVW_data['charging_level_y']]

    #changing charging_level_x to charging_level and getting rid of charging_level_y
    EVW_data.columns = ['id', 'station_id', 'port_id', 'start_time', 'start_time_zone',
        'end_time', 'end_time_zone', 'connected_mins', 'charging_mins',
        'kwh', 'account_id', 'charging_level', 'fee', 'currency', 'ended_by',
        'start_soc', 'end_soc', 'flag_id_session', 'address', 'city', 'state',
        'postal_code', 'local_time_zone', 'country', 'lat', 'lon', 'no_ports',
        'charging_level_y', 'station_location_id', 'rucc_id', 'venue',
        'access_type', 'coordinate_info_id', 'curbside',
        'afdc_corridor_designation', 'afdc_ev_pricing_designation',
        'ev_pricing_category', 'flag_id_station']
    EVW_data = EVW_data.loc[:, ~EVW_data.columns.isin(['charging_level_y'])]

    #changing DCFC to DC to match other dataset
    EVW_data = EVW_data.replace({'charging_level': {'DCFC': 'DC'}})

    EVW_data = EVW_data[EVW_data['state'].notnull()]
    EVW_data = EVW_data[EVW_data['connected_mins'].notnull()]
    EVW_data = EVW_data[EVW_data['start_time'].notnull()]
    EVW_data = EVW_data[EVW_data['kwh'].notnull()]
    EVW_data = EVW_data[EVW_data['charging_level'].notnull()]
    EVW_data = EVW_data[EVW_data['station_id'].notnull()]

    #add date column
    EVW_data['start_date'] = pd.to_datetime(EVW_data['start_time']).dt.date.astype("string")
    EVW_data['end_date'] = pd.to_datetime(EVW_data['end_time']).dt.date.astype("string")

    #creating year column
    EVW_data['year'] = pd.to_datetime(EVW_data['start_time']).dt.year

    #exclude 2023 due to small sample size
    if not include_2023:
        EVW_data=EVW_data[EVW_data['year']!=2023]

    # delete points with out of scope or inconsistent timezones
    EVW_data = EVW_data[EVW_data['start_time_zone']!="confidential"]
    EVW_data = EVW_data[EVW_data['start_time_zone']!="confidential"]

    EVW_data = EVW_data.loc[((((EVW_data['start_time_zone'] == "confidential") & (EVW_data['end_time_zone'] == "confidential")) | ((EVW_data['start_time_zone'] == "confidential")\
                                                    & (EVW_data['end_time_zone'] == "confidential")) | ((EVW_data['start_time_zone'] == "confidential") & (EVW_data['end_time_zone'] == "confidential"))\
                                                    | ((EVW_data['start_time_zone'] == "confidential") & (EVW_data['end_time_zone'] == "confidential"))\
                                                    | ((EVW_data['start_time_zone'] == "confidential") & (EVW_data['end_time_zone'] == "confidential")))\
                                                    & ((EVW_data['start_date'] == '2016-03-12') | (EVW_data['start_date'] == '2016-03-13') | (EVW_data['start_date'] == '2017-03-11')\
                                                    | (EVW_data['start_date'] == '2017-03-12') | (EVW_data['start_date'] == '2018-03-10') | (EVW_data['start_date'] == '2018-03-11')\
                                                    | (EVW_data['start_date'] == '2019-03-09') | (EVW_data['start_date'] == '2019-03-10') | (EVW_data['start_date'] == '2020-03-07')\
                                                    | (EVW_data['start_date'] == '2020-03-08') | (EVW_data['start_date'] == '2021-03-13') | (EVW_data['start_date'] == '2021-03-14')\
                                                    | (EVW_data['start_date'] == '2022-03-12') | (EVW_data['start_date'] == '2022-03-13') | (EVW_data['end_date'] == '2016-03-13')\
                                                    | (EVW_data['end_date'] == '2017-03-12') | (EVW_data['end_date'] == '2018-03-11') | (EVW_data['end_date'] == '2019-03-10')\
                                                    | (EVW_data['end_date'] == '2020-03-08') | (EVW_data['end_date'] == '2021-03-14') | (EVW_data['end_date'] == '2022-03-13')))\
                                                    | (EVW_data['end_date'].isnull()) | (EVW_data['start_time_zone']==EVW_data['end_time_zone'])]
    EVW_data = EVW_data.loc[((((EVW_data['start_time_zone'] == "confidential") & (EVW_data['end_time_zone'] == "confidential"))\
                            | ((EVW_data['start_time_zone'] == "confidential") & (EVW_data['end_time_zone'] == "confidential"))\
                            | ((EVW_data['start_time_zone'] == "confidential") & (EVW_data['end_time_zone'] == "confidential"))\
                            | ((EVW_data['start_time_zone'] == "confidential") & (EVW_data['end_time_zone'] == "confidential"))\
                            | ((EVW_data['start_time_zone'] == "confidential") & (EVW_data['end_time_zone'] == "confidential")))\
                            & ((EVW_data['start_date'] == '2016-11-05') | (EVW_data['start_date'] == '2016-11-06')\
                            | (EVW_data['start_date'] == '2017-11-04') | (EVW_data['start_date'] == '2017-11-05')\
                            | (EVW_data['start_date'] == '2018-11-03') | (EVW_data['start_date'] == '2018-11-04')\
                            | (EVW_data['start_date'] == '2019-11-02') | (EVW_data['start_date'] == '2019-11-03')\
                            | (EVW_data['start_date'] == '2020-10-31') | (EVW_data['start_date'] == '2020-11-01')\
                            | (EVW_data['start_date'] == '2021-11-06') | (EVW_data['start_date'] == '2021-11-07')\
                            | (EVW_data['start_date'] == '2022-11-05') | (EVW_data['start_date'] == '2022-11-06')\
                            | (EVW_data['end_date'] == '2016-11-06') | (EVW_data['end_date'] == '2017-11-05') | (EVW_data['end_date'] == '2018-11-04')\
                            | (EVW_data['end_date'] == '2019-11-03') | (EVW_data['end_date'] == '2020-11-01') | (EVW_data['end_date'] == '2021-11-07')\
                            | (EVW_data['end_date'] == '2022-11-06'))) & (EVW_data['end_date'].isnull()) | (EVW_data['start_time_zone']==EVW_data['end_time_zone'])]

    #cutting down based on minutes connected and kwh like for afdc data
    EVW_data['connected_mins'] = EVW_data['connected_mins'].apply(pd.to_numeric)
    EVW_data['charging_mins'] = EVW_data['charging_mins'].apply(pd.to_numeric)
    EVW_data = EVW_data[EVW_data['connected_mins']>=0.08333]
    EVW_data = EVW_data[EVW_data['connected_mins']<=48.0]
    EVW_data = EVW_data[EVW_data['charging_level'].notnull()]
    EVW_data['kwh'] = EVW_data['kwh'].apply(pd.to_numeric)
    EVW_data = EVW_data[EVW_data['kwh']>=0.5]
    EVW_data = EVW_data[EVW_data['kwh']<= 212.7]
    EVW_data = EVW_data.drop(EVW_data[(EVW_data.charging_level=='DC') & (EVW_data.connected_mins>10.)].index)

    # getting rid of any sessions where the charging time is greater that the connected time
    EVW_data = EVW_data[((EVW_data['connected_mins'] >= EVW_data['charging_mins']) | (EVW_data['charging_mins'].isnull()))]

    #fixing inconsistent city names
    EVW_data = EVW_data.replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}})

    EVW_data = EVW_data.replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})

    EVW_data = EVW_data.replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})

    EVW_data = EVW_data.replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})

    EVW_data = EVW_data[EVW_data['city']!="confidential"]

    #changing city names to title case for easier matching
    EVW_data['city']=EVW_data['city'].str.title()

    #fixing city names that don't use title case
    EVW_data = EVW_data.replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})

    #list of columns
    columns = EVW_data.columns
    #creating uniform column types
    EVW_data.loc[:, 'station_location_id'] = EVW_data['station_location_id'].astype('float64')
    EVW_data.loc[:, 'rucc_id'] = EVW_data['rucc_id'].astype('float64')
    EVW_data.loc[:, 'coordinate_info_id'] = EVW_data['coordinate_info_id'].astype('float64')
    EVW_data.loc[:, 'account_id'] = EVW_data['account_id'].astype('float64')
    EVW_data.loc[:, 'flag_id_session'] = EVW_data['flag_id_session'].astype('float64')

    #converting other columns
    EVW_data = EVW_data.astype(dtype= {columns[0]:"Int64", columns[1]:"Int64", columns[2]:"Int64", columns[3]:"datetime64[ns]", columns[4]:"string", columns[5]:"datetime64[ns]", 
                                            columns[6]:"string", columns[7]:"float64", columns[8]:"float64", columns[9]:"float64", columns[10]:"Int64", columns[11]:"string", columns[12]: "float64",
                                            columns[13]:"string", columns[14]: "string", columns[15]: "float64", columns[16]: "float64", columns[17]: "Int64",
                                            columns[18]:"string", columns[19]:"string", columns[20]:"string", columns[21]:"string", columns[22]:"string", columns[23]:"string",
                                            columns[24]:"float64", columns[25]:"float64", columns[26]:"Int64",
                                            columns[27]:"Int64", columns[28]:"Int64", columns[29]:"string", columns[30]:"string", columns[31]:"Int64", 
                                            columns[32]:"object", columns[33]:"object", columns[34]:"string", columns[35]:"string", columns[36]:"Int64",
                                            columns[37]:"string", columns[38]:"string", columns[39]:"Int32"})

    #converting connected_mins and charging_mins to mins from hours
    EVW_data.loc[:,'connected_mins']=EVW_data['connected_mins'].apply(convert_to_mins)
    EVW_data.loc[:,'charging_mins']=EVW_data['charging_mins'].apply(convert_to_mins)

    #getting rid of any data points where charging minutes are nonzero/non-null and less than 5
    EVW_data = EVW_data[~((EVW_data['charging_mins']!=0.) & (EVW_data['charging_mins'].notnull()) & (EVW_data['charging_mins']<5.))]

    #adding EVW to end of id values
    #first, must make id values into strings
    EVW_data = EVW_data.astype(dtype= {columns[0]:"string", columns[1]:"string", columns[2]:"Int64", columns[3]:"datetime64[ns]", columns[4]:"string", columns[5]:"datetime64[ns]", 
                                            columns[6]:"string", columns[7]:"float64", columns[8]:"float64", columns[9]:"float64", columns[10]:"Int64", columns[11]:"string", columns[12]: "float64",
                                            columns[13]:"string", columns[14]: "string", columns[15]: "float64", columns[16]: "float64", columns[17]: "Int64",
                                            columns[18]:"string", columns[19]:"string", columns[20]:"string", columns[21]:"string", columns[22]:"string", columns[23]:"string",
                                            columns[24]:"float64", columns[25]:"float64", columns[26]:"Int64",
                                            columns[27]:"Int64", columns[28]:"Int64", columns[29]:"string", columns[30]:"string", columns[31]:"Int64", 
                                            columns[32]:"object", columns[33]:"object", columns[34]:"string", columns[35]:"string", columns[36]:"Int64"})

    EVW_data.loc[:,'id']=EVW_data['id'].apply(add_evw)
    EVW_data.loc[:,'station_id']=EVW_data['station_id'].apply(add_evw)

    # get rid of rows with data error flags
    EVW_data = EVW_data[(EVW_data['flag_id_session']!=2) & (EVW_data['flag_id_session']!=1) & (EVW_data['flag_id_session']!=3) & (EVW_data['flag_id_session']!=66) & (EVW_data['flag_id_session']!=33) & (EVW_data['flag_id_session']!=4) & (EVW_data['flag_id_session']!=36) & (EVW_data['flag_id_session']!=6) & (EVW_data['flag_id_session']!=5)]

    #storing L2 datapoints in feather format
    EVW_data_L2 = EVW_data[EVW_data['charging_level']=='L2']
    EVW_data_L2.to_feather(output_dir + "/" + l2_output_file)

    #storing DC datapoints in feather format
    EVW_data_DC = EVW_data[EVW_data['charging_level']=='DC']
    EVW_data_DC.to_feather(output_dir + "/" + dc_output_file)

    return EVW_data_L2, EVW_data_DC