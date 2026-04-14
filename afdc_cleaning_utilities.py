"""
@Author: Robin Steuteville
Objective: Functions to use when accessing and cleaning AFDC data
Date: April 13, 2026
Contact: Robin.Steuteville@nlr.gov
Corresponding Author: Ranjit Desai (Ranjit.Desai@nlr.gov)
Notes: 
1. Anywhere the string "confidential" is used, this is replacing
confidential information.
2. Historical AFDC station locator data from Jan. 1 2017
2018, 2019, 2020, as well as AFDC station locator data from 
Jan. 4 2024 was used to add venue types to some charging sessions that
did not have a venue type. This data is publicly available at
https://afdc.energy.gov/stations#/analyze.
"""

from typing import Tuple
import psycopg2
import pandas as pd

AFDC_STATION_LOCATION_DIR = "data_cleaning_and_analysis"

# Accesses afdc data and saves locally -- only needs to be done once
def access_afdc_data(output_dir, output_file):
    connection = psycopg2.connect(
    host="confidential",
    port="confidential",
    user="confidential",
    password="confidential",
    database="confidential",
    )
    QUERY = """
            Confidential
            """
    df = pd.read_sql(QUERY, con=connection)

    df.head()

    df.to_feather(output_dir + "/" + output_file)

    print("data storage is complete!")

# add AFDC in front of a string
def add_afdc(id):
    new_id = 'AFDC' + id
    return new_id

# adds zero in front of any zip code of length 4
def add_zero(zip):
    if not pd.isna(zip):
        if len(zip) == 4:
            zip = '0' + zip
    return zip

# clean afdc data
def clean_afdc_data(afdc_data: pd.DataFrame, output_dir, output_file) -> pd.DataFrame:
    # excluding or updating data with typos or inconsistencies
    afdc_data = afdc_data[(afdc_data['state'] != "confidential")]

    afdc_data = afdc_data.replace("confidential", "confidential")

    afdc_data = afdc_data[afdc_data['country']!="confidential"]

    afdc_data = afdc_data[afdc_data['address']!='Test']

    afdc_data = afdc_data[afdc_data['city']!='Test City']

    afdc_data = afdc_data.replace({'postal_code': {"confidential": "confidential"}})

    afdc_data = afdc_data.replace({'postal_code': {"confidential": "confidential"}})

    afdc_data = afdc_data.replace({'postal_code': {"confidential": "confidential"}})

    afdc_data = afdc_data.replace({'city': {"confidential": "confidential"}})

    afdc_data = afdc_data.replace({'city': {"confidential": "confidential"}})

    afdc_data = afdc_data.replace({'city': {"confidential": "confidential"}})

    #changing all caps cities to capital first letter, lowercase other letters
    afdc_data['city'] = afdc_data['city'].str.title()

    # excluding or updating data with typos or inconsistencies
    afdc_data = afdc_data[afdc_data['postal_code']!="confidential"]

    afdc_data = afdc_data.replace({'postal_code': {"confidential": "confidential"}})

    afdc_data = afdc_data.replace({'postal_code': {"confidential": "confidential"}})

    afdc_data = afdc_data[afdc_data['postal_code']!="confidential"]

    afdc_data.loc[afdc_data['address']=="confidential", 'city'] = "confidential"

    afdc_data.loc[afdc_data['address']=="confidential", 'postal_code'] = "confidential"

    afdc_data.loc[afdc_data['address']=="confidential", 'city'] = "confidential"

    afdc_data.loc[afdc_data['address']=="confidential", 'postal_code'] = "confidential"

    afdc_data = afdc_data.replace({'city': {"confidential": "confidential"}})\
        .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
            .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
                .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
                    .replace({'city': {"confidential": "confidential"}}).replace({'city': {"confidential": "confidential"}})\
                        .replace({'city': {"confidential": "confidential"}})\
                            .replace({'city': {"confidential": "confidential"}})\
                                .replace({'city': {"confidential": "confidential"}})\
                                    .replace({'city': {"confidential": "confidential"}})\
                                        .replace({'city': {"confidential": "confidential"}})\
                                            .replace({'city': {"confidential": "confidential"}})\
                                                .replace({'city': {"confidential": "confidential"}})\
                                                    .replace({'city': {"confidential": "confidential"}})\
                                                        .replace({'city': {"confidential": "confidential"}})

    #get rid of connection instances less than 5 mins
    afdc_data = afdc_data[afdc_data['connected_mins']>=5.]

    #getting rid of datapoints with too large connection times
    afdc_data = afdc_data[afdc_data['connected_mins']<=2880.0]

    #limiting to kWh>= 0.5
    afdc_data = afdc_data[afdc_data['kwh']>=0.5]

    #getting rid of datapoints with kwh greater than 212.7
    afdc_data = afdc_data[afdc_data['kwh']<= 212.7]

    # ensure required fields are not null
    afdc_data = afdc_data[afdc_data['id'].notnull()]
    afdc_data = afdc_data[afdc_data['state'].notnull()]
    afdc_data = afdc_data[afdc_data['connected_mins'].notnull()]
    afdc_data = afdc_data[afdc_data['start_time'].notnull()]
    afdc_data = afdc_data[afdc_data['kwh'].notnull()]
    afdc_data = afdc_data[afdc_data['charging_level'].notnull()]
    afdc_data=afdc_data[afdc_data['afdc_station_id'].notnull()]

    #drop dc charging sessions that are longer than 600 minutes
    afdc_data = afdc_data.drop(afdc_data[(afdc_data.charging_level=='DC') & (afdc_data.connected_mins>600.)].index)

    afdc_data = afdc_data.loc[:, ~afdc_data.columns.isin(['start_time'])]

    afdc_data=afdc_data.rename(columns={"local_start_time": "start_time"})

    #creating year column
    afdc_data['year'] = pd.to_datetime(afdc_data['start_time']).dt.year

    #add date column
    afdc_data['start_date'] = pd.to_datetime(afdc_data['start_time']).dt.date.astype("string")

    # getting rid of datapoints which have a charging time that is longer than the connected time
    afdc_data = afdc_data[((afdc_data['connected_mins'] >= afdc_data['charging_mins']) | (afdc_data['charging_mins'].isnull()))]

    columns = afdc_data.columns
    afdc_data = afdc_data.astype(dtype= {columns[0]:"string", columns[1]:"string", columns[2]:"string", columns[3]:"string", columns[4]:"string", columns[5]:"string", 
                                            columns[6]:"string", columns[7]:"string", columns[8]:"string", columns[9]:"float64", columns[10]:"float64", 
                                            columns[11]:"string", columns[12]:"string",
                                            columns[13]:"float64", columns[14]:"string", columns[15]:"string", columns[16]:"Int64", columns[17]:"datetime64[ns, UTC]", 
                                            columns[18]:"datetime64[ns, UTC]", columns[19]:"string", columns[20]:"datetime64[ns, UTC]",
                                            columns[21]:"string", columns[22]:"string", 
                                            columns[23]:"string", columns[24]:"string", columns[25]:"string", columns[26]:"string", columns[27]:"datetime64[ns]",
                                            columns[28]: "float64", columns[29]: "float64", columns[30]:"Int64", columns[31]:"string"})

    #adding AFDC to front of ids
    afdc_data.loc[:,'id']=afdc_data['id'].apply(add_afdc)
    afdc_data.loc[:,'afdc_station_id']=afdc_data['afdc_station_id'].apply(add_afdc)

    #standardizing state names across AFDC and EVW (replacing abbreviation with full name)
    afdc_data=afdc_data.replace({'state': {"confidential": "confidential"}}).replace({'state': {"confidential": "confidential"}})\
        .replace({'state': {"confidential": "confidential"}}).replace({'state': {"confidential": "confidential"}})\
        .replace({'state': {"confidential": "confidential"}}).replace({'state': {"confidential": "confidential"}})\
        .replace({'state': {"confidential": "confidential"}}).replace({'state': {"confidential": "confidential"}})\
        .replace({'state': {"confidential": "confidential"}}).replace({'state': {"confidential": "confidential"}})\
        .replace({'state': {"confidential": "confidential"}}).replace({'state': {"confidential": "confidential"}})\
        .replace({'state': {"confidential": "confidential"}}).replace({'state': {"confidential": "confidential"}})\
        .replace({'state': {"confidential": "confidential"}}).replace({'state': {"confidential": "confidential"}})\
        .replace({'state': {"confidential": "confidential"}}).replace({'state': {"confidential": "confidential"}})\
        .replace({'state': {"confidential": "confidential"}}).replace({'state': {"confidential": "confidential"}})\
        .replace({'state': {"confidential": "confidential"}}).replace({'state': {"confidential": "confidential"}})\
        .replace({'state': {"confidential": "confidential"}}).replace({'state': {"confidential", "confidential"}})\
        .replace({'state': {"confidential": "confidential"}}).replace({'state': {"confidential": "confidential"}})\
        .replace({'state': {"confidential": "confidential"}}).replace({'state': {"confidential": "confidential"}})\
        .replace({'state': {"confidential": "confidential"}}).replace({'state': {"confidential": "confidential"}})\
        .replace({'state': {"confidential": "confidential"}}).replace({'state': {"confidential": "confidential"}})\
        .replace({'state': {"confidential": "confidential"}}).replace({'state': {"confidential": "confidential"}})\
        .replace({'state': {"confidential": "confidential"}}).replace({'state': {"confidential": "confidential"}})\
        .replace({'state': {"confidential": "confidential"}}).replace({'state': {"confidential": "confidential"}})\
        .replace({'state': {"confidential": "confidential"}}).replace({'state': {"confidential": "confidential"}})\
        .replace({'state': {"confidential": "confidential"}}).replace({'state': {"confidential": "confidential"}})
    
    afdc_data=afdc_data.rename(columns={"afdc_station_id": "station_id"})

    #getting rid of any data points where charging minutes are nonzero/non-null and less than 5
    afdc_data = afdc_data[~((afdc_data['charging_mins']!=0.) & (afdc_data['charging_mins'].notnull()) & (afdc_data['charging_mins']<5.))]

    #storing all datapoints in feather format
    afdc_data.to_feather(output_dir + "/" + output_file)

    return afdc_data

# combining afdc data with afdc station locator information that includes more location types
def add_afdc_location_types(afdc_df: pd.DataFrame, output_dir, output_file_l2, output_file_dc) -> Tuple[pd.DataFrame, pd.DataFrame]:
    # accessing afdc station info
    afdc_stations = pd.read_feather(AFDC_STATION_LOCATION_DIR + '/' + 'alt_fuel_stations_cleaned.feather')
    afdc_stations_paid = pd.read_feather(AFDC_STATION_LOCATION_DIR + '/' + 'alt_fuel_stations_paid_work_and_not_work_cleaned.feather')
    afdc_stations_lat_lon = pd.read_feather(AFDC_STATION_LOCATION_DIR + '/' + 'alt_fuel_stations_cleaned_lat_lon.feather')
    afdc_stations_hist = pd.read_feather(AFDC_STATION_LOCATION_DIR + '/' + 'hist_alt_fuel_stations_cleaned.feather')
    afdc_stations_hist_2018 = pd.read_feather(AFDC_STATION_LOCATION_DIR + '/' + '2018_hist_alt_fuel_stations_cleaned.feather')

    # changing necessary column names
    afdc_df = afdc_df.rename(columns={"latitude": "lat"})
    afdc_df = afdc_df.rename(columns={"longitude": "lon"})

    # rounding afdc_df lat and lon to 5 decimal places
    afdc_df['lat'] = afdc_df['lat'].round(5)
    afdc_df['lon'] = afdc_df['lon'].round(5)

    # creating combined lat_lon column in afdc_df
    afdc_df['lat_lon'] = afdc_df['lat'].astype('string') + afdc_df['lon'].astype('string')

    # adding zeroes to the front of any postal code that has length four
    afdc_df['postal_code'] = afdc_df['postal_code'].apply(add_zero)

    # creating address_zip column in afdc_df
    afdc_df['address_zip'] = afdc_df['address'] + afdc_df['postal_code']

    # merging afdc data with afdc station info
    afdc_df = afdc_df.merge(afdc_stations, left_on='address_zip', right_on='address_zip', how='outer')
    afdc_df = afdc_df.merge(afdc_stations_paid, left_on='address_zip', right_on='address_zip', how='outer')
    afdc_df = afdc_df.merge(afdc_stations_lat_lon, left_on='lat_lon', right_on='lat_lon', how='outer')

    # creating single venues, station_address, station_zip columns and getting rid of address_zip and lat_lon columns
    afdc_df['venue'] = afdc_df['venue_x'].fillna(afdc_df['venue_y'])
    afdc_df['station_address'] = afdc_df['station_address_x'].fillna(afdc_df['station_address_y'])
    afdc_df['station_zip'] = afdc_df['station_zip_x'].fillna(afdc_df['station_zip_y'])
    afdc_df = afdc_df.drop(['venue_x', 'venue_y', 'station_address_x', 'station_address_y', 'station_zip_x', 'station_zip_y', 'lat_lon'], axis=1)

    #merging with historical (Jan 1 2019) AFDC station locator data to add venue information where possible and lacking
    afdc_df = afdc_df.merge(afdc_stations_hist, left_on='address_zip', right_on='address_zip', how='outer')

    # getting rid of charging stations that do not have any corresponding sessions
    afdc_df = afdc_df[afdc_df['connected_mins'].notnull()]

    # creating single venues, station_address, station_zip columns and getting rid of address_zip and lat_lon columns
    afdc_df['venue'] = afdc_df['venue_x'].fillna(afdc_df['venue_y'])
    afdc_df['station_address'] = afdc_df['station_address_x'].fillna(afdc_df['station_address_y'])
    afdc_df['station_zip'] = afdc_df['station_zip_x'].fillna(afdc_df['station_zip_y'])
    afdc_df = afdc_df.drop(['venue_x', 'venue_y', 'station_address_x', 'station_address_y', 'station_zip_x', 'station_zip_y'], axis=1)

    # merging with Jan 1 2018 AFDC station locator data to add venue information where possible and lacking
    afdc_df = afdc_df.merge(afdc_stations_hist_2018, left_on='address_zip', right_on='address_zip', how='outer')

    # getting rid of charging stations that do not have any corresponding sessions
    afdc_df = afdc_df[afdc_df['connected_mins'].notnull()]

    # creating single venues, station_address, station_zip columns and getting rid of address_zip and lat_lon columns
    afdc_df['station_address'] = afdc_df['station_address_x'].fillna(afdc_df['station_address_y'])
    afdc_df['station_zip'] = afdc_df['station_zip_x'].fillna(afdc_df['station_zip_y'])
    afdc_df = afdc_df.drop(['station_address_x', 'station_address_y', 'station_zip_x', 'station_zip_y'], axis=1)

    # getting rid of venues for any ones that are conflicting
    afdc_df.loc[(((afdc_df['venue_x']!=afdc_df['venue_y']) & (afdc_df['venue_x'].notnull()) & (afdc_df['venue_y'].notnull())) | ((afdc_df['venue_x']!=afdc_df['venue']) & (afdc_df['venue_x'].notnull()) & (afdc_df['venue'].notnull())) | ((afdc_df['venue_y']!=afdc_df['venue']) & (afdc_df['venue_y'].notnull()) & (afdc_df['venue'].notnull()))), 'venue'] = "Unknown"
    afdc_df.loc[(((afdc_df['venue_x']!=afdc_df['venue_y']) & (afdc_df['venue_x'].notnull()) & (afdc_df['venue_y'].notnull())) | ((afdc_df['venue_x']!=afdc_df['venue']) & (afdc_df['venue_x'].notnull()) & (afdc_df['venue'].notnull())) | ((afdc_df['venue_y']!=afdc_df['venue']) & (afdc_df['venue_y'].notnull()) & (afdc_df['venue'].notnull()))), 'venue_x'] = "Unknown"
    afdc_df.loc[(((afdc_df['venue_x']!=afdc_df['venue_y']) & (afdc_df['venue_x'].notnull()) & (afdc_df['venue_y'].notnull())) | ((afdc_df['venue_x']!=afdc_df['venue']) & (afdc_df['venue_x'].notnull()) & (afdc_df['venue'].notnull())) | ((afdc_df['venue_y']!=afdc_df['venue']) & (afdc_df['venue_y'].notnull()) & (afdc_df['venue'].notnull()))), 'venue_y'] = "Unknown"

    # getting rid of charging stations that do not have any corresponding sessions
    afdc_df = afdc_df[afdc_df['connected_mins'].notnull()]

    # creating single venues, station_address, station_zip columns and getting rid of address_zip and lat_lon columns
    afdc_df['venue'] = afdc_df['venue'].fillna(afdc_df['venue_x'])
    afdc_df['venue'] = afdc_df['venue'].fillna(afdc_df['venue_y'])
    afdc_df['station_address'] = afdc_df['station_address_x'].fillna(afdc_df['station_address_y'])
    afdc_df['station_zip'] = afdc_df['station_zip_x'].fillna(afdc_df['station_zip_y'])
    afdc_df = afdc_df.drop(['venue_x', 'venue_y', 'station_address_x', 'station_address_y', 'station_zip_x', 'station_zip_y'], axis=1)

    # adding Jan 1 2017 payment info from AFDC station locator
    afdc_stations_paid_2017 = pd.read_feather(AFDC_STATION_LOCATION_DIR + '/' + '2017_alt_fuel_stations_paid.feather')

    afdc_df = afdc_df.merge(afdc_stations_paid_2017, left_on='address_zip', right_on='address_zip', how='outer')

    afdc_df = afdc_df[afdc_df["connected_mins"].notnull()]

    # adding Jan 1 2018 payment info from AFDC station locator
    afdc_stations_paid_2018 = pd.read_feather(AFDC_STATION_LOCATION_DIR + '/' + '2018_alt_fuel_stations_paid.feather')

    afdc_df = afdc_df.merge(afdc_stations_paid_2018, left_on='address_zip', right_on='address_zip', how='outer')

    afdc_df = afdc_df[afdc_df["connected_mins"].notnull()]

    afdc_df.loc[((afdc_df['station_ev_pricing_designation_x']!=afdc_df['station_ev_pricing_designation_y']) & (afdc_df['station_ev_pricing_designation_x'].notnull()) & (afdc_df['station_ev_pricing_designation_y'].notnull())) | ((afdc_df['station_ev_pricing_designation_x']!=afdc_df['station_ev_pricing_designation']) & (afdc_df['station_ev_pricing_designation_x'].notnull()) & (afdc_df['station_ev_pricing_designation'].notnull())) | ((afdc_df['station_ev_pricing_designation_y']!=afdc_df['station_ev_pricing_designation']) & (afdc_df['station_ev_pricing_designation_y'].notnull()) & (afdc_df['station_ev_pricing_designation'].notnull())), 'station_ev_pricing_designation_x'] = pd.NA
    afdc_df.loc[((afdc_df['station_ev_pricing_designation_x']!=afdc_df['station_ev_pricing_designation_y']) & (afdc_df['station_ev_pricing_designation_x'].notnull()) & (afdc_df['station_ev_pricing_designation_y'].notnull())) | ((afdc_df['station_ev_pricing_designation_x']!=afdc_df['station_ev_pricing_designation']) & (afdc_df['station_ev_pricing_designation_x'].notnull()) & (afdc_df['station_ev_pricing_designation'].notnull())) | ((afdc_df['station_ev_pricing_designation_y']!=afdc_df['station_ev_pricing_designation']) & (afdc_df['station_ev_pricing_designation_y'].notnull()) & (afdc_df['station_ev_pricing_designation'].notnull())), 'station_ev_pricing_designation_y'] = pd.NA
    afdc_df.loc[((afdc_df['station_ev_pricing_designation_x']!=afdc_df['station_ev_pricing_designation_y']) & (afdc_df['station_ev_pricing_designation_x'].notnull()) & (afdc_df['station_ev_pricing_designation_y'].notnull())) | ((afdc_df['station_ev_pricing_designation_x']!=afdc_df['station_ev_pricing_designation']) & (afdc_df['station_ev_pricing_designation_x'].notnull()) & (afdc_df['station_ev_pricing_designation'].notnull())) | ((afdc_df['station_ev_pricing_designation_y']!=afdc_df['station_ev_pricing_designation']) & (afdc_df['station_ev_pricing_designation_y'].notnull()) & (afdc_df['station_ev_pricing_designation'].notnull())), 'station_ev_pricing_designation'] = pd.NA

    # creating single venues, station_address, station_zip columns and getting rid of address_zip and lat_lon columns
    afdc_df['station_ev_pricing_designation'] = afdc_df['station_ev_pricing_designation'].fillna(afdc_df['station_ev_pricing_designation_x'])
    afdc_df['station_ev_pricing_designation'] = afdc_df['station_ev_pricing_designation'].fillna(afdc_df['station_ev_pricing_designation_y'])
    afdc_df['station_address'] = afdc_df['station_address_x'].fillna(afdc_df['station_address_y'])
    afdc_df['station_zip'] = afdc_df['station_zip_x'].fillna(afdc_df['station_zip_y'])
    afdc_df = afdc_df.drop(['station_ev_pricing_designation_x', 'station_ev_pricing_designation_y', 'station_address_x', 'station_address_y', 'station_zip_x', 'station_zip_y'], axis=1)

    # adding Jan 1 2020 AFDC station locator venue data
    afdc_stations_hist_2020 = pd.read_feather(AFDC_STATION_LOCATION_DIR + '/' + '2020_hist_alt_fuel_stations_cleaned.feather')
    afdc_df = afdc_df.merge(afdc_stations_hist_2020, left_on='address_zip', right_on='address_zip', how='outer')
    afdc_df = afdc_df[afdc_df['connected_mins'].notnull()]
    afdc_df.loc[(((afdc_df['venue_x']!=afdc_df['venue_y']) & (afdc_df['venue_x'].notnull()) & (afdc_df['venue_y'].notnull()))), 'venue_x'] = "Unknown"
    afdc_df.loc[(((afdc_df['venue_x']!=afdc_df['venue_y']) & (afdc_df['venue_x'].notnull()) & (afdc_df['venue_y'].notnull()))), 'venue_y'] = "Unknown"
    
    # creating single venues, station_address, station_zip columns and getting rid of address_zip and lat_lon columns
    afdc_df['venue'] = afdc_df['venue_x'].fillna(afdc_df['venue_y'])
    afdc_df['station_address'] = afdc_df['station_address_x'].fillna(afdc_df['station_address_y'])
    afdc_df['station_zip'] = afdc_df['station_zip_x'].fillna(afdc_df['station_zip_y'])
    afdc_df = afdc_df.drop(['venue_x', 'venue_y', 'station_address_x', 'station_address_y', 'station_zip_x', 'station_zip_y', 'address_zip'], axis=1)

    afdc_df['station_ev_pricing_designation'] = afdc_df['station_ev_pricing_designation'].astype("string")
    afdc_df['venue'] = afdc_df['venue'].astype("string")
    afdc_df['station_address'] = afdc_df['station_address'].astype("string")
    afdc_df['postal_code'] = afdc_df['postal_code'].astype("string")
    afdc_df['station_zip'] = afdc_df['station_zip'].astype("string")

    #storing AFDC datapoints in feather format
    afdc_df_DC = afdc_df[afdc_df['charging_level']=='DC']
    afdc_df_DC.to_feather(output_dir + "/" + output_file_dc)
    afdc_df_L2 = afdc_df[afdc_df['charging_level']=='L2']
    afdc_df_L2.to_feather(output_dir + "/" + output_file_l2)

    return (afdc_df_L2, afdc_df_DC)
