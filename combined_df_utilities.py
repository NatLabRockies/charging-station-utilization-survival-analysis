"""
@Author: Robin Steuteville
Objective: Functions to use when accessing and cleaning combined data
Date: April 13, 2026
Contact: Robin.Steuteville@nlr.gov
Corresponding Author: Ranjit Desai (Ranjit.Desai@nlr.gov)
"""

import pandas as pd

# takes day of the week, and returns whether it is a weekday or weekend
def day_type(day):
    if day == 'Saturday' or day == 'Sunday':
        day_type = 'Weekend'
    elif day == 'Monday' or day == 'Tuesday' or day == 'Wednesday' or day == 'Thursday' or day == 'Friday':
        day_type = 'Weekday'
    else:
        day_type = 'NA'
    return day_type

# creates uniform venue types based on EVW and AFDC venue types
def update_venue_names(venue_type):
    if not pd.isna(venue_type):
        if venue_type == 'Medical or Educational Campus':
            venue_type = 'Med. or Educ.'
        if venue_type == 'Municipal Building' or venue_type == 'FRSTN':
            venue_type = 'Govt. Office'
        if venue_type == 'Multi-use Parking Garage/Lot':
            venue_type = 'Parking'
        if venue_type == 'OTHENT':
            venue_type = 'Leisure Destination'
        if venue_type == 'FACTORY' or venue_type == 'Business Office':
            venue_type = 'Business'
        if venue_type == 'Leisure Destination' or venue_type == 'ARN' or venue_type == 'Arena':
            venue_type = 'Leisure'
        if venue_type == 'SRVSTN':
            venue_type == 'Retail'
        if venue_type == 'RSTSTP':
            venue_type = 'Rest Stop'
        if venue_type == 'TRKSTP':
            venue_type = 'Truck Stop'
        if venue_type == 'UTL':
            venue_type = 'Utility'
        if venue_type == 'OTH':
            venue_type = 'Other'
        if venue_type == 'ARPRT':
            venue_type = 'Airport'
        if venue_type == 'STNDALN':
            venue_type = 'Standalone Station'
        if venue_type == 'MULTI_UNIT_DWELLING' or venue_type == 'Multi-Unit Dwelling':
            venue_type = 'MUD'
        if venue_type == 'Single Family Residential':
            venue_type = 'SFR'
    return venue_type

# creates slightly more general uniform venue types based on EVW and AFDC venue types
def update_venue_names_less_specific(venue_type):
    if not pd.isna(venue_type):
        if venue_type == 'Rest Stop' or venue_type == 'Truck Stop' or venue_type == 'Transit Facility':
            venue_type = 'Travel'
        if venue_type == 'MUD' or venue_type == 'SFR':
            venue_type = 'Private Housing'
        if venue_type == 'Utility' or venue_type == 'Other' or venue_type == 'Standalone Station' or venue_type == 'FLRSLR' or venue_type == 'RFL' or venue_type == 'RTR' or venue_type == 'ARS':
            venue_type = 'Other'
    return venue_type

# sorts hours of the day into 6-hour periods
def every_six_hours(hour):
    if not pd.isna(hour):
        if 0.<=hour<6.:
            hour = "12am - 5:59am"
        elif 6.<=hour<12.:
            hour = "6am - 11:59am"
        elif 12.<=hour<18.:
            hour = "12pm - 5:59pm"
        elif 18.<=hour<24.:
            hour = "6pm - 11:59pm"
    return hour

# sorts hours of the day into 3-hour periods
def every_three_hours(hour):
    if not pd.isna(hour):
        if 0.<=hour<3.:
            hour = "12am - 2:59am"
        elif 3.<=hour<6.:
            hour = "3am - 5:59am"
        elif 6.<=hour<9.:
            hour = "6am - 8:59am"
        elif 9.<=hour<12.:
            hour = "9am - 11:59am"
        elif 12.<=hour<15.:
            hour = "12pm - 2:59pm"
        elif 15.<=hour<18.:
            hour = "3pm - 5:59pm"
        elif 18.<=hour<21.:
            hour = "6pm - 8:59pm"
        elif 21.<=hour<24.:
            hour = "9pm - 11:59pm"
    return hour

# combines EVW and AFDC data for yearly analysis
def combine_basic_df(AFDC_cleaned_L2: pd.DataFrame, AFDC_cleaned_DC: pd.DataFrame, EVW_cleaned_L2: pd.DataFrame, EVW_cleaned_DC: pd.DataFrame, output_dir, output_l2_file, output_dc_file):
    #appending the dataframes:
    combined_L2 = pd.concat([AFDC_cleaned_L2,EVW_cleaned_L2], ignore_index = True)
    combined_DC = pd.concat([AFDC_cleaned_DC,EVW_cleaned_DC], ignore_index = True)

    # not 2023
    combined_L2 = combined_L2[combined_L2['year']!=2023]
    combined_DC = combined_DC[combined_DC['year']!=2023]

    # not 2015
    combined_L2 = combined_L2[combined_L2['year']!=2015]
    combined_DC = combined_DC[combined_DC['year']!=2015]

    # not 2016
    combined_L2 = combined_L2[combined_L2['year']!=2016]
    combined_DC = combined_DC[combined_DC['year']!=2016]

    #make column for weekend or weekday
    combined_L2["day"] = combined_L2.start_time.dt.day_name().astype('str')
    combined_DC["day"] = combined_DC.start_time.dt.day_name().astype('str')

    combined_L2['day_type'] = combined_L2['day'].apply(day_type)
    combined_DC['day_type'] = combined_DC['day'].apply(day_type)

    # update types for day/day type
    combined_L2['day'] = combined_L2['day'].astype("string")
    combined_L2['day_type'] = combined_L2['day_type'].astype("string")
    combined_DC['day'] = combined_DC['day'].astype("string")
    combined_DC['day_type'] = combined_DC['day_type'].astype("string")

    # updating venue names
    combined_L2['venue_specific'] = combined_L2['venue'].apply(update_venue_names).astype("string")
    combined_DC['venue_specific'] = combined_DC['venue'].apply(update_venue_names).astype("string")

    combined_L2['venue'] = combined_L2['venue'].apply(update_venue_names).astype("string").apply(update_venue_names_less_specific).astype("string")
    combined_DC['venue'] = combined_DC['venue'].apply(update_venue_names).astype("string").apply(update_venue_names_less_specific).astype("string")

    # creating separate datetime and time (no date) columns
    combined_L2['start_datetime'] = combined_L2['start_time']
    combined_DC['start_datetime'] = combined_DC['start_time']
    combined_L2['start_time'] = combined_L2['start_datetime'].dt.time
    combined_DC['start_time'] = combined_DC['start_datetime'].dt.time

    # Adding columns to help with splitting up by time of day
    combined_L2['start_hour'] = combined_L2['start_datetime'].dt.hour
    combined_DC['start_hour'] = combined_DC['start_datetime'].dt.hour

    combined_L2['start_half_hour'] = combined_L2['start_datetime'].dt.floor("30min").dt.time
    combined_DC['start_half_hour'] = combined_DC['start_datetime'].dt.floor("30min").dt.time

    combined_L2['six_hour_blocks'] = combined_L2['start_hour'].apply(every_six_hours).astype("string")
    combined_DC['six_hour_blocks'] = combined_DC['start_hour'].apply(every_six_hours).astype("string")

    combined_L2['three_hour_blocks'] = combined_L2['start_hour'].apply(every_three_hours).astype("string")
    combined_DC['three_hour_blocks'] = combined_DC['start_hour'].apply(every_three_hours).astype("string")

    # Saving the combined dataframes
    combined_L2.to_feather(output_dir + "/" + output_l2_file)
    combined_DC.to_feather(output_dir + "/" + output_dc_file)

# combines EVW and AFDC data for venue type analysis
def combine_survival_df(AFDC_cleaned_L2: pd.DataFrame, AFDC_cleaned_DC: pd.DataFrame, EVW_cleaned_L2: pd.DataFrame, EVW_cleaned_DC: pd.DataFrame, output_dir, output_l2_file, output_dc_file):
    #appending the dataframes:
    combined_L2 = pd.concat([AFDC_cleaned_L2,EVW_cleaned_L2], ignore_index = True)
    combined_DC = pd.concat([AFDC_cleaned_DC,EVW_cleaned_DC], ignore_index = True)
    #not 2023
    combined_L2 = combined_L2[combined_L2['year']!=2023]
    combined_DC = combined_DC[combined_DC['year']!=2023]

    # not 2015
    combined_L2 = combined_L2[combined_L2['year']!=2015]
    combined_DC = combined_DC[combined_DC['year']!=2015]

    # not 2016
    combined_L2 = combined_L2[combined_L2['year']!=2016]
    combined_DC = combined_DC[combined_DC['year']!=2016]

    #make column for weekend or weekday
    combined_L2["day"] = combined_L2.start_time.dt.day_name().astype('str')
    combined_DC["day"] = combined_DC.start_time.dt.day_name().astype('str')

    combined_L2['day_type'] = combined_L2['day'].apply(day_type)
    combined_DC['day_type'] = combined_DC['day'].apply(day_type)

    # update types for day/day type
    combined_L2['day'] = combined_L2['day'].astype("string")
    combined_L2['day_type'] = combined_L2['day_type'].astype("string")
    combined_DC['day'] = combined_DC['day'].astype("string")
    combined_DC['day_type'] = combined_DC['day_type'].astype("string")

    # updating venue names
    combined_L2['venue_specific'] = combined_L2['venue'].apply(update_venue_names).astype("string")
    combined_DC['venue_specific'] = combined_DC['venue'].apply(update_venue_names).astype("string")

    combined_L2['venue'] = combined_L2['venue'].apply(update_venue_names).astype("string").apply(update_venue_names_less_specific).astype("string")
    combined_DC['venue'] = combined_DC['venue'].apply(update_venue_names).astype("string").apply(update_venue_names_less_specific).astype("string")

    # creating separate datetime and time (no date) columns
    combined_L2['start_datetime'] = combined_L2['start_time']
    combined_DC['start_datetime'] = combined_DC['start_time']
    combined_L2['start_time'] = combined_L2['start_datetime'].dt.time
    combined_DC['start_time'] = combined_DC['start_datetime'].dt.time

    # Adding columns to help with splitting up by day time
    combined_L2['start_hour'] = combined_L2['start_datetime'].dt.hour
    combined_DC['start_hour'] = combined_DC['start_datetime'].dt.hour

    combined_L2['start_half_hour'] = combined_L2['start_datetime'].dt.floor("30min").dt.time
    combined_DC['start_half_hour'] = combined_DC['start_datetime'].dt.floor("30min").dt.time

    combined_L2['six_hour_blocks'] = combined_L2['start_hour'].apply(every_six_hours).astype("string")
    combined_DC['six_hour_blocks'] = combined_DC['start_hour'].apply(every_six_hours).astype("string")

    combined_L2['three_hour_blocks'] = combined_L2['start_hour'].apply(every_three_hours).astype("string")
    combined_DC['three_hour_blocks'] = combined_DC['start_hour'].apply(every_three_hours).astype("string")

    # Saving the combined dataframes
    combined_L2.to_feather(output_dir + "/" + output_l2_file)
    combined_DC.to_feather(output_dir + "/" + output_dc_file)
