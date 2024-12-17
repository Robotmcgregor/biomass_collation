#!/usr/bin/env python

"""
Biomass collation pipeline
==========================================

Description:


step2.py
===============================
Description: This script initiates the Fractional cover zonal statistics pipeline.
This script:

1. Imports and passes the command line arguments.




Author: Rob McGregor
email: Robert.Mcgregor@nt.gov.au
Date: 04/08/2023
Version: 1.0

###############################################################################################

MIT License

Copyright (c) 2023 Rob McGregor

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the 'Software'), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.


THE SOFTWARE IS PROVIDED 'AS IS', WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

##################################################################################################

===================================================================================================

Command arguments:
------------------

"""

# import modules
import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)
import pandas as pd
from glob import glob
import os
from calendar import monthrange
from datetime import datetime, timedelta
import re
from functools import reduce


def mk_dir_fn(dir_):
    if not os.path.isdir(dir_):
        os.mkdir(dir_)


def export_csv_fn(list_, dir_, file_name):
    if len(list_) > 0:
        df_final = pd.concat(list_, axis=0)
        output_path = os.path.join(dir_, file_name)
        df_final.to_csv(os.path.join(output_path), index=False)
        print("File output to: ", output_path)
    else:
        df_final = None

    return df_final


def start_seasonal_date(date_):
    """ extract the end dates of the seasonal image zonal stats."""

    year = date_[:4]
    month = date_[4:]

    start_date = str(year) + str(month) + "01"

    return start_date


def end_seasonal_date(date_):
    """ extract the start dates of the seasonal image zonal stats."""
    # print("date: ", date_)
    year = str(date_[:4])
    month = str(date_[4:])
    # print("month: ", month)

    month_, day_range = monthrange(int(year), int(month))
    end_date = str(year) + str(month) + str(day_range)
    # print(end_date)
    return end_date


def im_date_season(df):
    """Collate start date of image into im_date column"""

    st_date_list = []
    e_date_list = []
    for i in df.im_name:
        # print(i)
        list_name = i.split("_")
        date = list_name[-2]
        st_date = date[1:7]
        start_date = start_seasonal_date(st_date)
        st_date_list.append(start_date)

        e_date = date[7:]
        end_date = end_seasonal_date(e_date)
        e_date_list.append(end_date)

    df["im_s_date"] = st_date_list
    df["im_e_date"] = e_date_list

    return df


def im_date_annual(df):
    """Collate start date of image into im_date column"""

    st_date_list = []
    e_date_list = []
    for i in df.im_name:
        # print(i)
        list_name = i.split("_")
        date = list_name[-2]
        st_date = str(date) + "01"
        start_date = start_seasonal_date(st_date)
        st_date_list.append(start_date)

        e_date = str(date) + "12"
        end_date = end_seasonal_date(e_date)
        e_date_list.append(end_date)

    df["im_s_date"] = st_date_list
    df["im_e_date"] = e_date_list

    return df


def convert_to_datetime(df, col_nm_s, col_nm_d):
    date_list = []
    for i in df[col_nm_s]:
        #print("i: ", i)
        st_i = str(i)
        #print(len(st_i))
        if len(st_i) == 8:
            datetime_object = datetime.strptime(str(i), '%Y%m%d')
            #print(datetime_object)
            date_list.append(datetime_object)
        elif len(st_i) == 6:
            #print(str(i))
            datetime_object = datetime.strptime(str(i) + "01", '%Y%m%d')
            #print(datetime_object)
            # import sys
            # sys.exit()
            # Add the first day of the month
            #datetime_object = datetime_object.replace(day=1)
            date_list.append(datetime_object)
        else:
            print("error==")
            import sys
            sys.exit()
        # print(datetime_object)
        # df[col_nm_d] =  pd.to_datetime(df[col_nm_s], format='%Y%m%d.%f')
        # date_time = now.strftime("%m/%d/%Y, %H:%M:%S")
    df[col_nm_d] = date_list
    #print("image date completed: ", df)
    #print(list(df))
    return df


def temp_dir_fn(output_dir, pos):
    temp_dir = os.path.join(output_dir, "{0}_temp".format(pos))

    if not os.path.isdir(temp_dir):
        os.mkdir(temp_dir)

    return temp_dir


def out_file_fn(temp_dir, pos, sub_dir, df__, f_type):
    #print("temp dir: ", temp_dir)
    out_file = os.path.join(temp_dir, "{0}_{1}_{2}_zonal_stats.csv".format(pos, sub_dir, f_type))
    df__.to_csv(os.path.join(temp_dir, out_file), index=False)
    print("fire out file: ", out_file)


# def glob_fn(temp_dir):
#     csv_list = []
#     for f in glob(os.path.join(temp_dir, "*.csv")):
#         df__ = pd.read_csv(f)
#         csv_list.append(df__)
#     final_df = pd.concat(csv_list, axis=1)
#
#     return final_df

def glob_fn(temp_dir):
    csv_files = []

    for dirName, subdirList, fileList in os.walk(temp_dir):
        for fname in fileList:
            if fname.endswith('.csv'):
                csv_files.append(os.path.join(dirName, fname))

    csv_list = []
    # Print the list of CSV files
    for csv_file in csv_files:
        #print(csv_file)
        df__ = pd.read_csv(csv_file)
        csv_list.append(df__)

    features = ['uid', 'site_clean', 'site_name', 'date', 'lon_gda94', 'lat_gda94', 'bio_l_kg1ha', 'bio_t_kg1ha',
                'bio_b_kg1ha', 'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha', 'bio_agb_kg1ha',
                'c_l_kg1ha', 'c_t_kg1ha', 'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha', 'c_r_kg1ha',
                'c_agb_kg1ha', 'geometry', 'basal_dt']
    # Use reduce to merge all DataFrames
    from functools import reduce
    merged_df = reduce(lambda left, right: pd.merge(left, right, on=features), csv_list)

    # List of feature names to drop
    features_to_drop = ["mean_f_afyn", 'min_f_afyn', 'max_f_afyn',
                        "mean_f_dfyn", 'min_f_dfyn', 'max_f_dfyn',
                        "mean_f_lfyn", 'min_f_lfyn', 'max_f_lfyn',]

    # Drop the features
    merged_df.drop(columns=features_to_drop, inplace=True)
    print("merged_df.columns: ", merged_df.columns)

    return merged_df


def merge_df_list_fn(df_list):

    print('df_list: ', df_list)
    features = ['uid', 'site_clean', 'site_name', 'date', 'lon_gda94', 'lat_gda94', 'bio_l_kg1ha', 'bio_t_kg1ha',
                'bio_b_kg1ha', 'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha', 'bio_agb_kg1ha',
                'c_l_kg1ha', 'c_t_kg1ha', 'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha', 'c_r_kg1ha',
                'c_agb_kg1ha', 'geometry', 'basal_dt']
    # Use reduce to merge all DataFrames
    from functools import reduce
    merged_df = reduce(lambda left, right: pd.merge(left, right, on=features), df_list)
    merged_df.to_csv(r"C:\Users\robot\projects\biomass\collated_zonal_stats\test_merged_df_fire.csv", index=False)
    # drop columns for data checking
    print("merged_df.columns: ", list(merged_df))

    # import sys
    # sys.exit()
    # # List of feature names to drop
    # features_to_drop = ["mean_f_afyn", 'min_f_afyn', 'max_f_afyn',
    #                     "mean_f_dfyn", 'min_f_dfyn', 'max_f_dfyn',
    #                     "mean_f_lfyn", 'min_f_lfyn', 'max_f_lfyn',]
    #merged_df = pd.concat(df_list, ignore_index=True)
    # Drop the features
    #merged_df.drop(columns=features_to_drop, inplace=True)
    print("merged_df.columns: ", merged_df.columns)

    return merged_df

def export_fn(output_dir, pos, dff):
    print("output export dir: ", dff.columns)
    out = os.path.join(output_dir, "{0}_met_zonal_stats.csv".format(pos))
    dff.to_csv((out), index=False)
    print("output to: ", out)


def dropnull_lat_3(df):
    # Assuming you have a DataFrame df

    # Select the last three columns
    last_three_columns = df.columns[-3:]

    # Drop rows where any of the last three columns have null values
    df_cleaned = df.dropna(subset=last_three_columns)

    # Print the first few rows to verify the changes
    print(df_cleaned.head())
    return df_cleaned

def fire_merge(dfs):
    # Concatenate all DataFrames
    concatenated_df = pd.concat(dfs, ignore_index=True)
    print("concatenated_df: ", concatenated_df)
    concatenated_df.to_csv(r"H:\scratch\concat_df.csv", index=False)
    # Group by common columns and aggregate the last three columns
    # Assuming the last three columns may have different names, we'll aggregate them accordingly

    # Identify common columns (all columns except the last three)
    common_columns = concatenated_df.columns[:-3].tolist()

    # For each of the last three columns, use groupby and aggregate with first non-null value
    for col in concatenated_df.columns[-3:]:
        concatenated_df[col] = concatenated_df.groupby(common_columns)[col].transform('first')

    # Drop duplicates to remove any redundant rows after grouping
    final_df = concatenated_df.drop_duplicates()

    # Print the resulting DataFrame
    print(final_df)
    final_df.to_csv(r"H:\scratch\final_df.csv", index=False)

    # Verify the number of rows
    print(f"Number of rows: {final_df.shape[0]}")

    return final_df

def groupby_site_name(df):
    # Identify common columns (excluding the last three)
    common_columns = df.columns[:-3].tolist()

    # Identify the last three columns
    last_three_columns = df.columns[-3:].tolist()

    # Group by 'site_name' and aggregate
    grouped_df = df.groupby('site_name').agg({**{col: 'first' for col in common_columns if col != 'site_name'},
                                              **{col: 'first' for col in last_three_columns}}).reset_index()

    # Display the shape and first few rows of the resulting DataFrame
    grouped_df.shape, grouped_df.head()

    return grouped_df

def merge_biomass(biomass, fire):


    print("biomass: ", biomass)
    print(list(biomass))
    print("fire: ", fire)
    print(list(fire))

    # Identify the first 23 columns, excluding the second column
    merge_columns = biomass.columns[:23].tolist()
    merge_columns.pop(1)  # Remove the second column (index 1)

    # Merge the DataFrames with a left join
    merged_df = pd.merge(biomass, fire, on=merge_columns, how='left')

    # Fill any NaN values with 0
    merged_df.fillna(0, inplace=True)

    # Drop columns that contain 'fysn' in their names
    merged_df = merged_df.loc[:, ~merged_df.columns.str.contains('fyn')]
    merged_df.to_csv(r"H:\scratch\merged_df.csv", index=False)

    # Display the result
    print(merged_df.head())

    return merged_df


def collect_csv_files_to_df(root_dir):
    dfs = []  # List to store DataFrames

    # Iterate through all directories and subdirectories
    for root, dirs, files in os.walk(root_dir):
        csv_files = [f for f in files if f.endswith('.csv')]  # Find all .csv files

        if csv_files:
            # Sort CSV files to concatenate in alphabetical order (optional)
            csv_files.sort()

            # Read each CSV file and append it to the DataFrame list
            for csv_file in csv_files:
                csv_path = os.path.join(root, csv_file)
                #print("csv_path: ", csv_path)
                df = pd.read_csv(csv_path)  # Read the CSV file into a DataFrame
                dfs.append(df)  # Append DataFrame to the list

    # Concatenate all DataFrames into one DataFrame
    if dfs:
        combined_df = pd.concat(dfs, ignore_index=True)
        return combined_df
    else:
        return pd.DataFrame()  # Return empty DataFrame if no CSVs found


# Function to filter rows based on whether the substring from d_type exists in fire_dict
def filter_rows_based_on_dict(df, fire_dict):
    # Create a new column by extracting the substring from the 8th character onwards
    df['d_type_substr'] = df['d_type'].str[8:]

    # Filter rows where the substring from 'd_type_substr' exists as a key in fire_dict
    df_filtered = df[df['d_type_substr'].isin(fire_dict.keys())]

    return df_filtered


def missing_values(df1, df2, column_name):
    # Find entries unique to df1
    duplicated_in_df2 = df2[df2[column_name].duplicated()][column_name]
    duplicated_in_both = set(df1[column_name]) & set(df2[column_name])

    print("\nEntries duplicated in df2 (count):", len(duplicated_in_df2))
    print("Entries duplicated in df2:")
    print(duplicated_in_df2 if not duplicated_in_df2.empty else "None")

    print("\nEntries duplicated in both DataFrames (count):", len(duplicated_in_both))
    print("Entries duplicated in both DataFrames:")
    print(duplicated_in_both if duplicated_in_both else "None")

    missing_from_df2 = set(df1[column_name]) - set(df2[column_name])
    # Find entries unique to df2
    missing_from_df1 = set(df2[column_name]) - set(df1[column_name])

    print("Entries in the first DataFrame missing from the second (count):", len(missing_from_df2))
    print("Entries in the first DataFrame missing from the second:")
    print(missing_from_df2 if missing_from_df2 else "None")

    print("\nEntries in the second DataFrame missing from the first (count):", len(missing_from_df1))
    print("Entries in the second DataFrame missing from the first:")
    print(missing_from_df1 if missing_from_df1 else "None")

def main_routine(biomass_csv, fire_dir,
                 output_dir, dp0_dbg_si_single_annual_density_near_met,
    dp0_dbg_si_mask_single_annual_density_near_met,
    dp0_dbg_si_single_dry_density_near_met,
    dp0_dbg_si_mask_single_dry_density_near_met,
    dp1_dbi_si_annual_density_near_met,
    dp1_dbi_si_annual_mask_density_near_met,
    dp1_dbi_si_dry_density_near_met,
    dp1_dbi_si_dry_mask_density_near_met):


    """    biomass_csv, dir_,
                 output_dir, dp0_dbg_si_single_annual_density_near_met,
    dp0_dbg_si_mask_single_annual_density_near_met,
    dp0_dbg_si_single_dry_density_near_met,
    dp0_dbg_si_mask_single_dry_density_near_met,
    dp1_dbi_si_annual_density_near_met,
    dp1_dbi_si_annual_mask_density_near_met,
    dp1_dbi_si_dry_density_near_met,
    dp1_dbi_si_dry_mask_density_near_met)"""
    # biomass_csv = r"C:\Users\robot\projects\biomass\collated_agb\20240707\slats_tern_biomass.csv"
    #
    # fire_dir = r"C:\Users\robot\projects\biomass\zonal_stats_raw\fire_nafi_200_2024"
    # output_dir = r"C:\Users\robot\projects\biomass\scratch_outputs\fire_zonal"
    # print("fire_dir: ", fire_dir)
    # import sys
    # sys.exit()

    fire_dict = {"ann_fsm_afsum": ["afsm", 0],
                 "ann_fyn_afysn": ["afyn", 0],
                 "ann_pos_aposf": ["apos", 0],
                 "ann_rio_afrio": ["ario", 0],
                 "ann_gap_aavgf": ["agap", "im_name"],

                 "dry_fsm_dfsum": ["dfsm", 0],
                 "dry_fyn_dfysn": ["dfyn", 0],
                 "dry_pos_dposf": ["dpos", 0],
                 "dry_rio_dfrio": ["drio", 0],
                 "dry_gap_davgf": ["dgap", "im_name"],

                 "lds_fsm_lfsum": ["lfsm", 0],
                 "lds_fyn_lfysn": ["lfyn", 0],
                 "lds_pos_lposf": ["lpos", 0],
                 "lds_rio_lfrio": ["lrio", 0],
                 "lds_gap_lavgf": ["lgap", "im_name"],

                 "cor": ["corr", 0],

                 }

    biomass_df = pd.read_csv(biomass_csv)

    biomass_df = convert_to_datetime(biomass_df, "date", "basal_dt")
    biomass_df.sort_values(by='basal_dt', inplace=True)
    biomass_df.info()
    print(list(biomass_df.columns))

    # import sys
    #
    # sys.exit()
    near_df_list = []
    ml_near_df_list = []
    # correct biomass site name
    site_list = []
    site_name = biomass_df["site"].tolist()
    for i in site_name:
        # print(i)
        n = i.replace("_", ".")
        # print(n)
        site_list.append(n)

    biomass_df["site_clean"] = site_list
    biomass_df['site_clean'] = biomass_df['site_clean'].str.upper()

    biomass_df.to_csv(r"H:\scratch\biomass.csv", index=False)

    # Call the function and get the concatenated DataFrame
    final_df = collect_csv_files_to_df(fire_dir)
    #print("final_df shape: ", final_df.shape)

    # Apply the filter function
    filtered_df = filter_rows_based_on_dict(final_df, fire_dict)
    print("filtered_df shape: ", filtered_df.shape)
    #print(list(filtered_df.columns))

    # Create a new column 'site_name' by converting the 'site' column to uppercase
    filtered_df['site_clean'] = filtered_df['site'].str.upper()

    #print(list(filtered_df))

    combined_df = convert_to_datetime(filtered_df, "im_date", "image_dt")
    print("filtered_df head: ", combined_df.head())
    print("filtered_df columns: ", list(combined_df))
    # import sys
    # sys.exit("496")

    # Ensure both DataFrames are sorted by the respective keys before merge_asof
    biomass_df.sort_values(by='basal_dt', inplace=True)
    biomass_df.to_csv(r"H:\scratch\nafi_stats\biomass.csv", index=False)
    # import sys
    # sys.exit()

    d_type_substr_list = combined_df['d_type_substr'].unique()
    print("d_type_suvstring_list: ", d_type_substr_list)

    merged_df_list = []
    for d_type_substr in d_type_substr_list:
        print("d_type_substr: ", d_type_substr)
        # if d_type_substr == "lds_rio_lfrio":
        #     print("located: ", "lds_rio_lfrio")
        #     import sys
        #     sys.exit("found it!!!")
        # continue
        # Filter and sort combined_df for the current d_type_substr
        temp_df = combined_df[combined_df['d_type_substr'] == d_type_substr].sort_values(by='image_dt')
        # Remove the last 4 characters from 'site_clean'
        temp_df['site_clean'] = temp_df['site_clean'].str[:-4]
        #print("temp_df: ", temp_df.shape)
        temp_df.to_csv(r"H:\scratch\nafi_stats\scratch\temp_df_{0}.csv".format(d_type_substr), index=False)
        # Drop rows where there are NaN values in the columns 'mean', 'min', and 'max'
        #temp_df = temp_df.dropna(subset=['mean'])

        # Check if the variable exists in fire_dict
        if d_type_substr in fire_dict:


            # Check if the variable exists in fire_dict
            if d_type_substr in fire_dict:
                # Retrieve the value corresponding to the d_type_substr
                variable, condition = fire_dict[d_type_substr]

                # Example DataFrame operations based on the condition
                if condition == 0:
                    # If the condition is 0, fill NaN with a specific value (e.g., 0)
                    #print(list(temp_df))
                    # import sys
                    # sys.exit()
                    temp_df_filled = temp_df.fillna({'mean': 0, 'maximum': 0, 'minimum': 0})
                    #print(f"Processed with 'fillna' for variable: {variable}")

                    temp_df_filled.rename(columns={
                        'mean': 'mean_{0}'.format(variable),
                        'maximum': 'max_{0}'.format(variable),
                        'minimum': 'min_{0}'.format(variable),
                        'max': 'max_{0}'.format(variable),
                        'min': 'min_{0}'.format(variable),
                    }, inplace=True)

                    # if d_type_substr == "ann_fsm_afsum":
                    #     variable, condition = fire_dict[d_type_substr]
                    #     print("variable: ", variable)
                    #     print("condition: ", condition)
                    #     print("temp_df_filled: ", list(temp_df_filled))
                    #     import sys
                    #     sys.exit()

                    temp_df_filled.to_csv(r"H:\scratch\nafi_stats\scratch\temp_df_filled_{0}.csv".format(d_type_substr),
                                          index=False)
                    print("exported temp_df_filled", r"H:\scratch\nafi_stats\scratch\temp_df_filled_{0}.csv".format(d_type_substr))

                else:
                    # Extract the file name from temp_df[condition]
                    file_name_series = temp_df[condition]  # Assuming 'condition' is a column name in temp_df
                    #print("file_name_series:", file_name_series)

                    # Initialize a list to store year differences
                    year_differences = []

                    for file_name in file_name_series:
                        if isinstance(file_name, str):
                            match = re.search(r'(\d{4})\d{2}(\d{4})\d{2}', file_name)
                            #print("match: ", match)
                            if match:
                                # Extract years and calculate year difference
                                start_year = int(match.group(1))  # First year (2000)
                                end_year = int(match.group(2))  # Second year (2023)
                                year_difference = end_year - start_year  # Calculate the difference
                                year_differences.append(year_difference)
                            else:
                                import sys
                                sys.exit("ERROR: Regex did not match!")
                                year_differences.append(None)  # Handle cases where regex does not match
                        else:
                            year_differences.append(None)  # Handle non-string entries
                            import sys
                            sys.exit("ERROR: File name is not a string!")
                    # Assign year differences to the DataFrame
                    temp_df['year_difference'] = year_differences

                    # Fill NaN in specified columns with the corresponding year difference
                    temp_df_filled = temp_df.copy()
                    #print("temp_df_filled: ", list(temp_df_filled))
                    # import sys
                    # sys.exit()
                    temp_df_filled[f'mean'] = temp_df_filled['mean'].fillna(temp_df_filled['year_difference'])
                    temp_df_filled[f'maximum'] = temp_df_filled['maximum'].fillna(temp_df_filled['year_difference'])
                    temp_df_filled[f'minimum'] = temp_df_filled['minimum'].fillna(temp_df_filled['year_difference'])
                    temp_df_filled.drop(columns=['year_difference'], inplace=True)
                    temp_df_filled.to_csv(r"H:\scratch\nafi_stats\scratch\temp_df_filled_{0}.csv".format(d_type_substr), index=False)
                    print("exported temp_df_filled", r"H:\scratch\nafi_stats\scratch\temp_df_filled_{0}.csv".format(d_type_substr))

                    temp_df_filled.rename(columns={
                        'mean': 'mean_{0}'.format(variable),
                        'maximum': 'max_{0}'.format(variable),
                        'minimum': 'min_{0}'.format(variable),
                    }, inplace=True)

                    print(f"Processed with 'fillna' using year differences for variable: {variable}")
                    pass
            else:
                print(f"Variable '{d_type_substr}' not found in fire_dict!")

        else:
            print("d_type_substr not in fire_dict")
            import sys
            sys.exit()


        # Get the first value from fire_dict based on d_type_substr
        suffix_value = fire_dict.get(d_type_substr)[0]

        temp_df_filled.rename(columns={
            'mean': 'mean_{0}'.format(suffix_value),
            'max': 'max'.format(suffix_value),
            'min': 'min'.format(suffix_value),
        }, inplace=True)

        out_temp_csv = r"H:\scratch\nafi_stats\biomass_{0}.csv".format(d_type_substr)
        print("out_temp_csv: ", out_temp_csv)

        temp_df_filled.to_csv(out_temp_csv, index=False)

        # import sys
        # sys.exit("line 543")

        print("biomass shape: ", biomass_df.shape)
        print("temp_df_filled shape: ", temp_df_filled.shape)
        print("Number of unique values in temp_df_filled.site_clean: ", temp_df_filled['site_clean'].nunique())
        missing_values(biomass_df, temp_df_filled, "site_clean")

        # Perform the merge_asof
        merged_df = pd.merge_asof(
            biomass_df,
            temp_df_filled,
            left_on='basal_dt',
            right_on='image_dt',
            by='site_clean',
            direction='nearest',
            suffixes=('', f'_{suffix_value}')
        )

        # Identify rows in biomass_df that didn't merge
        unmerged_rows = biomass_df[~biomass_df['basal_dt'].isin(merged_df['basal_dt'])]

        # Print or process unmerged rows
        if not unmerged_rows.empty:
            print("Rows in the left DataFrame that didn't merge:")
            print(unmerged_rows)
            import sys
            sys.exit()
        else:
            print("All rows in the left DataFrame successfully merged.")

        # Drop unnecessary columns (like image_dt)
        merged_df.drop(columns=['image_dt'], inplace=True)
        print("merged_df: ", list(merged_df))
        print("merged_df shape: ", merged_df.shape)
        # import sys
        # sys.exit("line 561")

        # Replace the original biomass_df with the newly merged DataFrame
        #biomass_df = merged_df
        merged_df_list.append(merged_df)


    merged_df = merge_df_list_fn(merged_df_list)
    #merged_df.drop(columns=["site_clean"], inplace=True)
    merged_df.to_csv(r"C:\Users\robot\projects\biomass\scratch_outputs\fire_biomass_data.csv", index=False)
    print("merged_df: ", merged_df.shape)
    # # Merge all DataFrames left to right
    # merged_df_new = reduce(lambda left, right: pd.merge(left, right, on='site_clean', how='outer'), merged_df_list)
    #
    # merged_df_new.to_csv(r"H:\scratch\nafi_stats\biomass_merged_new_way.csv", index=False)
    #
    # # Display the merged DataFrame
    # print(merged_df_new)
    print(biomass_df)



    # Output the final DataFrame
    print("_" * 100)
    print(biomass_df.shape)
    print("biomass list: ", list(biomass_df))
    biomass_df.to_csv(r"H:\scratch\nafi_stats\biomass_after mergeasof.csv", index=False)
    # import sys
    # sys.exit()

    # Step 1: Keep only the specified columns
    columns_to_keep = [
        'uid', 'site', 'site_name', 'date', 'lat_gda94', 'lon_gda94',
        'bio_b_kg1ha', 'bio_br_kg1ha', 'bio_l_kg1ha', 'bio_r_kg1ha',
        'bio_s_kg1ha', 'bio_t_kg1ha', 'bio_w_kg1ha', 'bio_agb_kg1ha',
        'c_agb_kg1ha', 'c_b_kg1ha', 'c_br_kg1ha', 'c_l_kg1ha', 'c_r_kg1ha',
        'c_s_kg1ha', 'c_t_kg1ha', 'c_w_kg1ha', 'geometry', 'basal_dt', 'site_clean'
    ]

    # Keep only these columns from the DataFrame
    filtered_biomass_df = biomass_df[columns_to_keep]

    filtered_biomass_df.to_csv(r"H:\scratch\nafi_stats\biomass_filtered.csv", index=False)

    # # Step 2: Find and keep only columns that contain 'mean', 'min', and 'max'
    # mean_min_max_columns = [col for col in biomass_df.columns if 'mean' in col or 'min' in col or 'max' in col]

    # Step 2: Find and keep only columns that contain 'mean', 'min', 'max', or are named 'site_clean'
    mean_min_max_columns = [col for col in merged_df.columns if
                            'mean' in col or 'min' in col or 'max' in col or col == 'site_clean']

    # Filter the DataFrame to keep only the desired columns
    filtered_fire_df = merged_df[mean_min_max_columns]

    filtered_fire_df.to_csv(r"H:\scratch\nafi_stats\biomass_mean_min_max.csv", index=False)
    print("biomass_df shape: ", biomass_df.shape)



    # required_columns = [
    #     'uid', 'site', 'site_name', 'date', 'lat_gda94', 'lon_gda94',
    #     'bio_b_kg1ha', 'bio_br_kg1ha', 'bio_l_kg1ha', 'bio_r_kg1ha',
    #     'bio_s_kg1ha', 'bio_t_kg1ha', 'bio_w_kg1ha', 'bio_agb_kg1ha',
    #     'c_agb_kg1ha', 'c_b_kg1ha', 'c_br_kg1ha', 'c_l_kg1ha', 'c_r_kg1ha',
    #     'c_s_kg1ha', 'c_t_kg1ha', 'c_w_kg1ha', 'geometry', 'basal_dt', 'site_clean'
    # ]
    #
    # # # Dynamically include columns containing 'mean', 'min', or 'max'
    # # mean_min_max_columns = [col for col in biomass_df.columns if
    # #                         'mean' in col or 'min' in col or 'max' in col]

    # # Combine explicitly required columns with dynamic mean/min/max columns
    # final_columns = set(columns_to_keep + mean_min_max_columns)
    #
    # # Filter the DataFrame
    # filtered_biomass_df = biomass_df[final_columns]
    #
    # filtered_biomass_df.to_csv(r"H:\scratch\nafi_stats\biomass_mean_min_max_filtered_biomass.csv", index=False)
    # print("biomass_df shape: ", biomass_df.shape)

    # Merge the two DataFrames on 'site_clean' using an inner join
    merged_df = pd.merge(filtered_biomass_df, filtered_fire_df, on='site_clean', how='inner')

    # Display the first few rows of the merged DataFrame
    print(merged_df.head())
    merged_df.to_csv(r"H:\scratch\nafi_stats\biomass_mean_min_max_merged.csv", index=False)
    # Check for any potential mismatches
    missing_sites = set(filtered_biomass_df['site_clean']) - set(filtered_fire_df['site_clean'])
    if missing_sites:
        print("Sites in filtered_biomass_df missing in filtered_fire_df:")
        print(missing_sites)
    else:
        print("All sites matched successfully!")

    # import sys
    # sys.exit()
    # # Combine both filtered datasets (specific columns + mean/min/max columns)
    # final_df = pd.concat([filtered_df, biomass_df[mean_min_max_columns]], axis=1)

    print("filtered_biomass_df: ", filtered_biomass_df.shape)
    print("filtered_fire_df: ", list(filtered_fire_df))
    print("filtered_fire_df: ", filtered_fire_df.shape)
    print("filtered_fire_df: ", list(filtered_fire_df))
    
    # Inner join (default)
    ml_merged_df = pd.merge(filtered_biomass_df, filtered_fire_df, on='site_clean', how='inner')

    # Display the final DataFrame
    print(ml_merged_df.shape)

    # Print the final result
    # final_df.to_csv(r"H:\scratch\test_biomass_collation.csv", index=False)
    ml_merged_df.to_csv(r"H:\scratch\nafi_stats\final_biomass_mean_min_max.csv", index=False)


    import sys
    sys.exit("forced stop")

    # # import sys
    # # sys.exit()
    #ml_merged_df = final_df
    # import sys
    # sys.exit()

    # n_df1 = final_df
    # print(list(n_df1.columns))

    # out = os.path.join(r"C:\Users\robot\projects\biomass\collated_zonal_stats\fire", "near_met.csv")
    #
    # n_df1.to_csv(out, index=False)

    # out = os.path.join(r"C:\Users\robot\projects\biomass\collated_zonal_stats\fire",
    #                    "dp0_dbg_si_single_annual_density.csv")
    # dp0_dbg_si_single_annual_density.to_csv(out, index=False)
    #
    # print("-" * 100)
    # print("-" * 100)
    #
    # print("n_df1: ", list(n_df1.columns))
    # print("dp0_dbg_si_single_annual_density: ", list(dp0_dbg_si_single_annual_density.columns))
    #
    # # ==================================================================================================================
    #
    # # ---------------------------------------------------- n_df --------------------------------------------------------
    #
    # --------------------------------------------- annual merge -------------------------------------------------------

    # dp0_dbg_si_single_annual_density_near_met = pd.read_csv(
    #     r"C:\Users\robot\projects\biomass\collated_zonal_stats\single\dp0_dbg_si_single_annual_density_near_met.csv")
    print("ml_merged_df: ", list(ml_merged_df))
    print("dp0_dbg_si_single_annual_density_near_met: ", list(dp0_dbg_si_single_annual_density_near_met))
    
    print("ml_merged_df: ", ml_merged_df.head())
    print("dp0_dbg_si_single_annual_density_near_met: ", dp0_dbg_si_single_annual_density_near_met.head())
    # import sys
    # sys.exit()
    dp0_dbg_si_single_annual_density_near_met_fire = pd.merge(right=dp0_dbg_si_single_annual_density_near_met,
                                                         left=ml_merged_df,
                                                         how="outer",
                                                         on=['uid', 'site_clean', 'date', 'bio_agb_kg1ha',
                                                             'lon_gda94', 'lat_gda94',
                                                             'geometry',
                                                             'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                                                             'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha',
                                                             'bio_r_kg1ha',
                                                             'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
                                                             'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha',
                                                             'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'
                                                             ])

    # # List of columns (features) to drop
    # columns_to_drop = ['site_name_x', 'geometry', 'basal_dt', 'site_clean', 'site_name_y',
    #    'wfp_dt', 'wfp_dir', 'wfp_seas', 'ccw_dt', 'ccw_dir', 'ccw_seas']

    import sys
    sys.exit("step 6 831")

    # Drop the specified columns
    dp0_dbg_si_single_annual_density_near_met_fire_dropped = dp0_dbg_si_single_annual_density_near_met_fire.drop(columns=columns_to_drop)


    out_file = r"C:\Users\robot\projects\biomass\collated_zonal_stats\single\dp0_dbg_si_single_annual_density_near_met_fire.csv"

    dp0_dbg_si_single_annual_density_near_met_fire.to_csv(out_file, index=False)

    print("dp0_dbg_si_single_annual_density_near_met_fire - output ...", out_file)

    dp0_dbg_si_mask_single_annual_density_near_met_fire = pd.merge(right=dp0_dbg_si_mask_single_annual_density_near_met,
                                                              left=ml_merged_df,
                                                              how="outer",
                                                              on=['uid', 'site_clean', 'date', 'bio_agb_kg1ha',
                                                                  'lon_gda94', 'lat_gda94',
                                                                  'geometry',
                                                                  'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                                                                  'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha',
                                                                  'bio_r_kg1ha',
                                                                  'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
                                                                  'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha',
                                                                  'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'
                                                                  ])

    # List of columns (features) to drop
    columns_to_drop = ['site_name_x', 'geometry', 'basal_dt', 'site_clean', 'site_name_y',
       'wfp_dt', 'wfp_dir', 'wfp_seas', 'ccw_dt', 'ccw_dir', 'ccw_seas']

    # Drop the specified columns
    dp0_dbg_si_mask_single_annual_density_near_met_fire_dropped = dp0_dbg_si_mask_single_annual_density_near_met_fire.drop(columns=columns_to_drop)


    out_file = r"C:\Users\robot\projects\biomass\collated_zonal_stats\single_mask\dp0_dbg_si_mask_single_annual_density_near_met_fire.csv"
    dp0_dbg_si_mask_single_annual_density_near_met_fire.to_csv(out_file, index=False)
    print("out_file: ", out_file)

    # ------------------------------------------------------ dry merge -------------------------------------------------

    dp0_dbg_si_single_dry_density_near_met_fire = pd.merge(right=dp0_dbg_si_single_dry_density_near_met,
                                                      left=ml_merged_df,
                                                      how="outer",
                                                      on=['uid', 'site_clean', 'date', 'bio_agb_kg1ha',
                                                          'lon_gda94', 'lat_gda94',
                                                          'geometry',
                                                          'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                                                          'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha',
                                                          'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
                                                          'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha',
                                                          'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'
                                                          ])

    # # List of columns (features) to drop
    # columns_to_drop = ['geometry', 'basal_dt', 'site_clean', 'site_name_y',
    #    'wfp_dt', 'wfp_dir', 'wfp_seas', 'ccw_dt', 'ccw_dir', 'ccw_seas']
    #
    # # Drop the specified columns
    # dp0_dbg_si_single_dry_density_near_met_dropped = dp0_dbg_si_single_dry_density_near_met.drop(columns=columns_to_drop)

    out_file = r"C:\Users\robot\projects\biomass\collated_zonal_stats\single\dp0_dbg_si_single_dry_density_near_met_fire.csv"
    dp0_dbg_si_single_dry_density_near_met.to_csv(out_file, index=False)
    print("out_file: ", out_file)

    dp0_dbg_si_mask_single_dry_density_near_met_fire = pd.merge(right=dp0_dbg_si_mask_single_dry_density_near_met,
                                                           left=ml_merged_df,
                                                           how="outer",
                                                           on=['uid', 'site_clean', 'date', 'bio_agb_kg1ha',
                                                               'lon_gda94', 'lat_gda94',
                                                               'geometry',
                                                               'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                                                               'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha',
                                                               'bio_r_kg1ha',
                                                               'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
                                                               'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha',
                                                               'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'
                                                               ])
    # # List of columns (features) to drop
    # columns_to_drop = ['site_name_x', 'geometry', 'basal_dt', 'site_clean', 'site_name_y',
    #    'wfp_dt', 'wfp_dir', 'wfp_seas', 'ccw_dt', 'ccw_dir', 'ccw_seas']
    #
    # # Drop the specified columns
    # dp0_dbg_si_mask_single_dry_density_near_met_fire_dropped = dp0_dbg_si_mask_single_dry_density_near_met_fire.drop(columns=columns_to_drop)

    out_file = r"C:\Users\robot\projects\biomass\collated_zonal_stats\single_mask\dp0_dbg_si_mask_single_dry_density_near_met_fire.csv"
    dp0_dbg_si_mask_single_dry_density_near_met_fire.to_csv(out_file, index=False)
    print("out_file: ", out_file)
    # ------------------------------------------------------ annual merge dp1 bbi --------------------------------------

    dp1_dbi_si_annual_density_near_met_fire = pd.merge(right=dp1_dbi_si_annual_density_near_met,
                                                  left=ml_merged_df,
                                                  how="outer",
                                                  on=['uid', 'site_clean', 'date', 'bio_agb_kg1ha',
                                                      'lon_gda94', 'lat_gda94', 'geometry',
                                                      'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                                                      'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha',
                                                      'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
                                                      'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha',
                                                      'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'
                                                      ])

    # # List of columns (features) to drop
    # columns_to_drop = ['site_name_x', 'geometry', 'basal_dt', 'site_clean', 'site_name_y',
    #    'wfp_dt', 'wfp_dir', 'wfp_seas', 'ccw_dt', 'ccw_dir', 'ccw_seas']
    #
    # # Drop the specified columns
    # dp1_dbi_si_annual_density_near_met_fire_dropped = dp1_dbi_si_annual_density_near_met_fire.drop(columns=columns_to_drop)


    out_file =  r"C:\Users\robot\projects\biomass\collated_zonal_stats\annual\dp1_dbi_si_annual_density_near_met_fire.csv"
    dp1_dbi_si_annual_density_near_met_fire.to_csv(out_file, index=False)
    print("out_file: ", out_file)

    dp1_dbi_si_annual_mask_density_near_met_fire = pd.merge(right=dp1_dbi_si_annual_mask_density_near_met,
                                                       left=ml_merged_df,
                                                       how="outer",
                                                       on=['uid', 'site_clean', 'date', 'bio_agb_kg1ha',
                                                           'lon_gda94', 'lat_gda94',
                                                           'geometry',
                                                           'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                                                           'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha',
                                                           'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
                                                           'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha',
                                                           'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'
                                                           ])
    # # List of columns (features) to drop
    # columns_to_drop = ['site_name_x', 'geometry', 'basal_dt', 'site_clean', 'site_name_y',
    #    'wfp_dt', 'wfp_dir', 'wfp_seas', 'ccw_dt', 'ccw_dir', 'ccw_seas']
    #
    # # Drop the specified columns
    # dp1_dbi_si_annual_mask_density_near_met_fire_dropped = dp1_dbi_si_annual_mask_density_near_met_fire.drop(columns=columns_to_drop)


    out_file = r"C:\Users\robot\projects\biomass\collated_zonal_stats\annual_mask\dp1_dbi_si_annual_mask_density_near_met_fire.csv"
    dp1_dbi_si_annual_mask_density_near_met_fire.to_csv(out_file, index=False)
    print("out_file: ", out_file)

    # ---------------------------------------------- dry merge dp1 bbi -------------------------------------------------

    dp1_dbi_si_dry_density_near_met_fire = pd.merge(right=dp1_dbi_si_dry_density_near_met,
                                               left=ml_merged_df,
                                               how="outer",
                                               on=['uid', 'site_clean', 'date', 'bio_agb_kg1ha',
                                                   'lon_gda94', 'lat_gda94', 'geometry',
                                                   'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                                                   'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha',
                                                   'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
                                                   'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha',
                                                   'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'
                                                   ])

    # # List of columns (features) to drop
    # columns_to_drop = ['site_name_x', 'geometry', 'basal_dt', 'site_clean', 'site_name_y',
    #    'wfp_dt', 'wfp_dir', 'wfp_seas', 'ccw_dt', 'ccw_dir', 'ccw_seas']
    #
    # # Drop the specified columns
    # dp1_dbi_si_dry_density_near_met_fire_dropped = dp1_dbi_si_dry_density_near_met_fire.drop(columns=columns_to_drop)


    out_file = r"C:\Users\robot\projects\biomass\collated_zonal_stats\dry\dp1_dbi_si_dry_density_near_df_near_met_fire.csv"
    dp1_dbi_si_dry_density_near_met_fire.to_csv(out_file, index=False)
    print("out_file: ", out_file)

    dp1_dbi_si_dry_mask_density_near_met_fire = pd.merge(right=dp1_dbi_si_dry_mask_density_near_met,
                                                    left=ml_merged_df,
                                                    how="outer",
                                                    on=['uid', 'site_clean', 'date', 'bio_agb_kg1ha',
                                                        'lon_gda94', 'lat_gda94',
                                                        'geometry',
                                                        'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                                                        'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha',
                                                        'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
                                                        'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha',
                                                        'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'
                                                        ])

    # # List of columns (features) to drop
    # columns_to_drop = ['site_name_x', 'geometry', 'basal_dt', 'site_clean', 'site_name_y',
    #    'wfp_dt', 'wfp_dir', 'wfp_seas', 'ccw_dt', 'ccw_dir', 'ccw_seas']
    #
    # # Drop the specified columns
    # dp1_dbi_si_dry_mask_density_near_met_fire_dropped = dp1_dbi_si_dry_mask_density_near_met_fire.drop(columns=columns_to_drop)

    out_file = r"C:\Users\robot\projects\biomass\collated_zonal_stats\dry_mask\dp1_dbi_si_dry_mask_density_near_met_fire.csv"
    dp1_dbi_si_dry_mask_density_near_met_fire.to_csv(out_file, index=False)
    print("out_file: ", out_file)

    print("ml_merged_df: ", ml_merged_df.columns.tolist())
    print("dp0_dbg_si_single_annual_density_near_met_fire: ", dp0_dbg_si_single_annual_density_near_met_fire.columns.tolist())
    print("dp0_dbg_si_mask_single_annual_density_near_met_fire: ", dp0_dbg_si_mask_single_annual_density_near_met_fire.columns.tolist())
    print("dp0_dbg_si_single_dry_density_near_met_fire: ", dp0_dbg_si_single_dry_density_near_met_fire.columns.tolist())
    print("dp0_dbg_si_mask_single_dry_density_near_met_fire: ", dp0_dbg_si_mask_single_dry_density_near_met_fire.columns.tolist())
    print("dp1_dbi_si_annual_density_near_met_fire: ", dp1_dbi_si_annual_density_near_met_fire.columns.tolist())
    print("dp1_dbi_si_annual_mask_density_near_met_fire: ", dp1_dbi_si_annual_mask_density_near_met_fire.columns.tolist())
    print("dp1_dbi_si_dry_density_near_met_fire: ", dp1_dbi_si_dry_density_near_met_fire.columns.tolist())
    print("dp1_dbi_si_dry_mask_density_near_met_fire: ", dp1_dbi_si_dry_mask_density_near_met_fire.columns.tolist())



    return ml_merged_df, dp0_dbg_si_single_annual_density_near_met_fire, \
        dp0_dbg_si_mask_single_annual_density_near_met_fire, \
        dp0_dbg_si_single_dry_density_near_met_fire, \
        dp0_dbg_si_mask_single_dry_density_near_met_fire, \
        dp1_dbi_si_annual_density_near_met_fire, \
        dp1_dbi_si_annual_mask_density_near_met_fire, \
        dp1_dbi_si_dry_density_near_met_fire, \
        dp1_dbi_si_dry_mask_density_near_met_fire,


if __name__ == '__main__':
    main_routine()
