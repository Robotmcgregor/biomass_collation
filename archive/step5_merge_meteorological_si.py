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
from datetime import datetime


def mk_dir_fn(dir_):
    if not os.path.isdir(dir_):
        os.mkdir(dir_)
        print("made dir: ", dir_)


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
        # print(i)
        datetime_object = datetime.strptime(str(i), '%Y%m%d')
        date_list.append(datetime_object)
        # print(datetime_object)
        # df[col_nm_d] =  pd.to_datetime(df[col_nm_s], format='%Y%m%d.%f')
        # date_time = now.strftime("%m/%d/%Y, %H:%M:%S")
    df[col_nm_d] = date_list
    return df


def temp_dir_fn(output_dir, pos):
    temp_dir = os.path.join(output_dir, "{0}_temp".format(pos))

    if not os.path.isdir(temp_dir):
        os.mkdir(temp_dir)

    return temp_dir


def out_file_fn(temp_dir, pos, sub_dir, df__):
    out_file = os.path.join(temp_dir, "{0}_{1}_zonal_stats.csv".format(pos, sub_dir))
    df__.to_csv(os.path.join(temp_dir, out_file), index=False)
    print("output: ", out_file)


def glob_fn(temp_dir):
    csv_list = []
    for f in glob(os.path.join(temp_dir, "*.csv")):
        df__ = pd.read_csv(f)
        csv_list.append(df__)
    final_df = pd.concat(csv_list, axis=1)

    return final_df


def export_fn(output_dir, pos, type_, dff):
    out = os.path.join(output_dir, "{0}_met_{1}_zonal_stats.csv".format(pos, type_))
    dff.to_csv((out), index=False)
    print("output to: ", out)


def export_site_fn(output_dir, pos, type_, dff, s):
    out = os.path.join(output_dir, "{0}_{1}_{2}_met_{3}_zonal_stats.csv".format(s[:-5], s[-4:], pos, type_))
    dff.to_csv((out), index=False)
    print("output to: ", out)


def drop_cols_fn(df):
    df1 = df.copy()
    df_columns = df.columns.tolist()
    print("df_columns: ", df_columns)
    print("-" * 100)

    drop_list = [columns for columns in df_columns if "mean" not in columns]
    print("drop column list: ", drop_list)

    #     print(len(drop_list))
    #     drop_list.remove("site")
    #     drop_list.remove("bio_agb_kg1ha")
    #     print("length of drop: ", len(drop_list))
    #     print("mean column: ", mean_column)
    #     test = mean_column
    #     print("test: ", test)
    df.drop(drop_list, axis=1, inplace=True)
    print("cleaned df shape:", df.shape)

    var_ = df1.iloc[:, [24, 13]]

    var_columns = var_.columns.tolist()
    print("var_columns: ", var_columns)
    df_out = pd.concat([var_, df], axis=1)
    print("cleaned df shape:", df_out.columns.tolist())

    # import sys
    # sys.exit()

    return df_out


def glob_concat_monthly_fn(sub_dir_path):
    sub_dir_list = []
    sub_dir_df_list = []
    for file_ in glob(os.path.join(sub_dir_path, "*.csv")):
        if "zonal_stats" in file_:
            sub_dir_list.append(file_)
            # print(file_)

            df = pd.read_csv(file_)

            # df = convert_to_datetime(df, "im_date", "image_dt")
            # df.sort_values(by='image_dt', inplace=True)
            # print(df.columns)
            sub_dir_df_list.append(df)
    df1 = pd.concat(sub_dir_df_list)

    # correct df site name
    site_list = []
    site_name = df1["site"].tolist()
    site_year = df1["site_year"].tolist()
    for i, y in zip(site_name, site_year):
        n = "{0}.{1}".format(i, str(y))
        print(n)
        site_list.append(n)

    df1["site_clean"] = site_list

    return df1


def glob_concat_seasonal_fn(sub_dir_path):
    sub_dir_list = []
    sub_dir_df_list = []
    for file_ in glob(os.path.join(sub_dir_path, "*.csv")):
        if "zonal_stats" in file_:
            sub_dir_list.append(file_)
            # print(file_)

            df = pd.read_csv(file_)

            # df = convert_to_datetime(df, "im_date", "image_dt")
            # df.sort_values(by='image_dt', inplace=True)
            # print(df.columns)
            sub_dir_df_list.append(df)
    df1 = pd.concat(sub_dir_df_list)

    # correct df site name
    site_list = []
    site_name = df1["site"].tolist()
    site_year = df1["year"].tolist()
    for i, y in zip(site_name, site_year):
        n = "{0}.{1}".format(i, str(y))
        print(n)
        site_list.append(n)

    df1["site_clean"] = site_list

    return df1


def pivot_month_avg(df, sub_dir_dir):
    d = {
        "site": [],
        "year": [],
        "site_clean": [],
        "met_var": [],
        "jan": [],
        "feb": [],
        "march": [],
        "april": [],
        "may": [],
        "june": [],
        "july": [],
        "aug": [],
        "sep": [],
        "oct": [],
        "nov": [],
        "dec": []

    }
    sc_list = df["site_clean"].unique().tolist()

    for sc in sc_list:
        sc_df = df[df["site_clean"] == sc]

        # site values
        d["site_clean"].append(sc)

        site = sc_df.loc[sc_df['month'] == 1, 'site'].values[0]
        d["site"].append(site)

        year = sc_df.loc[sc_df['month'] == 1, 'site_year'].values[0]
        d["year"].append(year)

        d["met_var"].append(sub_dir_dir)

        # monthly valuse
        jan = sc_df.loc[sc_df['month'] == 1, 'var'].values[0]
        d["jan"].append(jan)

        feb = sc_df.loc[sc_df['month'] == 2, 'var'].values[0]
        d["feb"].append(feb)

        march = sc_df.loc[sc_df['month'] == 3, 'var'].values[0]
        d["march"].append(march)

        april = sc_df.loc[sc_df['month'] == 4, 'var'].values[0]
        d["april"].append(april)

        may = sc_df.loc[sc_df['month'] == 5, 'var'].values[0]
        d["may"].append(may)

        june = sc_df.loc[sc_df['month'] == 6, 'var'].values[0]
        d["june"].append(june)

        july = sc_df.loc[sc_df['month'] == 7, 'var'].values[0]
        d["july"].append(july)

        aug = sc_df.loc[sc_df['month'] == 8, 'var'].values[0]
        d["aug"].append(aug)

        sep = sc_df.loc[sc_df['month'] == 9, 'var'].values[0]
        d["sep"].append(sep)

        oct = sc_df.loc[sc_df['month'] == 10, 'var'].values[0]
        d["oct"].append(oct)

        nov = sc_df.loc[sc_df['month'] == 11, 'var'].values[0]
        d["nov"].append(nov)

        dec = sc_df.loc[sc_df['month'] == 12, 'var'].values[0]
        d["dec"].append(dec)

    out_df = pd.DataFrame.from_dict(d)

    return out_df


def total_seasonal_avg(df, sub_dir_dir):
    print("pivot_seasonal_avg.... init")

    d = {
        "site": [],
        "year": [],
        "site_clean": [],
        "met_var": [],
        "tot_avg_djf": [],
        "tot_avg_mam": [],
        "tot_avg_jja": [],
        "tot_avg_son": [],
        "tot_avg_dry": [],
        "tot_avg_wet": [],
        "tot_avg_annual": [],

    }
    sc_list = df["site_clean"].unique().tolist()

    for sc in sc_list:
        sc_df = df[df["site_clean"] == sc]

        # site values
        d["site_clean"].append(sc)
        d["site"].append(sc_df["site"].unique()[0])
        d["year"].append(sc_df["year"].unique()[0])
        d["met_var"].append(sub_dir_dir)

        # total mean value
        d["tot_avg_djf"].append(sc_df["djf"].mean())
        d["tot_avg_mam"].append(sc_df["mam"].mean())
        d["tot_avg_jja"].append(sc_df["jja"].mean())
        d["tot_avg_son"].append(sc_df["son"].mean())
        d["tot_avg_dry"].append(sc_df["dry"].mean())
        d["tot_avg_wet"].append(sc_df["wet"].mean())
        d["tot_avg_annual"].append(sc_df["annual"].mean())

    out_df = pd.DataFrame.from_dict(d)

    return out_df


def total_si_avg(df, sub_dir_dir):
    """ This function calculates the total average for each si variable across all years of data.

    :param df: pandas dataframe object containing the collated si meteorological variable data
    :param sub_dir_dir: string object containing the subdirectory name (i.e. meteorological data variable name).
    :return out_df: pandas dataframe object containing the updated data.
    """
    print("total_si_avg.... init")

    d = {
        "site": [],
        "year": [],
        "site_clean": [],
        "met_var": [],
        "tot_annual_avg": [],
        "tot_annual_avg_R": [],
        "tot_annual_avg_si": [],

    }
    sc_list = df["site_clean"].unique().tolist()

    for sc in sc_list:
        sc_df = df[df["site_clean"] == sc]

        # site values
        d["site_clean"].append(sc)
        d["site"].append(sc_df["site"].unique()[0])
        d["year"].append(sc_df["year"].unique()[0])
        d["met_var"].append(sub_dir_dir)

        # total mean value
        d["tot_annual_avg"].append(sc_df["annual_total"].mean())
        d["tot_annual_avg_R"].append(sc_df["annual_average_R"].mean())
        d["tot_annual_avg_si"].append(sc_df["annual_si"].mean())

    out_df = pd.DataFrame.from_dict(d)
    print("completed: total_si_avg function")

    return out_df


def create_dt_fn(df, col_nm_d):
    # print("init datetime....")
    # print(df.shape)

    date_list = []
    for i in df["year"]:
        # print(i)
        st_i = str(i)
        d = f"{st_i}0615"
        datetime_object = datetime.strptime(str(d), '%Y%m%d')
        date_list.append(datetime_object)

    df[col_nm_d] = date_list

    # sort datetime values
    df.sort_values(by=col_nm_d, inplace=True)
    return df


def merge_fn(biomass_df, df1_dt, output_asof_dir, sub_dir_dir, var_):
    var_list = []

    for s in biomass_df["site_clean"].unique():
        print("working on site: ", s)
        # -------------------------------- biomass ------------------------------
        biomass_s_df = biomass_df[biomass_df["site_clean"] == s]
        # sort values
        biomass_s_df.sort_values(by="basal_dt", inplace=True)
        # --------------------------------- si ----------------------------------------
        total_s = df1_dt[df1_dt["site_clean"] == s]
        # sort values
        total_s.sort_values(by="var_dt", inplace=True)

        # ------------------------------------------ near --------------------------------------------------
        # merge data with basal dataset based on the nearest date to the field data colection
        # n_df = pd.merge_asof(biomass_s_df, total_s, left_on="basal_dt", right_on="var_dt", by="site_clean",
        #                      direction="nearest")
        # n_df["direction"] = "nearest"
        # print(n_df.columns)
        # n_df.drop(['site_y'], axis=1, inplace=True)
        # n_df.rename(columns={"site_x": "site"}, inplace=True)
        # export_site_fn(output_asof_dir, sub_dir_dir, f"{var_}_asof_nearest", n_df, s)
        # var_list.append(n_df)
        #
        # # -------------------------------------- forward ---------------------------------------------------
        # f_df = pd.merge_asof(biomass_s_df, total_s, left_on="basal_dt", right_on="var_dt", by="site_clean",
        #                      direction="forward")
        # f_df["direction"] = "forward"
        # print(f_df.columns)
        # f_df.drop(['site_y'], axis=1, inplace=True)
        # f_df.rename(columns={"site_x": "site"}, inplace=True)
        # export_site_fn(output_asof_dir, sub_dir_dir, f"{var_}_asof_forward", f_df, s)
        # var_list.append(f_df)
        # # ---------------------------------------- backwards ---------------------------------------------------
        # b_df = pd.merge_asof(biomass_s_df, total_s, left_on="basal_dt", right_on="var_dt", by="site_clean",
        #                      direction="backward")
        # b_df["direction"] = "backward"
        # print(b_df.columns)
        # b_df.drop(['site_y'], axis=1, inplace=True)
        # b_df.rename(columns={"site_x": "site"}, inplace=True)
        # export_site_fn(output_asof_dir, sub_dir_dir, f"{var_}_asof_backward", b_df, s)
        # var_list.append(b_df)

    return var_list


def asof_fn(biomass_df, df1_dt, output_asof_dir, sub_dir_dir, var_):
    var_list = []

    for s in biomass_df["site_clean"].unique():
        print("working on site: ", s)
        # -------------------------------- biomass ------------------------------
        biomass_s_df = biomass_df[biomass_df["site_clean"] == s]
        print("biomass_s_df shape: ", biomass_s_df.shape)
        print(biomass_s_df)
        # sort values
        biomass_s_df.sort_values(by="basal_dt", inplace=True)
        print("sorted: ", biomass_s_df)
        # --------------------------------- si ----------------------------------------
        total_s = df1_dt[df1_dt["site_clean"] == s]
        print("total_s shape: ", total_s.shape)
        print(total_s)
        # sort values
        total_s.sort_values(by="var_dt", inplace=True)
        print("sorted: ", total_s.shape)
        total_s.dropna(inplace=True)
        print("sorted dropna: ", total_s.shape)

        # ------------------------------------------ near --------------------------------------------------
        # merge data with basal dataset based on the nearest date to the field data colection
        n_df = pd.merge_asof(biomass_s_df, total_s, left_on="basal_dt", right_on="var_dt", by="site_clean",
                             direction="nearest")
        n_df["direction"] = "nearest"
        print(n_df.columns)
        n_df.drop(['site_y'], axis=1, inplace=True)
        n_df.rename(columns={"site_x": "site"}, inplace=True)
        export_site_fn(output_asof_dir, sub_dir_dir, f"{var_}_asof_nearest", n_df, s)
        var_list.append(n_df)

        # # -------------------------------------- forward ---------------------------------------------------
        # f_df = pd.merge_asof(biomass_s_df, total_s, left_on="basal_dt", right_on="var_dt", by="site_clean",
        #                      direction="forward")
        # f_df["direction"] = "forward"
        # print(f_df.columns)
        # f_df.drop(['site_y'], axis=1, inplace=True)
        # f_df.rename(columns={"site_x": "site"}, inplace=True)
        # export_site_fn(output_asof_dir, sub_dir_dir, f"{var_}_asof_forward", f_df, s)
        # var_list.append(f_df)
        # # ---------------------------------------- backwards ---------------------------------------------------
        # b_df = pd.merge_asof(biomass_s_df, total_s, left_on="basal_dt", right_on="var_dt", by="site_clean",
        #                      direction="backward")
        # b_df["direction"] = "backward"
        # print(b_df.columns)
        # b_df.drop(['site_y'], axis=1, inplace=True)
        # b_df.rename(columns={"site_x": "site"}, inplace=True)
        # export_site_fn(output_asof_dir, sub_dir_dir, f"{var_}_asof_backward", b_df, s)
        # var_list.append(b_df)

    return var_list


def main_routine(biomass_csv, dir_, output_dir,
                 dp0_dbg_si_single_annual_density_near_met,
                 dp0_dbg_si_mask_single_annual_density_near_met,
                 dp0_dbg_si_single_dry_density_near_met,
                 dp0_dbg_si_mask_single_dry_density_near_met,
                 dp1_dbi_si_annual_density_near_met,
                 dp1_dbi_si_annual_mask_density_near_met,
                 dp1_dbi_si_dry_density_near_met,
                 dp1_dbi_si_dry_mask_density_near_met
                 ):
    """This script hunts through the meteorological zonal statistics outputs, collates the data and merges as of with
    the biomass data csv.
    This script should be run following the completion of: step1_initiate_biomass_zonal_stats_collation_pipeline.py

    Outputs are temporarily being produced here: C:\Users\robot\projects\biomasss\zonal\met.
    """

    # biomass = r"C:\Users\robot\projects\biomasss\collated_agb\20230927\slats_tern_biomass.csv"
    biomass_df = pd.read_csv(biomass_csv)

    # dir_ = r"C:\Users\robot\projects\biomasss\raw_zonal_stats\met\collation"
    # output_dir = r"C:\Users\robot\projects\biomasss\zonal\new_met"

    biomass_df = convert_to_datetime(biomass_df, "date", "basal_dt")
    biomass_df.sort_values(by='basal_dt', inplace=True)

    # correct biomass site name
    site_list = []
    site_name = biomass_df["site"].tolist()
    for i in site_name:
        # print(i)
        n = i.replace("_", ".")
        # print(n)
        site_list.append(n)

    biomass_df["site_clean"] = site_list

    # extract a list of subdirectories from the qld meteorological zonal stats
    sub_list = next(os.walk(dir_))[1]
    print("sub_list: ", sub_list)

    si_asof_list = []
    seasonal_asof_list = []

    si = []
    total_si = []
    monthly_list = []
    seasonal_avg = []
    tot_seasonal_avg = []
    zonal_list = []
    sub_dir_list = []
    for sub_dir in sub_list:
        file_list = []
        print("Working on: ", sub_dir)
        sub_dir_path = os.path.join(dir_, sub_dir)
        print(sub_dir_path)

        sub_dir_list = next(os.walk(sub_dir_path))[1]
        print("sub_dir_list: ", sub_dir_list)

        for sub_dir_dir in sub_dir_list:
            print(sub_dir_dir)

            if sub_dir_dir.startswith("monthly"):

                output_monthly_orig_dir = os.path.join(output_dir, "monthly_orig_dir")
                print(output_monthly_orig_dir)
                mk_dir_fn(output_monthly_orig_dir)

                full_dir_path = os.path.join(dir_, sub_dir, sub_dir_dir)
                monthly_data = glob_concat_monthly_fn(full_dir_path)
                # pivot monthly average data
                month_df1 = pivot_month_avg(monthly_data, sub_dir_dir)
                # export csv
                export_fn(output_monthly_orig_dir, sub_dir_dir, "monthly", month_df1)
                # append to list
                monthly_list.append(month_df1)

                # monthly_list = merge_fn(biomass_df, month_df1, output_monthly_orig_dir, sub_dir_dir, "monthly")


            elif sub_dir_dir.startswith("seasonal"):

                output_seasonal_orig_dir = os.path.join(output_dir, "seasonal_orig_dir")
                print(output_seasonal_orig_dir)
                mk_dir_fn(output_seasonal_orig_dir)

                output_seasonal_asof_dir = os.path.join(output_dir, "seasonal_asof_dir")
                print(output_seasonal_asof_dir)
                mk_dir_fn(output_seasonal_asof_dir)

                output_seasonal_concat_asof_dir = os.path.join(output_dir, "seasonal_concat_asof_dir")
                print(output_seasonal_concat_asof_dir)
                mk_dir_fn(output_seasonal_concat_asof_dir)

                full_dir_path = os.path.join(dir_, sub_dir, sub_dir_dir)
                seasonal_data = glob_concat_seasonal_fn(full_dir_path)
                seasonal_data_dt = create_dt_fn(seasonal_data, "var_dt")
                # append original dataset
                seasonal_avg.append(seasonal_data_dt)
                seasonal_df1 = total_seasonal_avg(seasonal_data_dt, sub_dir_dir)
                seasonal_df1_dt = create_dt_fn(seasonal_df1, "var_dt")
                # export csv
                export_fn(output_seasonal_orig_dir, sub_dir_dir, "seasonal", seasonal_df1_dt)
                # append to list
                tot_seasonal_avg.append(seasonal_df1_dt)

                seasonal_list = asof_fn(biomass_df, seasonal_df1_dt, output_seasonal_asof_dir, sub_dir_dir, "seasonal")

                seasonal_concat = pd.concat(seasonal_list)

                print(list(seasonal_concat.columns))

                if sub_dir == "daily_rain":
                    col_str = "drse"

                elif sub_dir == 'et_morton_actual':
                    col_str = "emse"

                elif sub_dir == 'max_temp':
                    col_str = "matse"

                elif sub_dir == 'min_temp':
                    col_str = "mitse"

                elif sub_dir == 'rh_tmax':
                    col_str = "marse"

                elif sub_dir == 'rh_tmin':
                    col_str = "mirse"

                else:
                    print(sub_dir_dir)
                    import sys
                    sys.exit()

                seasonal_concat.rename(columns={'tot_avg_djf': f'tot_avg_{col_str}_djf',
                                                'tot_avg_mam': f'tot_avg_{col_str}_mam',
                                                'tot_avg_jja': f'tot_avg_{col_str}_jja',
                                                'tot_avg_son': f'tot_avg_{col_str}_son',
                                                'tot_avg_dry': f'tot_avg_{col_str}_dry',
                                                'tot_avg_wet': f'tot_avg_{col_str}_wet',
                                                'tot_avg_annual': f'tot_avg_{col_str}_annual',
                                                'var_dt': f'{col_str}_dt'}, inplace=True)

                seasonal_asof_list.append(seasonal_concat)
                export_fn(output_seasonal_concat_asof_dir, sub_dir_dir, "seasonal_merge_asof_concat", seasonal_concat)
                seasonal_concat.dropna(subset=[f'{col_str}_dt'], inplace=True)
                export_fn(output_seasonal_concat_asof_dir, sub_dir_dir, "seasonal_merge_asof_concat_dropna",
                          seasonal_concat)

            if sub_dir_dir.startswith("si"):

                output_si_orig_dir = os.path.join(output_dir, "si_orig_dir")
                print(output_si_orig_dir)
                mk_dir_fn(output_si_orig_dir)

                output_si_asof_dir = os.path.join(output_dir, "si_asof_dir")
                print(output_si_asof_dir)
                mk_dir_fn(output_si_asof_dir)

                output_si_concat_asof_dir = os.path.join(output_dir, "si_concat_asof_dir")
                print(output_si_concat_asof_dir)
                mk_dir_fn(output_si_concat_asof_dir)

                full_dir_path = os.path.join(dir_, sub_dir, sub_dir_dir)
                si_data = glob_concat_seasonal_fn(full_dir_path)
                si_data_dt = create_dt_fn(si_data, "var_dt")
                # append original dataset
                si.append(si_data_dt)
                si_df1 = total_si_avg(si_data_dt, sub_dir_dir)
                si_df1_dt = create_dt_fn(si_df1, "var_dt")
                # export csv
                export_fn(output_si_orig_dir, sub_dir_dir, "si", si_df1_dt)
                # append to list
                total_si.append(si_df1_dt)

                si_list = asof_fn(biomass_df, si_df1_dt, output_si_asof_dir, sub_dir_dir, "si")

                si_concat = pd.concat(si_list)

                print("=" * 50)
                print(si_concat.columns)

                if sub_dir == "daily_rain":
                    col_str = "drsi"

                elif sub_dir == 'et_morton_actual':
                    col_str = "emsi"

                elif sub_dir == 'max_temp':
                    col_str = "matsi"

                elif sub_dir == 'min_temp':
                    col_str = "mitsi"

                elif sub_dir == 'rh_tmax':
                    col_str = "marsi"

                elif sub_dir == 'rh_tmin':
                    col_str = "mirsi"

                else:
                    print(sub_dir_dir)
                    import sys
                    sys.exit()

                si_concat.rename(columns={'tot_annual_avg': f'tot_an_{col_str}_avg',
                                          'tot_annual_avg_R': f'tot_an_{col_str}_avg_R',
                                          'tot_annual_avg_si': f'tot_an_{col_str}_avg_si',
                                          'var_dt': f'{col_str}_dt'}, inplace=True)

                si_asof_list.append(si_concat)
                export_fn(output_si_concat_asof_dir, sub_dir_dir, "si_merge_asof_concat", si_concat)
                si_concat.dropna(subset=[f'{col_str}_dt'], inplace=True)
                export_fn(output_si_concat_asof_dir, sub_dir_dir, "si_merge_asof_concat_dropna", si_concat)

                pass
            else:
                pass

    print(si_asof_list)
    print(len(si_asof_list))
    print(seasonal_asof_list)
    print(len(seasonal_asof_list))

    # --------------------------------------- Merge si_list ----------------------------------------

    print(si_list[0].columns)

    si_list01 = pd.merge(right=si_asof_list[0], left=si_asof_list[1], how="outer",
                         on=['uid', 'site_name', 'site', 'year', 'date', 'lon_gda94', 'lat_gda94', 'geometry',
                             'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                             'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha',
                             'bio_r_kg1ha',
                             'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
                             'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha',
                             'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'])

    si_list01.to_csv(r"C:\Users\robot\projects\biomasss\collated_zonal_stats\met\si_list01.csv",
                     index=False)

    #
    # si_list012 = pd.merge(right=si_list01, left=si_asof_list[2], how="outer",
    #                                              on=['uid', 'site_name', 'site','year', 'date', 'lon_gda94', 'lat_gda94', 'geometry',
    #                                                  'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
    #                                                  'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha',
    #                                                  'bio_r_kg1ha',
    #                                                  'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
    #                                                  'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha',
    #                                                  'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'])
    #
    # si_list012.to_csv(r"C:\Users\robot\projects\biomasss\collated_zonal_stats\met\si_list012.csv",
    #                                          index=False)
    #
    # si_list0123 = pd.merge(right=si_list012, left=si_asof_list[3], how="outer",
    #                                              on=['uid', 'site_name', 'site','year', 'date', 'lon_gda94', 'lat_gda94', 'geometry',
    #                                                  'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
    #                                                  'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha',
    #                                                  'bio_r_kg1ha',
    #                                                  'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
    #                                                  'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha',
    #                                                  'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'])
    #
    # si_list0123.to_csv(r"C:\Users\robot\projects\biomasss\collated_zonal_stats\met\si_list0123.csv",
    #                                          index=False)
    #
    # si_list01234 = pd.merge(right=si_list0123, left=si_asof_list[4], how="outer",
    #                                              on=['uid', 'site_name', 'site','year', 'date', 'lon_gda94', 'lat_gda94', 'geometry',
    #                                                  'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
    #                                                  'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha',
    #                                                  'bio_r_kg1ha',
    #                                                  'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
    #                                                  'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha',
    #                                                  'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'])
    #
    # si_list01234.to_csv(r"C:\Users\robot\projects\biomasss\collated_zonal_stats\met\si_list01234.csv",
    #                                          index=False)

    # ------------------------------------ Merge Seasonal -----------------------------------------
    print(si_list[0].columns)

    seasonal_asof_list01 = pd.merge(right=seasonal_asof_list[0], left=seasonal_asof_list[1], how="outer",
                                    on=['uid', 'site_name', 'site', 'year', 'date', 'lon_gda94', 'lat_gda94',
                                        'geometry',
                                        'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                                        'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha',
                                        'bio_r_kg1ha',
                                        'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
                                        'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha',
                                        'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'])

    seasonal_asof_list01.to_csv(r"C:\Users\robot\projects\biomasss\collated_zonal_stats\met\seasonal_asof_list01.csv",
                                index=False)
    print(si_list[0].columns)

    # seasonal_asof_list012 = pd.merge(right=seasonal_asof_list01, left=seasonal_asof_list[2], how="outer",
    #                                              on=['uid', 'site_name', 'site', 'year', 'date', 'lon_gda94', 'lat_gda94', 'geometry',
    #                                                  'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
    #                                                  'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha',
    #                                                  'bio_r_kg1ha',
    #                                                  'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
    #                                                  'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha',
    #                                                  'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'])
    #
    # seasonal_asof_list012.to_csv(r"C:\Users\robot\projects\biomasss\collated_zonal_stats\met\seasonal_asof_list012.csv",
    #                                          index=False)

    # seasonal_asof_list0123 = pd.merge(right=seasonal_asof_list012, left=seasonal_asof_list[3],
    #                                              on=['uid', 'site_name', 'site_clean', 'site', 'year', 'date', 'lon_gda94', 'lat_gda94', 'geometry',
    #                                                  'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
    #                                                  'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha',
    #                                                  'bio_r_kg1ha',
    #                                                  'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
    #                                                  'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha',
    #                                                  'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'])
    #
    # seasonal_asof_list0123.to_csv(r"C:\Users\robot\projects\biomasss\collated_zonal_stats\met\seasonal_asof_list0123.csv",
    #                                          index=False)

    # seasonal_asof_list01234 = pd.merge(right=seasonal_asof_list0123, left=seasonal_asof_list[4], how="outer",
    #                                              on=['uid', 'site_name', 'site', 'year', 'date', 'lon_gda94', 'lat_gda94', 'geometry',
    #                                                  'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
    #                                                  'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha',
    #                                                  'bio_r_kg1ha',
    #                                                  'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
    #                                                  'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha',
    #                                                  'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'])
    #
    # seasonal_asof_list01234.to_csv(r"C:\Users\robot\projects\biomasss\collated_zonal_stats\met\seasonal_asof_list01234.csv",
    #                                          index=False)

    # ==================================================================================================================

    seasonal_si = pd.merge(right=seasonal_asof_list01, left=si_list01, how="outer",
                           on=['uid', 'site_name', 'site', 'year', 'date', 'lon_gda94', 'lat_gda94', 'geometry',
                               'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                               'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha',
                               'bio_r_kg1ha',
                               'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
                               'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha',
                               'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'])

    print(list(seasonal_si.columns))
    seasonal_si.to_csv(r"C:\Users\robot\projects\biomasss\collated_zonal_stats\met\seasonal_si.csv",
                       index=False)

    # import sys
    # sys.exit()

    # todo update when using full directory path

    seasonal_si_clean = seasonal_si[
        ['uid', 'site_clean_x_x', 'date', 'lon_gda94', 'lat_gda94', 'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
         'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha', 'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
         'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha', 'c_r_kg1ha', 'c_agb_kg1ha', 'geometry', 'basal_dt',
         'year', 'tot_an_emsi_avg', 'tot_an_emsi_avg_R', 'tot_an_emsi_avg_si',
         'emsi_dt', 'tot_an_drsi_avg', 'tot_an_drsi_avg_R',
         'tot_an_drsi_avg_si', 'drsi_dt', 'tot_avg_emse_djf',
         'tot_avg_emse_mam', 'tot_avg_emse_jja', 'tot_avg_emse_son', 'tot_avg_emse_dry', 'tot_avg_emse_wet',
         'tot_avg_emse_annual', 'emse_dt', 'tot_avg_drse_djf',
         'tot_avg_drse_mam', 'tot_avg_drse_jja', 'tot_avg_drse_son', 'tot_avg_drse_dry', 'tot_avg_drse_wet',
         'tot_avg_drse_annual', 'drse_dt']]

    seasonal_si_clean.rename(columns={'site_clean_x_x': 'site_clean'}, inplace=True)

    # ==================================================================================================================

    # ---------------------------------------------------- n_df --------------------------------------------------------

    # --------------------------------------------- annual merge -------------------------------------------------------
    dp0_dbg_si_single_annual_density_near_met_si = pd.merge(right=dp0_dbg_si_single_annual_density_near_met, left=seasonal_si_clean, how="outer",
                                                         on=['uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94',
                                                             'geometry',
                                                             'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                                                             'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha',
                                                             'bio_r_kg1ha',
                                                             'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
                                                             'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha',
                                                             'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'])

    dp0_dbg_si_single_annual_density_near_met_si.to_csv(
        r"C:\Users\robot\projects\biomasss\collated_zonal_stats\single\dp0_dbg_si_single_annual_density_near_met_si.csv",
        index=False)

    print("dp0_dbg_si_mask_single_annual_density_near_met: ", dp0_dbg_si_mask_single_annual_density_near_met.columns,
          dp0_dbg_si_mask_single_annual_density_near_met.shape)

    print("seasonal_si_clean: ", seasonal_si_clean.columns, seasonal_si_clean.shape)

    print("+"*100)

    dp0_dbg_si_mask_single_annual_density_near_met_si = pd.merge(right=dp0_dbg_si_mask_single_annual_density_near_met, left=seasonal_si_clean,
                                                              how="outer",
                                                              on=['uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94',
                                                                  'geometry',
                                                                  'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                                                                  'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha',
                                                                  'bio_r_kg1ha',
                                                                  'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
                                                                  'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha',
                                                                  'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'])

    dp0_dbg_si_mask_single_annual_density_near_met_si.to_csv(
        r"C:\Users\robot\projects\biomasss\collated_zonal_stats\single_mask\dp0_dbg_si_mask_single_annual_density_near_met_si.csv",
        index=False)

    # ------------------------------------------------------ dry merge -------------------------------------------------

    dp0_dbg_si_single_dry_density_near_met_si = pd.merge(right=dp0_dbg_si_single_dry_density_near_met, left=seasonal_si_clean, how="outer",
                                                      on=['uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94',
                                                          'geometry',
                                                          'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                                                          'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha',
                                                          'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
                                                          'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha',
                                                          'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'])

    dp0_dbg_si_single_dry_density_near_met_si.to_csv(
        r"C:\Users\robot\projects\biomasss\collated_zonal_stats\single\dp0_dbg_si_single_dry_density_near_met_si.csv",
        index=False)

    dp0_dbg_si_mask_single_dry_density_near_met_si = pd.merge(right=dp0_dbg_si_mask_single_dry_density_near_met, left=seasonal_si_clean,
                                                           how="outer",
                                                           on=['uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94',
                                                               'geometry',
                                                               'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                                                               'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha',
                                                               'bio_r_kg1ha',
                                                               'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
                                                               'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha',
                                                               'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'])

    dp0_dbg_si_mask_single_dry_density_near_met_si.to_csv(
        r"C:\Users\robot\projects\biomasss\collated_zonal_stats\single_mask\dp0_dbg_si_mask_single_dry_density_near_met_si.csv",
        index=False)

    # ------------------------------------------------------ annual merge dp1 bbi --------------------------------------

    dp1_dbi_si_annual_density_near_met_si = pd.merge(right=dp1_dbi_si_annual_density_near_met, left=seasonal_si_clean, how="outer",
                                                  on=['uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94', 'geometry',
                                                      'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                                                      'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha',
                                                      'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
                                                      'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha',
                                                      'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'])

    dp1_dbi_si_annual_density_near_met_si.to_csv(
        r"C:\Users\robot\projects\biomasss\collated_zonal_stats\annual\dp1_dbi_si_annual_density_near_met_si.csv",
        index=False)

    dp1_dbi_si_annual_mask_density_near_met_si = pd.merge(right=dp1_dbi_si_annual_mask_density_near_met, left=seasonal_si_clean, how="outer",
                                                       on=['uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94',
                                                           'geometry',
                                                           'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                                                           'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha',
                                                           'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
                                                           'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha',
                                                           'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'])

    dp1_dbi_si_annual_mask_density_near_met_si.to_csv(
        r"C:\Users\robot\projects\biomasss\collated_zonal_stats\annual_mask\dp1_dbi_si_annual_mask_density_near_met_si.csv",
        index=False)

    # ---------------------------------------------- dry merge dp1 bbi -------------------------------------------------

    dp1_dbi_si_dry_density_near_met_si = pd.merge(right=dp1_dbi_si_dry_density_near_met, left=seasonal_si_clean, how="outer",
                                               on=['uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94', 'geometry',
                                                   'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                                                   'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha',
                                                   'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
                                                   'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha',
                                                   'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'])

    dp1_dbi_si_dry_density_near_met_si.to_csv(
        r"C:\Users\robot\projects\biomasss\collated_zonal_stats\dry\dp1_dbi_si_dry_density_near_df_near_met_si.csv",
        index=False)

    dp1_dbi_si_dry_mask_density_near_met_si = pd.merge(right=dp1_dbi_si_dry_mask_density_near_met, left=seasonal_si_clean, how="outer",
                                                    on=['uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94',
                                                        'geometry',
                                                        'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                                                        'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha',
                                                        'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
                                                        'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha',
                                                        'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'])

    dp1_dbi_si_dry_mask_density_near_met_si.to_csv(
        r"C:\Users\robot\projects\biomasss\collated_zonal_stats\dry_mask\dp1_dbi_si_dry_mask_density_near_met_si.csv",
        index=False)

    return seasonal_si_clean, dp0_dbg_si_single_annual_density_near_met_si, \
        dp0_dbg_si_mask_single_annual_density_near_met_si, \
        dp0_dbg_si_single_dry_density_near_met_si, \
        dp0_dbg_si_mask_single_dry_density_near_met_si, \
        dp1_dbi_si_annual_density_near_met_si, \
        dp1_dbi_si_annual_mask_density_near_met_si, \
        dp1_dbi_si_dry_density_near_met_si, \
        dp1_dbi_si_dry_mask_density_near_met_si


if __name__ == '__main__':
    main_routine()
