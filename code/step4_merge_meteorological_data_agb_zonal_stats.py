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
    print("met output: ", out_file)


def glob_fn(temp_dir):
    csv_list = []
    for f in glob(os.path.join(temp_dir, "*.csv")):
        df__ = pd.read_csv(f)
        csv_list.append(df__)
    final_df = pd.concat(csv_list, axis=1)

    return final_df


def export_fn(output_dir, pos, dff):
    print("output export dir: ", dff.columns)
    out = os.path.join(output_dir, "{0}_met_zonal_stats.csv".format(pos))
    dff.to_csv((out), index=False)
    print("output to: ", out)


def drop_cols_fn(df):
    """ This function restructures the output dataframe to include the uid, site name, biomass and all meterological
    mean values.

    :param df: dataframe object
    :return df_out: cleaned dataframe object
    """
    df1 = df.copy()
    df_columns = df.columns.tolist()
    print("df_columns: ", df_columns)
    print("-" * 100)

    drop_list = [columns for columns in df_columns if "mean" not in columns]
    print("drop column list: ", drop_list)

    df.drop(drop_list, axis=1, inplace=True)
    print("cleaned df shape:", df.shape)

    var_ = df1.iloc[:, [24, 13]]
    uid_var = df1.iloc[:, 0]
    date_var = df1.iloc[:, 3]
    # rain_image_var = df1.iloc[:, 39]
    # et_image_var = df1.iloc[:, 80]
    # et_image_var = df1.iloc[:, 39]
    # et_image_var = df1.iloc[:, 39]
    # et_image_var = df1.iloc[:, 39]

    var_columns = var_.columns.tolist()
    print("var_columns: ", var_columns)
    df_out = pd.concat([var_, df], axis=1)
    print("cleaned df shape:", df_out.columns.tolist())

    #uid_list = df1["uid"].tolist()
    # Insert the uid to beginning od df
    df_out.insert(0, 'uid', uid_var)
    df_out.insert(2, 'date', date_var)
    # df_out.insert(3, 'rain_im', rain_image_var)
    # df_out.insert(4, 'et_im', et_image_var)
    print("cleaned df shape:", df_out.columns.tolist())

    # import sys
    # sys.exit()
    print("df_out columns: ", df_out)

    #todo add image name here
    return df_out


def main_routine(biomass_csv, dir_, output_dir, dp0_dbg_si_single_annual_density,
                 dp0_dbg_si_mask_single_annual_density,
                 dp0_dbg_si_single_dry_density,
                 dp0_dbg_si_mask_single_dry_density,
                 dp1_dbi_si_annual_density,
                 dp1_dbi_si_annual_mask_density,
                 dp1_dbi_si_dry_density,
                 dp1_dbi_si_dry_mask_density):
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

    # extract a list of subdirectories from the qld meterological zonal stats
    sub_list = next(os.walk(dir_))[1]
    print("sub_list: ", sub_list)

    # working_sub_list = sub_list[19:20]
    zonal_list = []
    sub_dir_list = []
    for sub_dir in sub_list:
        file_list = []
        print("Working on: ", sub_dir)
        sub_dir_path = os.path.join(dir_, sub_dir)
        sub_dir_df_list = []
        for file_ in glob(os.path.join(sub_dir_path, "*.csv")):
            if "zonal_stats" in file_:
                sub_dir_list.append(file_)
                print("file: ", file_)

                df = pd.read_csv(file_)
                df = convert_to_datetime(df, "im_date", "image_dt")
                df.sort_values(by='image_dt', inplace=True)
                # print(df.columns)
                sub_dir_df_list.append(df)
        df1 = pd.concat(sub_dir_df_list)
        print("met: df1: ", df1.columns)
        # sort values
        df1.sort_values(by="im_date", inplace=True)

        # drop null values on minimum column
        df_columns = df1.columns.tolist()
        min_column = [columns for columns in df_columns if "min" in columns]
        print("Min column: ", min_column)
        df1.dropna(subset=min_column, inplace=True)

        # correct df site name
        site_list = []
        site_name = df1["site"].tolist()
        for i in site_name:
            print("i: ", i)
            n = i.replace("_1ha", "")
            # print(n)
            site_list.append(n)

        df1["site_clean"] = site_list

        # merge data with basal dataset based on the nearest date to the field data colection
        n_df = pd.merge_asof(biomass_df, df1, left_on="basal_dt", right_on="image_dt", by="site_clean",
                             direction="nearest")
        f_df = pd.merge_asof(biomass_df, df1, left_on="basal_dt", right_on="image_dt", by="site_clean",
                             direction="forward")
        b_df = pd.merge_asof(biomass_df, df1, left_on="basal_dt", right_on="image_dt", by="site_clean",
                             direction="backward")

        # create temp dirs
        n_temp_dir = temp_dir_fn(output_dir, "near")
        f_temp_dir = temp_dir_fn(output_dir, "for")
        b_temp_dir = temp_dir_fn(output_dir, "back")

        # export csv
        out_file_fn(n_temp_dir, "near", sub_dir, n_df)
        out_file_fn(f_temp_dir, "for", sub_dir, f_df)
        out_file_fn(b_temp_dir, "back", sub_dir, b_df)

    n_final_df = glob_fn(n_temp_dir)
    export_fn(output_dir, "near", n_final_df)
    n_df1 = drop_cols_fn(n_final_df)
    export_fn(output_dir, "cleaned_near", n_df1)

    f_final_df = glob_fn(f_temp_dir)
    export_fn(output_dir, "for", f_final_df)
    f_df1 = drop_cols_fn(f_final_df)
    export_fn(output_dir, "cleaned_for", f_df1)

    b_final_df = glob_fn(b_temp_dir)
    export_fn(output_dir, "back", b_final_df)
    b_df1 = drop_cols_fn(b_final_df)
    export_fn(output_dir, "cleaned_back", b_df1)

    print(list(n_df1.columns))

    out = os.path.join(r"C:\Users\robot\projects\biomasss\collated_zonal_stats\met", "near_met.csv")

    n_df1.to_csv(out, index=False)

    out = os.path.join(r"C:\Users\robot\projects\biomasss\collated_zonal_stats\met", "dp0_dbg_si_single_annual_density.csv")
    dp0_dbg_si_single_annual_density.to_csv(out, index=False)

    print("-"*100)
    print("-"*100)

    print("n_df1: ", list(n_df1.columns))
    print("dp0_dbg_si_single_annual_density: ", list(dp0_dbg_si_single_annual_density.columns))

    # ==================================================================================================================

    # ---------------------------------------------------- n_df --------------------------------------------------------

    # --------------------------------------------- annual merge -------------------------------------------------------
    dp0_dbg_si_single_annual_density_near_met = pd.merge(right=dp0_dbg_si_single_annual_density, left=n_df1, how="outer",
                                                         on=['uid', 'site_clean', 'date', 'bio_agb_kg1ha',
                                                             # 'lon_gda94', 'lat_gda94',
                                                             # 'geometry',
                                                             # 'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                                                             # 'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha',
                                                             # 'bio_r_kg1ha',
                                                             # 'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
                                                             # 'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha',
                                                             # 'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'
                                                             ])

    dp0_dbg_si_single_annual_density_near_met.to_csv(
        r"C:\Users\robot\projects\biomasss\collated_zonal_stats\single\dp0_dbg_si_single_annual_density_near_met.csv",
        index=False)


    dp0_dbg_si_mask_single_annual_density_near_met = pd.merge(right=dp0_dbg_si_mask_single_annual_density, left=n_df1,
                                                              how="outer",
                                                              on=['uid', 'site_clean', 'date', 'bio_agb_kg1ha',
                                                                  # 'lon_gda94', 'lat_gda94',
                                                                  # 'geometry',
                                                                  # 'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                                                                  # 'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha',
                                                                  # 'bio_r_kg1ha',
                                                                  # 'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
                                                                  # 'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha',
                                                                  # 'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'
                                                                  ])

    dp0_dbg_si_mask_single_annual_density_near_met.to_csv(
        r"C:\Users\robot\projects\biomasss\collated_zonal_stats\single_mask\dp0_dbg_si_mask_single_annual_density_near_met.csv",
        index=False)

    # ------------------------------------------------------ dry merge -------------------------------------------------

    dp0_dbg_si_single_dry_density_near_met = pd.merge(right=dp0_dbg_si_single_dry_density, left=n_df1, how="outer",
                                                      on=['uid', 'site_clean', 'date', 'bio_agb_kg1ha',
                                                          # 'lon_gda94', 'lat_gda94',
                                                          # 'geometry',
                                                          # 'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                                                          # 'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha',
                                                          # 'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
                                                          # 'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha',
                                                          # 'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'
                                                          ])

    dp0_dbg_si_single_dry_density_near_met.to_csv(
        r"C:\Users\robot\projects\biomasss\collated_zonal_stats\single\dp0_dbg_si_single_dry_density_near_met.csv",
        index=False)

    dp0_dbg_si_mask_single_dry_density_near_met = pd.merge(right=dp0_dbg_si_mask_single_dry_density, left=n_df1,
                                                           how="outer",
                                                           on=['uid', 'site_clean', 'date', 'bio_agb_kg1ha',
                                                               # 'lon_gda94', 'lat_gda94',
                                                               # 'geometry',
                                                               # 'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                                                               # 'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha',
                                                               # 'bio_r_kg1ha',
                                                               # 'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
                                                               # 'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha',
                                                               # 'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'
                                                               ])

    dp0_dbg_si_mask_single_dry_density_near_met.to_csv(
        r"C:\Users\robot\projects\biomasss\collated_zonal_stats\single_mask\dp0_dbg_si_mask_single_dry_density_near_met.csv",
        index=False)

    # ------------------------------------------------------ annual merge dp1 bbi --------------------------------------

    dp1_dbi_si_annual_density_near_met = pd.merge(right=dp1_dbi_si_annual_density, left=n_df1, how="outer",
                                                  on=['uid', 'site_clean', 'date', 'bio_agb_kg1ha',
                                                      # 'lon_gda94', 'lat_gda94', 'geometry',
                                                      # 'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                                                      # 'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha',
                                                      # 'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
                                                      # 'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha',
                                                      # 'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'
                                                      ])

    dp1_dbi_si_annual_density_near_met.to_csv(
        r"C:\Users\robot\projects\biomasss\collated_zonal_stats\annual\dp1_dbi_si_annual_density_near_met.csv",
        index=False)

    dp1_dbi_si_annual_mask_density_near_met = pd.merge(right=dp1_dbi_si_annual_mask_density, left=n_df1, how="outer",
                                                       on=['uid', 'site_clean', 'date', 'bio_agb_kg1ha',
                                                           # 'lon_gda94', 'lat_gda94',
                                                           # 'geometry',
                                                           # 'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                                                           # 'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha',
                                                           # 'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
                                                           # 'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha',
                                                           # 'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'
                                                           ])

    dp1_dbi_si_annual_mask_density_near_met.to_csv(
        r"C:\Users\robot\projects\biomasss\collated_zonal_stats\annual_mask\dp1_dbi_si_annual_mask_density_near_met.csv",
        index=False)

    # ---------------------------------------------- dry merge dp1 bbi -------------------------------------------------

    dp1_dbi_si_dry_density_near_met = pd.merge(right=dp1_dbi_si_dry_density, left=n_df1, how="outer",
                                               on=['uid', 'site_clean', 'date', 'bio_agb_kg1ha',
                                                   # 'lon_gda94', 'lat_gda94', 'geometry',
                                                   # 'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                                                   # 'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha',
                                                   # 'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
                                                   # 'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha',
                                                   # 'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'
                                                   ])

    dp1_dbi_si_dry_density_near_met.to_csv(
        r"C:\Users\robot\projects\biomasss\collated_zonal_stats\dry\dp1_dbi_si_dry_density_near_df_near_met.csv",
        index=False)


    dp1_dbi_si_dry_mask_density_near_met = pd.merge(right=dp1_dbi_si_dry_mask_density, left=n_df1, how="outer",
                                                    on=['uid', 'site_clean', 'date', 'bio_agb_kg1ha',
                                                        # 'lon_gda94', 'lat_gda94',
                                                        # 'geometry',
                                                        # 'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                                                        # 'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha',
                                                        # 'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
                                                        # 'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha',
                                                        # 'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'
                                                        ])

    dp1_dbi_si_dry_mask_density_near_met.to_csv(
        r"C:\Users\robot\projects\biomasss\collated_zonal_stats\dry_mask\dp1_dbi_si_dry_mask_density_near_met.csv",
        index=False)

    return n_df1, dp0_dbg_si_single_annual_density_near_met, \
        dp0_dbg_si_mask_single_annual_density_near_met, \
        dp0_dbg_si_single_dry_density_near_met, \
        dp0_dbg_si_mask_single_dry_density_near_met, \
        dp1_dbi_si_annual_density_near_met, \
        dp1_dbi_si_annual_mask_density_near_met, \
        dp1_dbi_si_dry_density_near_met, \
        dp1_dbi_si_dry_mask_density_near_met,


if __name__ == '__main__':
    main_routine()
