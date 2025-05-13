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
    #print("met output: ", out_file)


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

    return merged_df


def merge_df_list_fn(df_list):


    features = ['uid', 'site_clean', 'site_name', 'date', 'lon_gda94', 'lat_gda94', 'bio_l_kg1ha', 'bio_t_kg1ha',
                'bio_b_kg1ha', 'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha', 'bio_agb_kg1ha',
                'c_l_kg1ha', 'c_t_kg1ha', 'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha', 'c_r_kg1ha',
                'c_agb_kg1ha', 'geometry', 'basal_dt']
    # Use reduce to merge all DataFrames
    from functools import reduce
    merged_df = reduce(lambda left, right: pd.merge(left, right, on=features), df_list)

    return merged_df

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

    df.rename(columns={"site_x_x": "site"}, inplace=True)
    import sys
    sys.exit()

    # List of feature names to drop
    # features_to_drop =  ['ident_x', 'site_y_x', 'im_date_x',  'im_name_x',
    #                      'd_type_x', 'image_dt_x',  'site_x_y',
    #                      'ident_y', 'site_y_y', 'im_date_y',  'im_name_y', 'd_type_y',
    #                      'image_dt_y', 'site_x_x', 'ident_x', 'site_y_x', 'im_date_x',
    #                       'im_name_x', 'd_type_x', 'image_dt_x',  'dr_asmd_im_y', 'dr_ann_mavg_x', 'dr_aavg_dt_x', 'dr_aavg_im_x', 'site_x_y', 'ident_y', 'site_y_y', 'im_date_y', 'dr_ann_asa_ssav_y', 'im_name_y', 'd_type_y', 'image_dt_y', 'dr_asav_dt_y', 'dr_asav_im_y', 'dr_ann_asm_ssmd_x', 'dr_asmd_dt_x', 'dr_asmd_im_x', 'dr_ann_mavg_y', 'dr_aavg_dt_y', 'dr_aavg_im_y', 'dr_ann_mmed_x', 'dr_amed_dt_x', 'dr_amed_im_x', 'site_x_x', 'ident_x', 'site_y_x', 'im_date_x', 'dr_ann_asa_ssav_x', 'im_name_x', 'd_type_x', 'image_dt_x', 'dr_asav_dt_x', 'dr_asav_im_x', 'dr_ann_asm_ssmd_y', 'dr_asmd_dt_y', 'dr_asmd_im_y', 'dr_ann_mavg_x', 'dr_aavg_dt_x', 'dr_aavg_im_x', 'dr_ann_mmed_y', 'dr_amed_dt_y', 'dr_amed_im_y', 'dr_ann_msin_siav_x', 'dr_asiv_dt_x', 'dr_asiv_im_x', 'site_x_y', 'ident_y', 'site_y_y', 'im_date_y', 'dr_ann_asa_ssav_y', 'im_name_y', 'd_type_y', 'image_dt_y', 'dr_asav_dt_y', 'dr_asav_im_y', 'dr_ann_asm_ssmd_x', 'dr_asmd_dt_x', 'dr_asmd_im_x', 'dr_ann_mavg_y', 'dr_aavg_dt_y', 'dr_aavg_im_y', 'dr_ann_mmed_x', 'dr_amed_dt_x', 'dr_amed_im_x', 'dr_ann_msin_siav_y', 'dr_asiv_dt_y', 'dr_asiv_im_y', 'dr_ann_msin_simd_x', 'dr_asid_dt_x', 'dr_asid_im_x', 'site_x_x', 'ident_x', 'site_y_x', 'im_date_x', 'dr_ann_asa_ssav_x', 'im_name_x', 'd_type_x', 'image_dt_x', 'dr_asav_dt_x', 'dr_asav_im_x', 'dr_ann_asm_ssmd_y', 'dr_asmd_dt_y', 'dr_asmd_im_y', 'dr_ann_mavg_x', 'dr_aavg_dt_x', 'dr_aavg_im_x', 'dr_ann_mmed_y', 'dr_amed_dt_y', 'dr_amed_im_y', 'dr_ann_msin_siav_x', 'dr_asiv_dt_x', 'dr_asiv_im_x', 'dr_ann_msin_simd_y', 'dr_asid_dt_y', 'dr_asid_im_y', 'dr_ann_msum_x', 'dr_asum_dt_x', 'dr_asum_im_x', 'site_x_y', 'ident_y', 'site_y_y', 'im_date_y', 'dr_ann_asa_ssav_y', 'im_name_y', 'd_type_y', 'image_dt_y', 'dr_asav_dt_y', 'dr_asav_im_y', 'dr_ann_asm_ssmd_x', 'dr_asmd_dt_x', 'dr_asmd_im_x', 'dr_ann_mavg_y', 'dr_aavg_dt_y', 'dr_aavg_im_y', 'dr_ann_mmed_x', 'dr_amed_dt_x', 'dr_amed_im_x', 'dr_ann_msin_siav_y', 'dr_asiv_dt_y', 'dr_asiv_im_y', 'dr_ann_msin_simd_x', 'dr_asid_dt_x', 'dr_asid_im_x', 'dr_ann_msum_y', 'dr_asum_dt_y', 'dr_asum_im_y', 'dr_dry_asa_ssav_x', 'dr_dsav_dt_x', 'dr_dsav_im_x', 'site_x_x', 'ident_x', 'site_y_x', 'im_date_x', 'dr_ann_asa_ssav_x', 'im_name_x', 'd_type_x', 'image_dt_x', 'dr_asav_dt_x', 'dr_asav_im_x', 'dr_ann_asm_ssmd_y', 'dr_asmd_dt_y', 'dr_asmd_im_y', 'dr_ann_mavg_x', 'dr_aavg_dt_x', 'dr_aavg_im_x', 'dr_ann_mmed_y', 'dr_amed_dt_y', 'dr_amed_im_y', 'dr_ann_msin_siav_x', 'dr_asiv_dt_x', 'dr_asiv_im_x', 'dr_ann_msin_simd_y', 'dr_asid_dt_y', 'dr_asid_im_y', 'dr_ann_msum_x', 'dr_asum_dt_x', 'dr_asum_im_x', 'dr_dry_asa_ssav_y', 'dr_dsav_dt_y', 'dr_dsav_im_y', 'dr_dry_asm_ssmd_x', 'dr_dsmd_dt_x', 'dr_dsmd_im_x', 'site_x_y', 'ident_y', 'site_y_y', 'im_date_y', 'dr_ann_asa_ssav_y', 'im_name_y', 'd_type_y', 'image_dt_y', 'dr_asav_dt_y', 'dr_asav_im_y', 'dr_ann_asm_ssmd_x', 'dr_asmd_dt_x', 'dr_asmd_im_x', 'dr_ann_mavg_y', 'dr_aavg_dt_y', 'dr_aavg_im_y', 'dr_ann_mmed_x', 'dr_amed_dt_x', 'dr_amed_im_x', 'dr_ann_msin_siav_y', 'dr_asiv_dt_y', 'dr_asiv_im_y', 'dr_ann_msin_simd_x', 'dr_asid_dt_x', 'dr_asid_im_x', 'dr_ann_msum_y', 'dr_asum_dt_y', 'dr_asum_im_y', 'dr_dry_asa_ssav_x', 'dr_dsav_dt_x', 'dr_dsav_im_x', 'dr_dry_asm_ssmd_y', 'dr_dsmd_dt_y', 'dr_dsmd_im_y', 'dr_dry_mavg_x', 'dr_davg_dt_x', 'dr_davg_im_x', 'site_x_x', 'ident_x', 'site_y_x', 'im_date_x', 'dr_ann_asa_ssav_x', 'im_name_x', 'd_type_x', 'image_dt_x', 'dr_asav_dt_x', 'dr_asav_im_x', 'dr_ann_asm_ssmd_y', 'dr_asmd_dt_y', 'dr_asmd_im_y', 'dr_ann_mavg_x', 'dr_aavg_dt_x', 'dr_aavg_im_x', 'dr_ann_mmed_y', 'dr_amed_dt_y', 'dr_amed_im_y', 'dr_ann_msin_siav_x', 'dr_asiv_dt_x', 'dr_asiv_im_x', 'dr_ann_msin_simd_y', 'dr_asid_dt_y', 'dr_asid_im_y', 'dr_ann_msum_x', 'dr_asum_dt_x', 'dr_asum_im_x', 'dr_dry_asa_ssav_y', 'dr_dsav_dt_y', 'dr_dsav_im_y', 'dr_dry_asm_ssmd_x', 'dr_dsmd_dt_x', 'dr_dsmd_im_x', 'dr_dry_mavg_y', 'dr_davg_dt_y', 'dr_davg_im_y', 'dr_dry_mmed_x', 'dr_dmed_dt_x', 'dr_dmed_im_x', 'site_x_y', 'ident_y', 'site_y_y', 'im_date_y', 'dr_ann_asa_ssav_y', 'im_name_y', 'd_type_y', 'image_dt_y', 'dr_asav_dt_y', 'dr_asav_im_y', 'dr_ann_asm_ssmd_x', 'dr_asmd_dt_x', 'dr_asmd_im_x', 'dr_ann_mavg_y', 'dr_aavg_dt_y', 'dr_aavg_im_y', 'dr_ann_mmed_x', 'dr_amed_dt_x', 'dr_amed_im_x', 'dr_ann_msin_siav_y', 'dr_asiv_dt_y', 'dr_asiv_im_y', 'dr_ann_msin_simd_x', 'dr_asid_dt_x', 'dr_asid_im_x', 'dr_ann_msum_y', 'dr_asum_dt_y', 'dr_asum_im_y', 'dr_dry_asa_ssav_x', 'dr_dsav_dt_x', 'dr_dsav_im_x', 'dr_dry_asm_ssmd_y', 'dr_dsmd_dt_y', 'dr_dsmd_im_y', 'dr_dry_mavg_x', 'dr_davg_dt_x', 'dr_davg_im_x', 'dr_dry_mmed_y', 'dr_dmed_dt_y', 'dr_dmed_im_y', 'dr_dry_msin_siav_x', 'dr_dsiv_dt_x', 'dr_dsiv_im_x', 'site_x_x', 'ident_x', 'site_y_x', 'im_date_x', 'dr_ann_asa_ssav_x', 'im_name_x', 'd_type_x', 'image_dt_x', 'dr_asav_dt_x', 'dr_asav_im_x', 'dr_ann_asm_ssmd_y', 'dr_asmd_dt_y', 'dr_asmd_im_y', 'dr_ann_mavg_x', 'dr_aavg_dt_x', 'dr_aavg_im_x', 'dr_ann_mmed_y', 'dr_amed_dt_y', 'dr_amed_im_y', 'dr_ann_msin_siav_x', 'dr_asiv_dt_x', 'dr_asiv_im_x', 'dr_ann_msin_simd_y', 'dr_asid_dt_y', 'dr_asid_im_y', 'dr_ann_msum_x', 'dr_asum_dt_x', 'dr_asum_im_x', 'dr_dry_asa_ssav_y', 'dr_dsav_dt_y', 'dr_dsav_im_y', 'dr_dry_asm_ssmd_x', 'dr_dsmd_dt_x', 'dr_dsmd_im_x', 'dr_dry_mavg_y', 'dr_davg_dt_y', 'dr_davg_im_y', 'dr_dry_mmed_x', 'dr_dmed_dt_x', 'dr_dmed_im_x', 'dr_dry_msin_siav_y', 'dr_dsiv_dt_y', 'dr_dsiv_im_y', 'dr_dry_msin_simd_x', 'dr_dsid_dt_x', 'dr_dsid_im_x', 'site_x_y', 'ident_y', 'site_y_y', 'im_date_y', 'dr_ann_asa_ssav_y', 'im_name_y', 'd_type_y', 'image_dt_y', 'dr_asav_dt_y', 'dr_asav_im_y', 'dr_ann_asm_ssmd_x', 'dr_asmd_dt_x', 'dr_asmd_im_x', 'dr_ann_mavg_y', 'dr_aavg_dt_y', 'dr_aavg_im_y', 'dr_ann_mmed_x', 'dr_amed_dt_x', 'dr_amed_im_x', 'dr_ann_msin_siav_y', 'dr_asiv_dt_y', 'dr_asiv_im_y', 'dr_ann_msin_simd_x', 'dr_asid_dt_x', 'dr_asid_im_x', 'dr_ann_msum_y', 'dr_asum_dt_y', 'dr_asum_im_y', 'dr_dry_asa_ssav_x', 'dr_dsav_dt_x', 'dr_dsav_im_x', 'dr_dry_asm_ssmd_y', 'dr_dsmd_dt_y', 'dr_dsmd_im_y', 'dr_dry_mavg_x', 'dr_davg_dt_x', 'dr_davg_im_x', 'dr_dry_mmed_y', 'dr_dmed_dt_y', 'dr_dmed_im_y', 'dr_dry_msin_siav_x', 'dr_dsiv_dt_x', 'dr_dsiv_im_x', 'dr_dry_msin_simd_y', 'dr_dsid_dt_y', 'dr_dsid_im_y', 'dr_dry_msum_x', 'dr_dsum_dt_x', 'dr_dsum_im_x', 'site_x_x', 'ident_x', 'site_y_x', 'im_date_x', 'dr_ann_asa_ssav_x', 'im_name_x', 'd_type_x', 'image_dt_x', 'dr_asav_dt_x', 'dr_asav_im_x', 'dr_ann_asm_ssmd_y', 'dr_asmd_dt_y', 'dr_asmd_im_y', 'dr_ann_mavg_x', 'dr_aavg_dt_x', 'dr_aavg_im_x', 'dr_ann_mmed_y', 'dr_amed_dt_y', 'dr_amed_im_y', 'dr_ann_msin_siav_x', 'dr_asiv_dt_x', 'dr_asiv_im_x', 'dr_ann_msin_simd_y', 'dr_asid_dt_y', 'dr_asid_im_y', 'dr_ann_msum_x', 'dr_asum_dt_x', 'dr_asum_im_x', 'dr_dry_asa_ssav_y', 'dr_dsav_dt_y', 'dr_dsav_im_y', 'dr_dry_asm_ssmd_x', 'dr_dsmd_dt_x', 'dr_dsmd_im_x', 'dr_dry_mavg_y', 'dr_davg_dt_y', 'dr_davg_im_y', 'dr_dry_mmed_x', 'dr_dmed_dt_x', 'dr_dmed_im_x', 'dr_dry_msin_siav_y', 'dr_dsiv_dt_y', 'dr_dsiv_im_y', 'dr_dry_msin_simd_x', 'dr_dsid_dt_x', 'dr_dsid_im_x', 'dr_dry_msum_y', 'dr_dsum_dt_y', 'dr_dsum_im_y', 'dr_mth_mavg_x', 'dr_mavg_dt_x', 'dr_mavg_im_x', 'site_x_y', 'ident_y', 'site_y_y', 'im_date_y', 'dr_ann_asa_ssav_y', 'im_name_y', 'd_type_y', 'image_dt_y', 'dr_asav_dt_y', 'dr_asav_im_y', 'dr_ann_asm_ssmd_x', 'dr_asmd_dt_x', 'dr_asmd_im_x', 'dr_ann_mavg_y', 'dr_aavg_dt_y', 'dr_aavg_im_y', 'dr_ann_mmed_x', 'dr_amed_dt_x', 'dr_amed_im_x', 'dr_ann_msin_siav_y', 'dr_asiv_dt_y', 'dr_asiv_im_y', 'dr_ann_msin_simd_x', 'dr_asid_dt_x', 'dr_asid_im_x', 'dr_ann_msum_y', 'dr_asum_dt_y', 'dr_asum_im_y', 'dr_dry_asa_ssav_x', 'dr_dsav_dt_x', 'dr_dsav_im_x', 'dr_dry_asm_ssmd_y', 'dr_dsmd_dt_y', 'dr_dsmd_im_y', 'dr_dry_mavg_x', 'dr_davg_dt_x', 'dr_davg_im_x', 'dr_dry_mmed_y', 'dr_dmed_dt_y', 'dr_dmed_im_y', 'dr_dry_msin_siav_x', 'dr_dsiv_dt_x', 'dr_dsiv_im_x', 'dr_dry_msin_simd_y', 'dr_dsid_dt_y', 'dr_dsid_im_y', 'dr_dry_msum_x', 'dr_dsum_dt_x', 'dr_dsum_im_x', 'dr_mth_mavg_y', 'dr_mavg_dt_y', 'dr_mavg_im_y', 'dr_mth_mmed_x', 'dr_mmed_dt_x', 'dr_mmed_im_x', 'site_x_x', 'ident_x', 'site_y_x', 'im_date_x', 'dr_ann_asa_ssav_x', 'im_name_x', 'd_type_x', 'image_dt_x', 'dr_asav_dt_x', 'dr_asav_im_x', 'dr_ann_asm_ssmd_y', 'dr_asmd_dt_y', 'dr_asmd_im_y', 'dr_ann_mavg_x', 'dr_aavg_dt_x', 'dr_aavg_im_x', 'dr_ann_mmed_y', 'dr_amed_dt_y', 'dr_amed_im_y', 'dr_ann_msin_siav_x', 'dr_asiv_dt_x', 'dr_asiv_im_x', 'dr_ann_msin_simd_y', 'dr_asid_dt_y', 'dr_asid_im_y', 'dr_ann_msum_x', 'dr_asum_dt_x', 'dr_asum_im_x', 'dr_dry_asa_ssav_y', 'dr_dsav_dt_y', 'dr_dsav_im_y', 'dr_dry_asm_ssmd_x', 'dr_dsmd_dt_x', 'dr_dsmd_im_x', 'dr_dry_mavg_y', 'dr_davg_dt_y', 'dr_davg_im_y', 'dr_dry_mmed_x', 'dr_dmed_dt_x', 'dr_dmed_im_x', 'dr_dry_msin_siav_y', 'dr_dsiv_dt_y', 'dr_dsiv_im_y', 'dr_dry_msin_simd_x', 'dr_dsid_dt_x', 'dr_dsid_im_x', 'dr_dry_msum_y', 'dr_dsum_dt_y', 'dr_dsum_im_y', 'dr_mth_mavg_x', 'dr_mavg_dt_x', 'dr_mavg_im_x', 'dr_mth_mmed_y', 'dr_mmed_dt_y', 'dr_mmed_im_y', 'dr_mth_msum_x', 'dr_msum_dt_x', 'dr_msum_im_x', 'site_x_y', 'ident_y', 'site_y_y', 'im_date_y', 'dr_ann_asa_ssav_y', 'im_name_y', 'd_type_y', 'image_dt_y', 'dr_asav_dt_y', 'dr_asav_im_y', 'dr_ann_asm_ssmd_x', 'dr_asmd_dt_x', 'dr_asmd_im_x', 'dr_ann_mavg_y', 'dr_aavg_dt_y', 'dr_aavg_im_y', 'dr_ann_mmed_x', 'dr_amed_dt_x', 'dr_amed_im_x', 'dr_ann_msin_siav_y', 'dr_asiv_dt_y', 'dr_asiv_im_y', 'dr_ann_msin_simd_x', 'dr_asid_dt_x', 'dr_asid_im_x', 'dr_ann_msum_y', 'dr_asum_dt_y', 'dr_asum_im_y', 'dr_dry_asa_ssav_x', 'dr_dsav_dt_x', 'dr_dsav_im_x', 'dr_dry_asm_ssmd_y', 'dr_dsmd_dt_y', 'dr_dsmd_im_y', 'dr_dry_mavg_x', 'dr_davg_dt_x', 'dr_davg_im_x', 'dr_dry_mmed_y', 'dr_dmed_dt_y', 'dr_dmed_im_y', 'dr_dry_msin_siav_x', 'dr_dsiv_dt_x', 'dr_dsiv_im_x', 'dr_dry_msin_simd_y', 'dr_dsid_dt_y', 'dr_dsid_im_y', 'dr_dry_msum_x', 'dr_dsum_dt_x', 'dr_dsum_im_x', 'dr_mth_mavg_y', 'dr_mavg_dt_y', 'dr_mavg_im_y', 'dr_mth_mmed_x', 'dr_mmed_dt_x', 'dr_mmed_im_x', 'dr_mth_msum_y', 'dr_msum_dt_y', 'dr_msum_im_y', 'dr_wet_asa_ssav_x', 'dr_wsav_dt_x', 'dr_wsav_im_x', 'site_x_x', 'ident_x', 'site_y_x', 'im_date_x', 'dr_ann_asa_ssav_x', 'im_name_x', 'd_type_x', 'image_dt_x', 'dr_asav_dt_x', 'dr_asav_im_x', 'dr_ann_asm_ssmd_y', 'dr_asmd_dt_y', 'dr_asmd_im_y', 'dr_ann_mavg_x', 'dr_aavg_dt_x', 'dr_aavg_im_x', 'dr_ann_mmed_y', 'dr_amed_dt_y', 'dr_amed_im_y', 'dr_ann_msin_siav_x', 'dr_asiv_dt_x', 'dr_asiv_im_x', 'dr_ann_msin_simd_y', 'dr_asid_dt_y', 'dr_asid_im_y', 'dr_ann_msum_x', 'dr_asum_dt_x', 'dr_asum_im_x', 'dr_dry_asa_ssav_y', 'dr_dsav_dt_y', 'dr_dsav_im_y', 'dr_dry_asm_ssmd_x', 'dr_dsmd_dt_x', 'dr_dsmd_im_x', 'dr_dry_mavg_y', 'dr_davg_dt_y', 'dr_davg_im_y', 'dr_dry_mmed_x', 'dr_dmed_dt_x', 'dr_dmed_im_x', 'dr_dry_msin_siav_y', 'dr_dsiv_dt_y', 'dr_dsiv_im_y', 'dr_dry_msin_simd_x', 'dr_dsid_dt_x', 'dr_dsid_im_x', 'dr_dry_msum_y', 'dr_dsum_dt_y', 'dr_dsum_im_y', 'dr_mth_mavg_x', 'dr_mavg_dt_x', 'dr_mavg_im_x', 'dr_mth_mmed_y', 'dr_mmed_dt_y', 'dr_mmed_im_y', 'dr_mth_msum_x', 'dr_msum_dt_x', 'dr_msum_im_x', 'dr_wet_asa_ssav_y', 'dr_wsav_dt_y', 'dr_wsav_im_y', 'dr_wet_asm_ssmd_x', 'dr_wsmd_dt_x', 'dr_wsmd_im_x', 'site_x_y', 'ident_y', 'site_y_y', 'im_date_y', 'dr_ann_asa_ssav_y', 'im_name_y', 'd_type_y', 'image_dt_y', 'dr_asav_dt_y', 'dr_asav_im_y', 'dr_ann_asm_ssmd_x', 'dr_asmd_dt_x', 'dr_asmd_im_x', 'dr_ann_mavg_y', 'dr_aavg_dt_y', 'dr_aavg_im_y', 'dr_ann_mmed_x', 'dr_amed_dt_x', 'dr_amed_im_x', 'dr_ann_msin_siav_y', 'dr_asiv_dt_y', 'dr_asiv_im_y', 'dr_ann_msin_simd_x', 'dr_asid_dt_x', 'dr_asid_im_x', 'dr_ann_msum_y', 'dr_asum_dt_y', 'dr_asum_im_y', 'dr_dry_asa_ssav_x', 'dr_dsav_dt_x', 'dr_dsav_im_x', 'dr_dry_asm_ssmd_y', 'dr_dsmd_dt_y', 'dr_dsmd_im_y', 'dr_dry_mavg_x', 'dr_davg_dt_x', 'dr_davg_im_x', 'dr_dry_mmed_y', 'dr_dmed_dt_y', 'dr_dmed_im_y', 'dr_dry_msin_siav_x', 'dr_dsiv_dt_x', 'dr_dsiv_im_x', 'dr_dry_msin_simd_y', 'dr_dsid_dt_y', 'dr_dsid_im_y', 'dr_dry_msum_x', 'dr_dsum_dt_x', 'dr_dsum_im_x', 'dr_mth_mavg_y', 'dr_mavg_dt_y', 'dr_mavg_im_y', 'dr_mth_mmed_x', 'dr_mmed_dt_x', 'dr_mmed_im_x', 'dr_mth_msum_y', 'dr_msum_dt_y', 'dr_msum_im_y', 'dr_wet_asa_ssav_x', 'dr_wsav_dt_x', 'dr_wsav_im_x', 'dr_wet_asm_ssmd_y', 'dr_wsmd_dt_y', 'dr_wsmd_im_y', 'dr_wet_mavg_x', 'dr_wavg_dt_x', 'dr_wavg_im_x', 'site_x_x', 'ident_x', 'site_y_x', 'im_date_x', 'dr_ann_asa_ssav_x', 'im_name_x', 'd_type_x', 'image_dt_x', 'dr_asav_dt_x', 'dr_asav_im_x', 'dr_ann_asm_ssmd_y', 'dr_asmd_dt_y', 'dr_asmd_im_y', 'dr_ann_mavg_x', 'dr_aavg_dt_x', 'dr_aavg_im_x', 'dr_ann_mmed_y', 'dr_amed_dt_y', 'dr_amed_im_y', 'dr_ann_msin_siav_x', 'dr_asiv_dt_x', 'dr_asiv_im_x', 'dr_ann_msin_simd_y', 'dr_asid_dt_y', 'dr_asid_im_y', 'dr_ann_msum_x', 'dr_asum_dt_x', 'dr_asum_im_x', 'dr_dry_asa_ssav_y', 'dr_dsav_dt_y', 'dr_dsav_im_y', 'dr_dry_asm_ssmd_x', 'dr_dsmd_dt_x', 'dr_dsmd_im_x', 'dr_dry_mavg_y', 'dr_davg_dt_y', 'dr_davg_im_y', 'dr_dry_mmed_x', 'dr_dmed_dt_x', 'dr_dmed_im_x', 'dr_dry_msin_siav_y', 'dr_dsiv_dt_y', 'dr_dsiv_im_y', 'dr_dry_msin_simd_x', 'dr_dsid_dt_x', 'dr_dsid_im_x', 'dr_dry_msum_y', 'dr_dsum_dt_y', 'dr_dsum_im_y', 'dr_mth_mavg_x', 'dr_mavg_dt_x', 'dr_mavg_im_x', 'dr_mth_mmed_y', 'dr_mmed_dt_y', 'dr_mmed_im_y', 'dr_mth_msum_x', 'dr_msum_dt_x', 'dr_msum_im_x', 'dr_wet_asa_ssav_y', 'dr_wsav_dt_y', 'dr_wsav_im_y', 'dr_wet_asm_ssmd_x', 'dr_wsmd_dt_x', 'dr_wsmd_im_x', 'dr_wet_mavg_y', 'dr_wavg_dt_y', 'dr_wavg_im_y', 'dr_wet_mmed_x', 'dr_wmed_dt_x', 'dr_wmed_im_x', 'site_x_y', 'ident_y', 'site_y_y', 'im_date_y', 'dr_ann_asa_ssav_y', 'im_name_y', 'd_type_y', 'image_dt_y', 'dr_asav_dt_y', 'dr_asav_im_y', 'dr_ann_asm_ssmd_x', 'dr_asmd_dt_x', 'dr_asmd_im_x', 'dr_ann_mavg_y', 'dr_aavg_dt_y', 'dr_aavg_im_y', 'dr_ann_mmed_x', 'dr_amed_dt_x', 'dr_amed_im_x', 'dr_ann_msin_siav_y', 'dr_asiv_dt_y', 'dr_asiv_im_y', 'dr_ann_msin_simd_x', 'dr_asid_dt_x', 'dr_asid_im_x', 'dr_ann_msum_y', 'dr_asum_dt_y', 'dr_asum_im_y', 'dr_dry_asa_ssav_x', 'dr_dsav_dt_x', 'dr_dsav_im_x', 'dr_dry_asm_ssmd_y', 'dr_dsmd_dt_y', 'dr_dsmd_im_y', 'dr_dry_mavg_x', 'dr_davg_dt_x', 'dr_davg_im_x', 'dr_dry_mmed_y', 'dr_dmed_dt_y', 'dr_dmed_im_y', 'dr_dry_msin_siav_x', 'dr_dsiv_dt_x', 'dr_dsiv_im_x', 'dr_dry_msin_simd_y', 'dr_dsid_dt_y', 'dr_dsid_im_y', 'dr_dry_msum_x', 'dr_dsum_dt_x', 'dr_dsum_im_x', 'dr_mth_mavg_y', 'dr_mavg_dt_y', 'dr_mavg_im_y', 'dr_mth_mmed_x', 'dr_mmed_dt_x', 'dr_mmed_im_x', 'dr_mth_msum_y', 'dr_msum_dt_y', 'dr_msum_im_y', 'dr_wet_asa_ssav_x', 'dr_wsav_dt_x', 'dr_wsav_im_x', 'dr_wet_asm_ssmd_y', 'dr_wsmd_dt_y', 'dr_wsmd_im_y', 'dr_wet_mavg_x', 'dr_wavg_dt_x', 'dr_wavg_im_x', 'dr_wet_mmed_y', 'dr_wmed_dt_y', 'dr_wmed_im_y', 'dr_wet_msin_siav_x', 'dr_wsiv_dt_x', 'dr_wsiv_im_x', 'site_x_x', 'ident_x', 'site_y_x', 'im_date_x', 'dr_ann_asa_ssav_x', 'im_name_x', 'd_type_x', 'image_dt_x', 'dr_asav_dt_x', 'dr_asav_im_x', 'dr_ann_asm_ssmd_y', 'dr_asmd_dt_y', 'dr_asmd_im_y', 'dr_ann_mavg_x', 'dr_aavg_dt_x', 'dr_aavg_im_x', 'dr_ann_mmed_y', 'dr_amed_dt_y', 'dr_amed_im_y', 'dr_ann_msin_siav_x', 'dr_asiv_dt_x', 'dr_asiv_im_x', 'dr_ann_msin_simd_y', 'dr_asid_dt_y', 'dr_asid_im_y', 'dr_ann_msum_x', 'dr_asum_dt_x', 'dr_asum_im_x', 'dr_dry_asa_ssav_y', 'dr_dsav_dt_y', 'dr_dsav_im_y', 'dr_dry_asm_ssmd_x', 'dr_dsmd_dt_x', 'dr_dsmd_im_x', 'dr_dry_mavg_y', 'dr_davg_dt_y', 'dr_davg_im_y', 'dr_dry_mmed_x', 'dr_dmed_dt_x', 'dr_dmed_im_x', 'dr_dry_msin_siav_y', 'dr_dsiv_dt_y', 'dr_dsiv_im_y', 'dr_dry_msin_simd_x', 'dr_dsid_dt_x', 'dr_dsid_im_x', 'dr_dry_msum_y', 'dr_dsum_dt_y', 'dr_dsum_im_y', 'dr_mth_mavg_x', 'dr_mavg_dt_x', 'dr_mavg_im_x', 'dr_mth_mmed_y', 'dr_mmed_dt_y', 'dr_mmed_im_y', 'dr_mth_msum_x', 'dr_msum_dt_x', 'dr_msum_im_x', 'dr_wet_asa_ssav_y', 'dr_wsav_dt_y', 'dr_wsav_im_y', 'dr_wet_asm_ssmd_x', 'dr_wsmd_dt_x', 'dr_wsmd_im_x', 'dr_wet_mavg_y', 'dr_wavg_dt_y', 'dr_wavg_im_y', 'dr_wet_mmed_x', 'dr_wmed_dt_x', 'dr_wmed_im_x', 'dr_wet_msin_siav_y', 'dr_wsiv_dt_y', 'dr_wsiv_im_y', 'dr_wet_msin_simd_x', 'dr_wsid_dt_x', 'dr_wsid_im_x', 'site_x_y', 'ident_y', 'site_y_y', 'im_date_y', 'dr_ann_asa_ssav_y', 'im_name_y', 'd_type_y', 'image_dt_y', 'dr_asav_dt_y', 'dr_asav_im_y', 'dr_ann_asm_ssmd', 'dr_asmd_dt', 'dr_asmd_im', 'dr_ann_mavg_y', 'dr_aavg_dt_y', 'dr_aavg_im_y', 'dr_ann_mmed', 'dr_amed_dt', 'dr_amed_im', 'dr_ann_msin_siav_y', 'dr_asiv_dt_y', 'dr_asiv_im_y', 'dr_ann_msin_simd', 'dr_asid_dt', 'dr_asid_im', 'dr_ann_msum_y', 'dr_asum_dt_y', 'dr_asum_im_y', 'dr_dry_asa_ssav', 'dr_dsav_dt', 'dr_dsav_im', 'dr_dry_asm_ssmd_y', 'dr_dsmd_dt_y', 'dr_dsmd_im_y', 'dr_dry_mavg', 'dr_davg_dt', 'dr_davg_im', 'dr_dry_mmed_y', 'dr_dmed_dt_y', 'dr_dmed_im_y', 'dr_dry_msin_siav', 'dr_dsiv_dt', 'dr_dsiv_im', 'dr_dry_msin_simd_y', 'dr_dsid_dt_y', 'dr_dsid_im_y', 'dr_dry_msum', 'dr_dsum_dt', 'dr_dsum_im', 'dr_mth_mavg_y', 'dr_mavg_dt_y', 'dr_mavg_im_y', 'dr_mth_mmed', 'dr_mmed_dt', 'dr_mmed_im', 'dr_mth_msum_y', 'dr_msum_dt_y', 'dr_msum_im_y', 'dr_wet_asa_ssav', 'dr_wsav_dt', 'dr_wsav_im', 'dr_wet_asm_ssmd_y', 'dr_wsmd_dt_y', 'dr_wsmd_im_y', 'dr_wet_mavg', 'dr_wavg_dt', 'dr_wavg_im', 'dr_wet_mmed_y', 'dr_wmed_dt_y', 'dr_wmed_im_y', 'dr_wet_msin_siav', 'dr_wsiv_dt', 'dr_wsiv_im', 'dr_wet_msin_simd_y', 'dr_wsid_dt_y', 'dr_wsid_im_y', 'dr_wet_msum', 'dr_wsum_dt', 'dr_wsum_im']

    # Drop the features
    df_dropped = df.drop(columns=features_to_drop, inplace=True)

    drop_list = [columns for columns in df_columns if "site_X" not in columns]
    print("drop column list: ", drop_list)

    df.drop(drop_list, axis=1, inplace=True)
    #print("cleaned df shape:", df.shape)

    var_ = df1.iloc[:, [24, 13]]
    uid_var = df1.iloc[:, 0]
    date_var = df1.iloc[:, 3]
    # rain_image_var = df1.iloc[:, 39]
    # et_image_var = df1.iloc[:, 80]
    # et_image_var = df1.iloc[:, 39]
    # et_image_var = df1.iloc[:, 39]
    # et_image_var = df1.iloc[:, 39]

    var_columns = var_.columns.tolist()
    #("var_columns: ", var_columns)
    df_out = pd.concat([var_, df], axis=1)
    #print("cleaned df shape:", df_out.columns.tolist())

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


def main_routine(biomass_csv, dir_,
                 output_dir,
                 dp0_dbg_si_single_annual_density,
                 dp0_dbg_si_mask_single_annual_density,
                 dp0_dbg_si_single_dry_density,
                 dp0_dbg_si_mask_single_dry_density,
                 dp1_dbi_si_annual_density,
                 dp1_dbi_si_annual_mask_density,
                 dp1_dbi_si_dry_density,
                 dp1_dbi_si_dry_mask_density):


    
    """biomass_csv, dir_,
                 output_dir,
                 dp0_dbg_si_single_annual_density,
                 dp0_dbg_si_mask_single_annual_density,
                 dp0_dbg_si_single_dry_density,
                 dp0_dbg_si_mask_single_dry_density,
                 dp1_dbi_si_annual_density,
                 dp1_dbi_si_annual_mask_density,
                 dp1_dbi_si_dry_density,
                 dp1_dbi_si_dry_mask_density"""
    #biomass_csv = r"C:\Users\robot\projects\biomass\collated_agb\20241221\slats_tern_biomass.csv"
    # #
    #dir_ = r"C:\Users\robot\projects\biomass\zonal_stats_raw\met_clean_1988_2024"
    #dir_ = r"F:\silo\outputs\met_clean"
    # F:\silo\outputs\met_clean
    #output_dir = r"C:\Users\robot\projects\biomass\scratch_outputs\met_zonal"
    #
    # print("manual biomass: ", r"C:\Users\robot\projects\biomass\collated_agb\20240707\slats_tern_biomass.csv")
    #
    # print("manual dir_: ", r"C:\Users\robot\projects\biomass\zonal_stats_raw\met_clean")
    #
    # print("manual output_dir: ", r"C:\Users\robot\projects\biomass\scratch_outputs\met_zonal")
    # dp0_dbg_si_single_annual_density,)
    # dp0_dbg_si_mask_single_annual_density,
    # dp0_dbg_si_single_dry_density,
    # dp0_dbg_si_mask_single_dry_density,
    # dp1_dbi_si_annual_density,
    # dp1_dbi_si_annual_mask_density,
    # dp1_dbi_si_dry_density,
    # dp1_dbi_si_dry_mask_density

    # dictionary {variable: [out_name]
    qld_dict = {"rh_tmax": ["rx"],
                "rh_tmin": ["rn"],
                "daily_rain": ["dr"],
                "et_morton_actual": ["ma"],
                "max_temp": ["tx"],
                "min_temp": ["tn"],
                }

    # file_dict = {"ann_asa_ssav": ["asav"],
    #              "ann_asm_ssmd": ["asmd"],
    #              "ann_mavg": ["aavg"],
    #              "ann_mmed": ["amed"],
    #              "ann_msin_siav": ["asiv"],
    #              "ann_msin_simd": ["asid"],
    #              "ann_msum": ["asum"],
    #
    #              "cor": ["corr"],
    #
    #              "dry_asa_ssav": ["dsav"],
    #              "dry_asm_ssmd": ["dsmd"],
    #              "dry_mavg": ["davg"],
    #              "dry_mmed": ["dmed"],
    #              "dry_msin_siav": ["dsiv"],
    #              "dry_msin_simd": ["dsid"],
    #              "dry_msum": ["dsum"],
    #
    #              "mth_mavg": ["mavg"],
    #              "mth_mmed": ["mmed"],
    #              "mth_msum": ["msum"],
    #
    #              "wet_asa_ssav": ["wsav"],
    #              "wet_asm_ssmd": ["wsmd"],
    #              "wet_mavg": ["wavg"],
    #              "wet_mmed": ["wmed"],
    #              "wet_msin_siav": ["wsiv"],
    #              "wet_msin_simd": ["wsid"],
    #              "wet_msum": ["wsum"],
    #              }
    #
    # file_dict = {"ann_asa_ssav": ["asav"],
    #              "ann_asm_ssmd": ["asmd"],
    #              "ann_mavg": ["aavg"],
    #              "ann_mmed": ["amed"],
    #              "ann_msin_siav": ["asiv"],
    #              "ann_msin_simd": ["asid"],
    #              "ann_msum": ["asum"],
    #
    #              "cor": ["corr"],
    #
    #              "dry_asa_ssav": ["dsav"],
    #              "dry_asm_ssmd": ["dsmd"],
    #              "dry_mavg": ["davg"],
    #              "dry_mmed": ["dmed"],
    #              "dry_msin_siav": ["dsiv"],
    #              "dry_msin_simd": ["dsid"],
    #              "dry_msum": ["dsum"],
    #
    #              "mth_mavg": ["mavg"],
    #              "mth_mmed": ["mmed"],
    #              "mth_msum": ["msum"],
    #
    #              "wet_asa_ssav": ["wsav"],
    #              "wet_asm_ssmd": ["wsmd"],
    #              "wet_mavg": ["wavg"],
    #              "wet_mmed": ["wmed"],
    #              "wet_msin_siav": ["wsiv"],
    #              "wet_msin_simd": ["wsid"],
    #              "wet_msum": ["wsum"],
    #              }

    file_dict = {
        # DLYRN (Rainfall)
        "dlyrn_ann_mavg": ["aavg"], # Total seasonal average monthly rainfall
        "dlyrn_ann_mmed": ["amed"], # Total seasonal median monthly rainfall
        "dlyrn_ann_msum": ["asum"], # Total seasonal rainfall
        "dlyrn_ann_mmma": ["amma"],  # Total seasonal average monthly rainfall
        "dlyrn_ann_mmmd": ["ammd"],  # Total seasonal median monthly rainfall
        "dlyrn_ann_msin_sasi": ["asii"],  # Seasonal index

        "dlyrn_dry_mavg": ["davg"],
        "dlyrn_dry_mmed": ["dmed"],
        "dlyrn_dry_msum": ["dsum"],
        "dlyrn_dry_mmma": ["dmma"],  # Total seasonal average monthly rainfall
        "dlyrn_dry_mmmd": ["dmmd"],  # Total seasonal median monthly rainfall
        "dlyrn_dry_msin_sasi": ["dsii"],
        "dlyrn_mth_mavg": ["mavg"],
        "dlyrn_mth_mmed": ["mmed"],
        "dlyrn_mth_msum": ["msum"],
        "dlyrn_wet_mavg": ["wavg"],
        "dlyrn_wet_mmed": ["wmed"],
        "dlyrn_wet_msum": ["wsum"],
        "dlyrn_wet_mmma": ["wmma"],  # Total seasonal average monthly rainfall
        "dlyrn_wet_mmmd": ["wmmd"],  # Total seasonal median monthly rainfall
        "dlyrn_wet_msin_sasi": ["wsii"],

        # morat (Morton's ET)
        "morat_ann_mavg": ["aavg"],  # Total seasonal average monthly Mortons ET
        "morat_ann_mmed": ["amed"],  # Total seasonal median monthly Mortons ET
        "morat_ann_msum": ["asum"],  # Total seasonal Mortons ET
        "morat_ann_mmma": ["amma"],  # Total seasonal average monthly Mortons ET
        "morat_ann_mmmd": ["ammd"],  # Total seasonal median monthly Mortons ET
        "morat_ann_msin_sasi": ["asii"],  # Seasonal index

        "morat_dry_mavg": ["davg"],
        "morat_dry_mmed": ["dmed"],
        "morat_dry_msum": ["dsum"],
        "morat_dry_mmma": ["dmma"],  # Total seasonal average monthly Mortons ET
        "morat_dry_mmmd": ["dmmd"],  # Total seasonal median monthly Mortons ET
        "morat_dry_msin_sasi": ["dsii"],
        "morat_mth_mavg": ["mavg"],
        "morat_mth_mmed": ["mmed"],
        "morat_mth_msum": ["msum"],
        "morat_wet_mavg": ["wavg"],
        "morat_wet_mmed": ["wmed"],
        "morat_wet_msum": ["wsum"],
        "morat_wet_mmma": ["wmma"],  # Total seasonal average monthly Mortons ET
        "morat_wet_mmmd": ["wmmd"],  # Total seasonal median monthly Mortons ET
        "morat_wet_msin_sasi": ["wsii"],

        # # MORAT (Morton's ET)
        # "morat_ann_mavg": ["aavg"],
        # "morat_ann_mmed": ["amed"],
        # "morat_ann_msum": ["asum"],
        # "morat_ann_msin_sasi": ["assi"],
        # "morat_dry_mavg": ["davg"],
        # "morat_dry_mmed": ["dmed"],
        # "morat_dry_msum": ["dsum"],
        # "morat_dry_msin_sasi": ["dssi"],
        # "morat_mth_mavg": ["mavg"],
        # "morat_mth_mmed": ["ammed"],
        # "morat_mth_msum": ["msum"],
        # "morat_wet_mavg": ["wavg"],
        # "morat_wet_mmed": ["wmed"],
        # "morat_wet_msum": ["wsum"],
        # "morat_wet_msin_sasi": ["wssi"],

        # tpmax (max temp)
        "tpmax_ann_mavg": ["aavg"],  # Total seasonal average monthly max temp
        "tpmax_ann_mmed": ["amed"],  # Total seasonal median monthly max temp
        "tpmax_ann_msum": ["asum"],  # Total seasonal max temp
        "tpmax_ann_mmma": ["amma"],  # Total seasonal average monthly max temp
        "tpmax_ann_mmmd": ["ammd"],  # Total seasonal median monthly max temp
        "tpmax_ann_msin_sasi": ["asii"],  # Seasonal index

        "tpmax_dry_mavg": ["davg"],
        "tpmax_dry_mmed": ["dmed"],
        "tpmax_dry_msum": ["dsum"],
        "tpmax_dry_mmma": ["dmma"],  # Total seasonal average monthly max temp
        "tpmax_dry_mmmd": ["dmmd"],  # Total seasonal median monthly max temp
        "tpmax_dry_msin_sasi": ["dsii"],
        "tpmax_mth_mavg": ["mavg"],
        "tpmax_mth_mmed": ["mmed"],
        "tpmax_mth_msum": ["msum"],
        "tpmax_wet_mavg": ["wavg"],
        "tpmax_wet_mmed": ["wmed"],
        "tpmax_wet_msum": ["wsum"],
        "tpmax_wet_mmma": ["wmma"],  # Total seasonal average monthly max temp
        "tpmax_wet_mmmd": ["wmmd"],  # Total seasonal median monthly max temp
        "tpmax_wet_msin_sasi": ["wsii"],

        # # TPMAX (Maximum Temperature)
        # "tpmax_ann_mavg": ["aavg"],
        # "tpmax_ann_mmed": ["amed"],
        # "tpmax_ann_msum": ["asum"],
        # "tpmax_ann_msin_sasi": ["asum"],
        # "tpmax_dry_mavg": ["davg"],
        # "tpmax_dry_mmed": ["dmed"],
        # "tpmax_dry_msum": ["dsum"],
        # "tpmax_dry_msin_sasi": ["dssi"],
        # "tpmax_mth_mavg": ["mavg"],
        # "tpmax_mth_mmed": ["ammed"],
        # "tpmax_mth_msum": ["msum"],
        # "tpmax_wet_mavg": ["wavg"],
        # "tpmax_wet_mmed": ["wmed"],
        # "tpmax_wet_msum": ["wsum"],
        # "tpmax_wet_msin_sasi": ["wssi"],

        # rhmax (Relative humidity)
        "rhmax_ann_mavg": ["aavg"],  # Total seasonal average monthly
        "rhmax_ann_mmed": ["amed"],  # Total seasonal median monthly
        "rhmax_ann_msum": ["asum"],  # Total seasonal
        "rhmax_ann_mmma": ["amma"],  # Total seasonal average monthly
        "rhmax_ann_mmmd": ["ammd"],  # Total seasonal median monthly
        "rhmax_ann_msin_sasi": ["asii"],  # Seasonal index

        "rhmax_dry_mavg": ["davg"],
        "rhmax_dry_mmed": ["dmed"],
        "rhmax_dry_msum": ["dsum"],
        "rhmax_dry_mmma": ["dmma"],  # Total seasonal average monthly
        "rhmax_dry_mmmd": ["dmmd"],  # Total seasonal median monthly
        "rhmax_dry_msin_sasi": ["dsii"],
        "rhmax_mth_mavg": ["mavg"],
        "rhmax_mth_mmed": ["mmed"],
        "rhmax_mth_msum": ["msum"],
        "rhmax_wet_mavg": ["wavg"],
        "rhmax_wet_mmed": ["wmed"],
        "rhmax_wet_msum": ["wsum"],
        "rhmax_wet_mmma": ["wmma"],  # Total seasonal average monthly
        "rhmax_wet_mmmd": ["wmmd"],  # Total seasonal median monthly
        "rhmax_wet_msin_sasi": ["wsii"],

        # # RHMAX (Relative Humidity)
        # "rhmax_ann_mavg": ["aavg"],
        # "rhmax_ann_mmed": ["amed"],
        # "rhmax_ann_msum": ["asum"],
        # "rhmax_ann_msin_sasi": ["assi"],
        # "rhmax_dry_mavg": ["davg"],
        # "rhmax_dry_mmed": ["dmed"],
        # "rhmax_dry_msum": ["dsum"],
        # "rhmax_dry_msin_sasi": ["dssi"],
        # "rhmax_mth_mavg": ["mavg"],
        # "rhmax_mth_mmed": ["mmed"],
        # "rhmax_mth_msum": ["msum"],
        # "rhmax_wet_mavg": ["wavg"],
        # "rhmax_wet_mmed": ["wmed"],
        # "rhmax_wet_msum": ["wsum"],
        # "rhmax_wet_msin_sasi": ["wssi"],
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

    sub_list = next(os.walk(dir_))[1]
    #print("sub_list: ", sub_list)
    for sub_dir in sub_list:
        print("+" * 100)
        print("sub_dir: ", sub_dir)


        # get value from sub_dir
        dict_values = qld_dict.get(sub_dir)
        print("variable: ", dict_values)

        sub_dir_path = os.path.join(dir_, sub_dir)

        #out_ver_list = []

        # Iterate through all directories and subdirectories
        for root, dirs, files in os.walk(sub_dir_path):
            #print("dirs", dirs)
            #print("files", files)
            csv_files = [f for f in files if f.endswith('.csv')]
            if csv_files:
                # Sort CSV files to concatenate in alphabetical order
                csv_files.sort()
                dfs = []
                for csv_file in csv_files:
                    csv_path = os.path.join(root, csv_file)
                    df = pd.read_csv(csv_path)
                    dfs.append(df)

                # Concatenate all dataframes in the current directory
                combined_df = pd.concat(dfs, ignore_index=True)

                # Output or process the combined dataframe
                #print(f"Concatenated dataframe for {root}:")
                #print(combined_df)
                ver_ = combined_df.d_type.unique().tolist()
                print("ver_: ", ver_)

                file_dict_values = file_dict.get(ver_[0])
                print("variable: ", file_dict_values[0])
                # continue
                out_ver = f"{dict_values[0]}_{file_dict_values[0]}"
                combined_df.rename(columns={"im_name": f"im_{out_ver}",
                                            "mean": f"{out_ver}"}, inplace=True)

                combined_df = convert_to_datetime(combined_df, "im_date", "image_dt")
                combined_df[f"dt_{out_ver}"] = combined_df["im_date"]
                combined_df.sort_values(by='image_dt', inplace=True)

                # correct df site name
                site_list = []
                site_name = combined_df["site"].tolist()
                for i in site_name:
                    # print("i: ", i)
                    n = i.replace("_1ha", "")
                    # print(n)
                    site_list.append(n)

                combined_df["site_clean"] = site_list

                combined_df['site_clean'] = combined_df['site_clean'].str.upper()
                # print("biomass info: ", biomass_df.info())
                biomass_df.sort_values(by="basal_dt", inplace=True)
                # print("biomass_df: ", biomass_df.shape)
                # print("biomass_df: ", biomass_df["basal_dt"])
                # biomass_df.to_csv(
                #     r"C:\Users\robot\projects\biomass\scratch_outputs\met_zonal\{0}_biomass.csv".format(f_type),
                #     index=False)
                combined_df.sort_values(by="image_dt", inplace=True)
                # print("combined_df: ", combined_df.shape)
                outcsv = r"C:\Users\robot\projects\biomass\scratch_outputs\met_zonal\{0}_combined_df.csv".format(
                    out_ver)
                print("outcsv: ", outcsv)
                combined_df.to_csv(outcsv,
                                   index=False)
                print("combined_df: ", combined_df.info())

                # import sys
                # sys.exit()

                # merge data with basal dataset based on the nearest date to the field data colection
                n_df = pd.merge_asof(biomass_df, combined_df, left_on="basal_dt", right_on="image_dt", by="site_clean",
                                     direction="nearest")


                # drop columns for data checking
                #print("n_df.columns: ", n_df.columns)
                # List of feature names to drop
                features_to_drop = ["site_x", 'ident', 'site_y', 'im_date',
                                    'd_type', 'image_dt']

                # Drop the features
                n_df.drop(columns=features_to_drop, inplace=True)
                print("n_df.columns: ", n_df.columns)
                near_df_list.append(n_df)

                # drop columns for ml concat

                # List of feature names to drop
                features_to_drop = [f"dt_{out_ver}", f"im_{out_ver}"]

                # Drop the features
                n_df.drop(columns=features_to_drop, inplace=True)
                print("n_df.columns: ", n_df.columns)
                ml_near_df_list.append(n_df)

                # create temp dirs
                n_temp_dir_ = temp_dir_fn(output_dir, "near")
                # create temp dirs
                n_temp_dir = temp_dir_fn(n_temp_dir_, sub_dir)
                # export csv
                out_file_fn(n_temp_dir, "near", sub_dir, n_df, out_ver)

    # import sys
    # sys.exit("step5 line 559")
    merged_df = merge_df_list_fn(near_df_list)
    #merged_df.drop(columns=["site_clean"], inplace=True)
    merged_df.to_csv(r"C:\Users\robot\projects\biomass\scratch_outputs\silo_biomass_data.csv", index=False)

    ml_merged_df = merge_df_list_fn(ml_near_df_list)
    #ml_merged_df.drop(columns=["site_clean"], inplace=True)
    ml_merged_df.to_csv(r"C:\Users\robot\projects\biomass\scratch_outputs\final_silo_biomass_data.csv", index=False)
    # import sys
    # sys.exit()

    #
    # print(list(n_df1.columns))
    #
    # out = os.path.join(r"C:\Users\robot\projects\biomass\collated_zonal_stats\met", "near_met.csv")
    #
    # n_df1.to_csv(out, index=False)
    #
    # out = os.path.join(r"C:\Users\robot\projects\biomass\collated_zonal_stats\met",
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
    dp0_dbg_si_single_annual_density_near_met = pd.merge(right=dp0_dbg_si_single_annual_density,
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

    out_file = r"C:\Users\robot\projects\biomass\collated_zonal_stats\single\dp0_dbg_si_single_annual_density_near_met.csv"

    dp0_dbg_si_single_annual_density_near_met.to_csv(out_file, index=False)

    print("dp0_dbg_si_single_annual_density_near_met - output ...", out_file)

    dp0_dbg_si_mask_single_annual_density_near_met = pd.merge(right=dp0_dbg_si_mask_single_annual_density,
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
    out_file = r"C:\Users\robot\projects\biomass\collated_zonal_stats\single_mask\dp0_dbg_si_mask_single_annual_density_near_met.csv"
    dp0_dbg_si_mask_single_annual_density_near_met.to_csv(out_file, index=False)
    print("out_file: ", out_file)

    # ------------------------------------------------------ dry merge -------------------------------------------------

    dp0_dbg_si_single_dry_density_near_met = pd.merge(right=dp0_dbg_si_single_dry_density,
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
    out_file = r"C:\Users\robot\projects\biomass\collated_zonal_stats\single\dp0_dbg_si_single_dry_density_near_met.csv"
    dp0_dbg_si_single_dry_density_near_met.to_csv(out_file, index=False)
    print("out_file: ", out_file)

    dp0_dbg_si_mask_single_dry_density_near_met = pd.merge(right=dp0_dbg_si_mask_single_dry_density,
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

    out_file = r"C:\Users\robot\projects\biomass\collated_zonal_stats\single_mask\dp0_dbg_si_mask_single_dry_density_near_met.csv"
    dp0_dbg_si_mask_single_dry_density_near_met.to_csv(out_file, index=False)
    print("out_file: ", out_file)
    # ------------------------------------------------------ annual merge dp1 bbi --------------------------------------

    dp1_dbi_si_annual_density_near_met = pd.merge(right=dp1_dbi_si_annual_density,
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

    out_file =  r"C:\Users\robot\projects\biomass\collated_zonal_stats\annual\dp1_dbi_si_annual_density_near_met.csv"
    dp1_dbi_si_annual_density_near_met.to_csv(out_file, index=False)
    print("out_file: ", out_file)

    dp1_dbi_si_annual_mask_density_near_met = pd.merge(right=dp1_dbi_si_annual_mask_density,
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

    out_file = r"C:\Users\robot\projects\biomass\collated_zonal_stats\annual_mask\dp1_dbi_si_annual_mask_density_near_met.csv"
    dp1_dbi_si_annual_mask_density_near_met.to_csv(out_file, index=False)
    print("out_file: ", out_file)

    # ---------------------------------------------- dry merge dp1 bbi -------------------------------------------------

    dp1_dbi_si_dry_density_near_met = pd.merge(right=dp1_dbi_si_dry_density,
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

    out_file = r"C:\Users\robot\projects\biomass\collated_zonal_stats\dry\dp1_dbi_si_dry_density_near_df_near_met.csv"
    dp1_dbi_si_dry_density_near_met.to_csv(out_file, index=False)
    print("out_file: ", out_file)

    dp1_dbi_si_dry_mask_density_near_met = pd.merge(right=dp1_dbi_si_dry_mask_density,
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

    out_file = r"C:\Users\robot\projects\biomass\collated_zonal_stats\dry_mask\dp1_dbi_si_dry_mask_density_near_met.csv"
    dp1_dbi_si_dry_mask_density_near_met.to_csv(out_file, index=False)
    print("out_file: ", out_file)

    return ml_merged_df, dp0_dbg_si_single_annual_density_near_met, \
        dp0_dbg_si_mask_single_annual_density_near_met, \
        dp0_dbg_si_single_dry_density_near_met, \
        dp0_dbg_si_mask_single_dry_density_near_met, \
        dp1_dbi_si_annual_density_near_met, \
        dp1_dbi_si_annual_mask_density_near_met, \
        dp1_dbi_si_dry_density_near_met, \
        dp1_dbi_si_dry_mask_density_near_met,


if __name__ == '__main__':
    main_routine()
