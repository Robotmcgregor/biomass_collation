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
import pandas as pd
from glob import glob
import os
from calendar import monthrange
from datetime import datetime
from shapely.geometry import Point


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
    for t in df[col_nm_s]:
        i = str(t).strip()
        # print("i: ", i)
        datetime_object = datetime.strptime(str(i), '%Y%m%d')
        date_list.append(datetime_object)
        # print(datetime_object)
        # df[col_nm_d] =  pd.to_datetime(df[col_nm_s], format='%Y%m%d.%f')
        # date_time = now.strftime("%m/%d/%Y, %H:%M:%S")
    df[col_nm_d] = date_list
    return df


def zonal_image_date(df):
    date_list = []
    for i in df["image"]:
        i_list = i.split("_")
        t = i_list[2]
        # print(t)
        i = str(t).strip()
        # print("i: ", i)
        datetime_object = datetime.strptime(str(i), '%Y%m%d')
        date_list.append(datetime_object)
        # print(datetime_object)
    df["image_dt"] = date_list
    print("zonal_image_date: ", df.columns)
    return df


def seasonal_image_date(df):

    print("seasonal image date: ", df.columns)
    date_list = []
    # print("init seasonal image date....")
    for s, m, y in zip(df["s_day"], df["s_month"], df["s_year"]):
        # print(s, m, y)

        if s <= 9:
            st = str(s)
            sdt = "0{0}".format(st)
        else:
            sdt = str(s)

        if m <= 9:
            mt = str(m)
            mdt = "0{0}".format(mt)
        else:
            mdt = str(m)

        ydt = str(y)
        st_date = "{0}{1}{2}".format(ydt, mdt, sdt)
        # print("st_date: ", st_date)

        datetime_object = datetime.strptime(str(st_date), '%Y%m%d')
        date_list.append(datetime_object)
        # print(datetime_object)
    df["image_dt"] = date_list
    print("seasonal image df: ", df.columns)

    return df


def workflow(dir_):
    """ os walks through the tile zonal stats directory and locates all zonal stats csv files.
    Ensures that all date fields are labeled 'im_date' (i.e. image date).
    Creates a start and end image field for seasonal data (i.e. s_date and e_date).
    Appends files to file specific lists.

    :param dir_: string object containing the path to the tile zonal stats directory.
    :return: dbg_list, dp0_list, dp1_list, seasonal_list
    """
    single_list = []
    seasonal_list = []
    dbg_list = []
    dbi_list = []
    dp0_list = []
    dp1_list = []
    # ---- mask ----
    dbg_mask_list = []
    dbi_mask_list = []
    dp0_mask_list = []
    dp1_mask_list = []

    # print(dir_)
    sub_list = next(os.walk(dir_))[1]
    for i in sub_list:
        sub_list_path = os.path.join(dir_, i)
        sub_sub_list = next(os.walk(sub_list_path))[1]
        for n in sub_sub_list:
            sub_sub_list_path = os.path.join(sub_list_path, n)

            for file_ in glob(os.path.join(sub_sub_list_path, "*zonal_stats.csv")):
                #print(("workflow: ", file_))
                df1 = pd.read_csv(file_)
                #print('df1: ', df1.columns)

                if "date" in df1.columns and "im_date" not in df1.columns:
                    df1.rename(columns={"date": "im_date"}, inplace=True)

                elif "date" not in df1.columns and "im_date" not in df1.columns:
                    if "year" in df1.columns and "month" in df1.columns and "day" in df1.columns:

                        im_date_list = []
                        for index, row in df1.iterrows():
                            im_date_list.append(str(row["year"]) + str(row["month"]) + str(row["day"]))

                        df1["im_date"] = im_date_list

                path_, f = os.path.split(file_)
                name_list = f.split("_")

                if len(name_list) == 6:
                    type_ = name_list[-3]

                    if type_ == "dbg":
                        dbg = zonal_image_date(df1)
                        dbg_list.append(dbg)

                    elif type_ == "dbi":
                        dbi = seasonal_image_date(df1)
                        dbi_list.append(dbi)

                    elif type_ == "dp0":
                        dp0 = zonal_image_date(df1)
                        dp0_list.append(dp0)

                    elif type_ == "dp1":
                        dp1 = seasonal_image_date(df1)
                        dp1_list.append(dp1)

                    else:
                        pass

                elif len(name_list) == 7:
                    type_ = name_list[-4]

                    if type_ == "dbg":
                        dbg_mask = zonal_image_date(df1)
                        dbg_mask_list.append(dbg_mask)

                    elif type_ == "dbi":
                        dbi_mask = seasonal_image_date(df1)
                        dbi_mask_list.append(dbi_mask)

                    elif type_ == "dp0":
                        dp0_mask = zonal_image_date(df1)
                        dp0_mask_list.append(dp0_mask)

                    elif type_ == "dp1":
                        dp1_mask = seasonal_image_date(df1)
                        dp1_mask_list.append(dp1_mask)

                    else:
                        pass

                else:
                    pass
    print("dbg_list: ", dbg.columns)
    print("dp1_mask: ", dp1_mask.columns)
    print("dp0_mask: ", dp0_mask.columns)
    return dbg_list, dbi_list, dp0_list, dp1_list, dbg_mask_list, dbi_mask_list, dp0_mask_list, dp1_mask_list


def file_export(list_, output_dir, output_indv_dir, biomass_df, type_, mask):
    """
    Merge biomass data with tile zonal stats based on one of three merge techniques and export files.
    :param list_:
    :param output_dir:
    :param biomass_df:
    :param type_:
    """

    # sort df on datetime data
    df = pd.concat(list_, axis=0)

    df.sort_values(by="image_dt", inplace=True)

    df.dropna(subset=['b1_{0}_min'.format(type_)], inplace=True)

    # merge data with basal dataset based on the nearest date to the field data collection
    list_merge = ["nearest"]  # , "forward", "backward"]

    # correct df site name
    site_list = []
    site_name = df["site"].tolist()
    for i in site_name:
        # print("original_site name: ", i)
        n = i.replace("_1ha", "")
        # print("new_name: ", n)
        site_list.append(n)

    df["site_clean"] = site_list

    merge_df_list = []
    merge_df_dropna_list = []
    print("file export df: ", df.columns)
    for m_type in list_merge:
        merged_df = pd.merge_asof(biomass_df, df, left_on="basal_dt", right_on="image_dt", by="site_clean",
                                  direction=m_type)

        merged_df["direction"] = m_type
        merged_df["season"] = "single"

        print("merged: ", list(merged_df.columns))

        if mask == False:
            print("mask == False")
            output_path = os.path.join(output_dir, "merged_slats_field_agb_{0}_{1}_tile.csv".format(type_, m_type))
        else:
            print("mask != False")
            output_path = os.path.join(output_dir, "merged_slats_field_agb_{0}_mask_{1}_tile.csv".format(type_, m_type))
        merged_df.to_csv(os.path.join(output_path), index=False)
        print("Merged file output to: ", output_path)

        merge_df_list.append(merged_df)

        merged_df.dropna(subset=["site_y"], inplace=True)

        if mask == False:
            output_path = os.path.join(output_dir,
                                       "merged_slats_field_agb_{0}_{1}_tile_dropna.csv".format(type_, m_type))
        else:
            output_path = os.path.join(output_dir,
                                       "merged_slats_field_agb_{0}_mask_{1}_tile_dropna.csv".format(type_, m_type))

        merged_df.to_csv(os.path.join(output_path), index=False)
        print("File output to (file_export): ", output_path)

        merge_df_dropna_list.append(merged_df)

    return merge_df_list, merge_df_dropna_list


def file_fm_export(list_, output_dir, output_indv_dir, biomass_df, type_, mask):
    """
    Merge biomass data with tile zonal stats based on one of three merge techniques and export files.
    :param list_:
    :param output_dir:
    :param biomass_df:
    :param type_:
    """

    # sort df on datetime data
    df = pd.concat(list_, axis=0)

    df.sort_values(by="image_dt", inplace=True)
    # print(df.columns)
    # print(df.shape)
    df.dropna(subset=['b1_{0}fm_min'.format(type_)], inplace=True)
    # print(df.info())

    # merge data with basal dataset based on the nearest date to the field data collection
    list_merge = ["nearest"]  # , "forward", "backward"]

    # correct df site name
    site_list = []
    site_name = df["site"].tolist()
    for i in site_name:
        # print("original_site name: ", i)
        n = i.replace("_1ha", "")
        # print("new_name: ", n)
        site_list.append(n)

    df["site_clean"] = site_list

    # for i in biomass_df.site_clean.tolist():
    #     print("biomass_site: ", i)

    merge_df_list = []
    merge_df_dropna_list = []

    for m_type in list_merge:
        merged_df = pd.merge_asof(biomass_df, df, left_on="basal_dt", right_on="image_dt", by="site_clean",
                                  direction=m_type)

        merged_df["direction"] = m_type
        merged_df["season"] = "single"

        if mask == False:
            output_path = os.path.join(output_dir, "merged_slats_field_agb_{0}_{1}_tile.csv".format(type_, m_type))
        else:
            output_path = os.path.join(output_dir, "merged_slats_field_agb_{0}_mask_{1}_tile.csv".format(type_, m_type))
        merged_df.to_csv(os.path.join(output_path), index=False)
        print("File output to: ", output_path)

        merge_df_list.append(merged_df)

        merged_df.dropna(subset=["site_y"], inplace=True)

        if mask == False:
            output_path = os.path.join(output_dir,
                                       "merged_slats_field_agb_{0}_{1}_tile_dropna.csv".format(type_, m_type))
        else:
            output_path = os.path.join(output_dir,
                                       "merged_slats_field_agb_{0}_mask_{1}_tile_dropna.csv".format(type_, m_type))

        merged_df.to_csv(os.path.join(output_path), index=False)
        print("File output to: ", output_path)

        merge_df_dropna_list.append(merged_df)

    return merge_df_list, merge_df_dropna_list


def seasonal_file_export(list_, output_dir, output_indv_dir, biomass_df, type_, mask):
    print("seasonal file export init")
    # dp1_list, tile_export, tile_indv_export, biomass_df, "dp1", False
    #print("list_: ", list_.columns)
    dp1 = pd.concat(list_, axis=0)
    print("dp1 columns: ", dp1.columns)
    dp1.sort_values(by="image_dt", inplace=True)
    dp1.dropna(subset=['b1_{0}_min'.format(type_)], inplace=True)
    'b1_{0}_min'.format(type_)

    # export concatenated dp1 zonal stats
    output_path = os.path.join(output_dir, "{0}_zonal_concat.csv".format(type_))
    dp1.to_csv(os.path.join(output_path), index=False)
    print("File output to: ", output_path)

    # correct dp1 site name
    site_list = []
    site_name = dp1["site"].tolist()

    for i in site_name:
        n = i.replace("_1ha", "")
        site_list.append(n)

    dp1["site_clean"] = site_list

    dry = dp1[dp1["s_month"] == 5]
    annual = dp1[dp1["s_month"] == 1]

    df_list = [dry, annual]
    df_str_list = ["dry", "annual"]

    merge_df_list = []
    merge_df_dropna_list = []

    print("dry: ", dry.columns)
    print("annual: ", annual.columns)

    for f_type, df in zip(df_str_list, df_list):

        print("type: ", f_type)
        print("seasonal_columns: ", df.columns)
        # sort df on datetime data
        df.sort_values(by="image_dt", inplace=True)
        df.dropna(subset=['b1_{0}_min'.format(type_)], inplace=True)

        # correct df site name
        site_list = []
        site_name = df["site"].tolist()

        for i in site_name:
            n = i.replace("_1ha", "")
            site_list.append(n)

        df["site_clean"] = site_list

        list_merge = ["nearest"]  # , "forward", "backward"]

        for m_type in list_merge:
            start_merged_df = pd.merge_asof(biomass_df, df, left_on="basal_dt", right_on="image_dt", by="site_clean",
                                            direction=m_type)

            start_merged_df["direction"] = m_type
            start_merged_df["season"] = f_type

            if mask == False:
                output_path = os.path.join(output_dir,
                                           "merged_slats_field_agb_start_{0}_{1}_{2}_tile.csv".format(type_, f_type,
                                                                                                      m_type))
            else:
                output_path = os.path.join(output_dir,
                                           "merged_slats_field_agb_start_{0}_{1}_mask_{2}_tile.csv".format(type_,
                                                                                                           f_type,
                                                                                                           m_type))

            start_merged_df.to_csv(os.path.join(output_path), index=False)
            print("File output to: ", output_path)

            merge_df_list.append(start_merged_df)

            if mask == False:
                output_path = os.path.join(output_dir,
                                           "merged_slats_field_agb_start_{0}_{1}_{2}_tile_dropna.csv".format(type_,
                                                                                                             f_type,
                                                                                                             m_type))
            else:
                output_path = os.path.join(output_dir,
                                           "merged_slats_field_agb_start_{0}_{1}_mask_{2}_tile_dropna.csv".format(type_,
                                                                                                                  f_type,
                                                                                                                  m_type))

            start_merged_df.dropna(subset=["site_y"], inplace=True)
            start_merged_df.to_csv(os.path.join(output_path), index=False)
            print("File output to: ", output_path)

            merge_df_dropna_list.append(start_merged_df)

    return merge_df_list, merge_df_dropna_list


def seasonal_file_fm_export(list_, output_dir, output_indv_dir, biomass_df, type_, mask):
    # dp1_list, tile_export, tile_indv_export, biomass_df, "dp1", False
    dp1 = pd.concat(list_, axis=0)
    dp1.sort_values(by="image_dt", inplace=True)
    dp1.dropna(subset=['b1_{0}fm_min'.format(type_)], inplace=True)
    'b1_{0}fm_min'.format(type_)

    # export concatenated dp1 zonal stats
    output_path = os.path.join(output_dir, "{0}_zonal_concat.csv".format(type_))
    dp1.to_csv(os.path.join(output_path), index=False)
    print("File output to: ", output_path)

    # correct dp1 site name
    site_list = []
    site_name = dp1["site"].tolist()

    for i in site_name:
        n = i.replace("_1ha", "")
        site_list.append(n)

    dp1["site_clean"] = site_list

    dry = dp1[dp1["s_month"] == 5]
    annual = dp1[dp1["s_month"] == 1]

    df_list = [dry, annual]
    df_str_list = ["dry", "annual"]

    merge_df_list = []
    merge_df_dropna_list = []

    for f_type, df in zip(df_str_list, df_list):

        # print("seasonal_columns: ", df.columns)
        # sort df on datetime data
        df.sort_values(by="image_dt", inplace=True)
        print("line 558 df", df.columns)
        df.dropna(subset=['b1_{0}fm_min'.format(type_)], inplace=True)

        # correct df site name
        site_list = []
        site_name = df["site"].tolist()

        for i in site_name:
            n = i.replace("_1ha", "")
            site_list.append(n)

        df["site_clean"] = site_list

        list_merge = ["nearest"]  # , "forward", "backward"]

        for m_type in list_merge:
            start_merged_df = pd.merge_asof(biomass_df, df, left_on="basal_dt", right_on="image_dt", by="site_clean",
                                            direction=m_type)

            start_merged_df["direction"] = m_type
            start_merged_df["season"] = f_type

            if mask == False:
                output_path = os.path.join(output_dir,
                                           "merged_slats_field_agb_start_{0}_{1}_{2}_tile.csv".format(type_, f_type,
                                                                                                      m_type))
            else:
                output_path = os.path.join(output_dir,
                                           "merged_slats_field_agb_start_{0}_{1}_mask_{2}_tile.csv".format(type_,
                                                                                                           f_type,
                                                                                                           m_type))

            start_merged_df.to_csv(os.path.join(output_path), index=False)
            print("File output to: ", output_path)


            merge_df_list.append(start_merged_df)

            if mask == False:
                output_path = os.path.join(output_dir,
                                           "merged_slats_field_agb_start_{0}_{1}_{2}tile_dropna.csv".format(type_,
                                                                                                            f_type,
                                                                                                            m_type))
            else:
                output_path = os.path.join(output_dir,
                                           "merged_slats_field_agb_start_{0}_{1}_mask_{2}_tile_dropna.csv".format(type_,
                                                                                                                  f_type,
                                                                                                                  m_type))

            start_merged_df.dropna(subset=["site_y"], inplace=True)
            start_merged_df.to_csv(os.path.join(output_path), index=False)
            print("File output to: ", output_path)

            merge_df_dropna_list.append(start_merged_df)

    return merge_df_list, merge_df_dropna_list


def main_routine(biomass_csv, tile_dir, output_dir):
    # Dictionary identifies the data structure of the reference image

    print("tile_dir: ", tile_dir)

    tile_export = os.path.join(output_dir, "tile_concat")
    tile_indv_export = os.path.join(output_dir, "tile_indv_site")
    if not os.path.isdir(tile_export):
        os.mkdir(tile_export)

    if not os.path.isdir(tile_indv_export):
        os.mkdir(tile_indv_export)

    # ------------------------------------------------------------------------------------------------------------------

    # Read in biomass csv
    biomass_df = pd.read_csv(biomass_csv)

    #todo add geometry here

    # Add a geometry field
    #biomass_df['geometry'] = biomass_df.apply(lambda row: Point(row.lon_gda94, row.lat_gda94), axis=1)

    # Convert the pandas DataFrame to a GeoDataFrame
    #gdf = gpd.GeoDataFrame(biomass_df, geometry='geometry')


    print("biomass_df: ", list(biomass_df.columns))
    print("2 - 653")
    # import sys
    # sys.exit()


    # Call the convert to datetime function
    biomass_df = convert_to_datetime(biomass_df, "date", "basal_dt")
    biomass_df.sort_values(by='basal_dt', inplace=True)

    # correct biomass site name
    site_list = []
    site_name = biomass_df["site"].tolist()
    for i in site_name:
        n = i.replace("_", "")
        m = n[-4:]
        x = n[:-4] + "." + m
        # print("x: ", x)

        site_list.append(x)

    biomass_df["site_clean"] = site_list

    dbg_list, dbi_list, dp0_list, dp1_list, dbg_mask_list, dbi_mask_list, dp0_mask_list, dp1_mask_list = workflow(
        tile_dir)



    #todo remove this area
    # #dbg_list = []
    # dbi_list = []
    # dp0_list = []
    # dp1_list = []
    # dbg_mask_list = []
    # dbi_mask_list = []
    # dp0_mask_list = []
    # dp1_mask_list = []


    # --------------------------------------- no fire mask ------------------------------------------

    if len(dbg_list) > 0:
        print("DBG", len(dbg_list))
        merge_dbg_list, merge_dbg_dropna_list = file_export(dbg_list, tile_export, tile_indv_export, biomass_df, "dbg",
                                                            False)

        df = pd.concat(merge_dbg_list)
        df_drop = pd.concat(merge_dbg_dropna_list)
        # todo remove test outputs

        df.to_csv(r"C:\Users\robot\projects\biomass\collated_zonal_stats\sr_fc\df_dbg.csv")
        df_drop.to_csv(r"C:\Users\robot\projects\biomass\collated_zonal_stats\sr_fc\df_dbg_dropna.csv")

        print("df_drop: ", list(df_drop.columns))

        print("2 - 707")
        # import sys
        # sys.exit()


        df_drop_dbg_reformat = df_drop[['uid_x', 'site_clean', 'date', 'lon_gda94', 'lat_gda94', 'geometry',
                                        'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha', 'bio_w_kg1ha',
                                        'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha', 'bio_agb_kg1ha',
                                        'c_l_kg1ha', 'c_t_kg1ha', 'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha',
                                        'c_s_kg1ha', 'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt',
                                        # -------------------------- b1 ---------------------------------
                                        'b1_dbg_min', 'b1_dbg_max', 'b1_dbg_mean', 'b1_dbg_std',
                                        'b1_dbg_med', 'b1_dbg_p25', 'b1_dbg_p50', 'b1_dbg_p75', 'b1_dbg_p95',
                                        'b1_dbg_p99',
                                        # -------------------------- b2 ---------------------------------
                                        'b2_dbg_min', 'b2_dbg_max', 'b2_dbg_mean', 'b2_dbg_std',
                                        'b2_dbg_med', 'b2_dbg_p25', 'b2_dbg_p50', 'b2_dbg_p75', 'b2_dbg_p95',
                                        'b2_dbg_p99',
                                        # -------------------------- b3 ---------------------------------
                                        'b3_dbg_min', 'b3_dbg_max', 'b3_dbg_mean', 'b3_dbg_std',
                                        'b3_dbg_med', 'b3_dbg_p25', 'b3_dbg_p50', 'b3_dbg_p75', 'b3_dbg_p95',
                                        'b3_dbg_p99',
                                        # -------------------------- b4 ---------------------------------
                                        'b4_dbg_min', 'b4_dbg_max', 'b4_dbg_mean', 'b4_dbg_std',
                                        'b4_dbg_med', 'b4_dbg_p25', 'b4_dbg_p50', 'b4_dbg_p75', 'b4_dbg_p95',
                                        'b4_dbg_p99',
                                        # -------------------------- b5 ---------------------------------
                                        'b5_dbg_min', 'b5_dbg_max', 'b5_dbg_mean', 'b5_dbg_std',
                                        'b5_dbg_med', 'b5_dbg_p25', 'b5_dbg_p50', 'b5_dbg_p75', 'b5_dbg_p95',
                                        'b5_dbg_p99',
                                        # -------------------------- b6 ---------------------------------
                                        'b6_dbg_min', 'b6_dbg_max', 'b6_dbg_mean', 'b6_dbg_std',
                                        'b6_dbg_med', 'b6_dbg_p25', 'b6_dbg_p50', 'b6_dbg_p75', 'b6_dbg_p95',
                                        'b6_dbg_p99',
                                        'image_dt', 'direction', 'season', ]]
        #'image']]

        df_drop_dbg_reformat.rename(columns={"uid_x": "uid",
                                             "image_dt": "dbg_dt",
                                             "direction": "dbg_dir",
                                             "season": "dbg_seas"}, inplace=True)
    else:
        pass

    if len(dbi_list) > 0:
        print("DBI", len(dbi_list))
        print("DBI columns list: ", dbi_list[0].columns)
        merge_dbi_list, merge_dbi_dropna_list = seasonal_file_export(dbi_list, tile_export, tile_indv_export,
                                                                     biomass_df, "dbi", False)

        df = pd.concat(merge_dbi_list)
        df_drop = pd.concat(merge_dbi_dropna_list)
        # todo remove test outputs
        df.to_csv(r"C:\Users\robot\projects\biomass\collated_zonal_stats\sr_fc\df_dbi.csv")

        df_drop.to_csv(r"C:\Users\robot\projects\biomass\collated_zonal_stats\sr_fc\df_dbi_dropna.csv")
        print("df_drop: ", list(df_drop.columns))


        df_drop_dbi_reformat = df_drop[['uid_x', 'site_clean', 'date', 'lon_gda94', 'lat_gda94', 'geometry',
                                        'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha', 'bio_w_kg1ha',
                                        'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha', 'bio_agb_kg1ha',
                                        'c_l_kg1ha', 'c_t_kg1ha', 'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha',
                                        'c_s_kg1ha', 'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt',
                                        # -------------------------- b1 ---------------------------------
                                        'b1_dbi_min', 'b1_dbi_max', 'b1_dbi_mean', 'b1_dbi_std',
                                        'b1_dbi_med', 'b1_dbi_p25', 'b1_dbi_p50', 'b1_dbi_p75', 'b1_dbi_p95',
                                        'b1_dbi_p99',
                                        # -------------------------- b2 ---------------------------------
                                        'b2_dbi_min', 'b2_dbi_max', 'b2_dbi_mean', 'b2_dbi_std',
                                        'b2_dbi_med', 'b2_dbi_p25', 'b2_dbi_p50', 'b2_dbi_p75', 'b2_dbi_p95',
                                        'b2_dbi_p99',
                                        # -------------------------- b3 ---------------------------------
                                        'b3_dbi_min', 'b3_dbi_max', 'b3_dbi_mean', 'b3_dbi_std',
                                        'b3_dbi_med', 'b3_dbi_p25', 'b3_dbi_p50', 'b3_dbi_p75', 'b3_dbi_p95',
                                        'b3_dbi_p99',
                                        # -------------------------- b4 ---------------------------------
                                        'b4_dbi_min', 'b4_dbi_max', 'b4_dbi_mean', 'b4_dbi_std',
                                        'b4_dbi_med', 'b4_dbi_p25', 'b4_dbi_p50', 'b4_dbi_p75', 'b4_dbi_p95',
                                        'b4_dbi_p99',
                                        # -------------------------- b5 ---------------------------------
                                        'b5_dbi_min', 'b5_dbi_max', 'b5_dbi_mean', 'b5_dbi_std',
                                        'b5_dbi_med', 'b5_dbi_p25', 'b5_dbi_p50', 'b5_dbi_p75', 'b5_dbi_p95',
                                        'b5_dbi_p99',
                                        # -------------------------- b6 ---------------------------------
                                        'b6_dbi_min', 'b6_dbi_max', 'b6_dbi_mean', 'b6_dbi_std',
                                        'b6_dbi_med', 'b6_dbi_p25', 'b6_dbi_p50', 'b6_dbi_p75', 'b6_dbi_p95',
                                        'b6_dbi_p99',

                                        'image_dt', 'direction', 'season']]
        #, 'image']]

        df_drop_dbi_reformat.rename(columns={"uid_x": "uid",
                                             "image_dt": "dbi_dt",
                                             "direction": "dbi_dir",
                                             "season": "dbi_seas"}, inplace=True)

    else:
        pass

    if len(dp0_list) > 0:
        print("DPO", len(dp0_list))
        merge_dp0_list, merge_dp0_dropna_list = file_export(dp0_list, tile_export, tile_indv_export, biomass_df, "dp0",
                                                            False)

        df = pd.concat(merge_dp0_list)
        df_drop = pd.concat(merge_dp0_dropna_list)
        # todo remove test outputs
        df.to_csv(r"C:\Users\robot\projects\biomass\collated_zonal_stats\sr_fc\df_dp0.csv")
        df_drop.to_csv(r"C:\Users\robot\projects\biomass\collated_zonal_stats\sr_fc\df_dp0_dropna.csv")

        df_drop_dp0_reformat = df_drop[['uid_x', 'site_clean', 'date', 'lon_gda94', 'lat_gda94', 'geometry',
                                        'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha', 'bio_w_kg1ha',
                                        'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha', 'bio_agb_kg1ha',
                                        'c_l_kg1ha', 'c_t_kg1ha', 'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha',
                                        'c_s_kg1ha', 'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt',
                                        # -------------------------- b1 ---------------------------------
                                        'b1_dp0_min', 'b1_dp0_max', 'b1_dp0_mean', 'b1_dp0_std',
                                        'b1_dp0_med', 'b1_dp0_p25', 'b1_dp0_p50', 'b1_dp0_p75', 'b1_dp0_p95',
                                        'b1_dp0_p99',
                                        # -------------------------- b2 ---------------------------------
                                        'b2_dp0_min', 'b2_dp0_max', 'b2_dp0_mean', 'b2_dp0_std',
                                        'b2_dp0_med', 'b2_dp0_p25', 'b2_dp0_p50', 'b2_dp0_p75', 'b2_dp0_p95',
                                        'b2_dp0_p99',
                                        # -------------------------- b3 ---------------------------------
                                        'b3_dp0_min', 'b3_dp0_max', 'b3_dp0_mean', 'b3_dp0_std',
                                        'b3_dp0_med', 'b3_dp0_p25', 'b3_dp0_p50', 'b3_dp0_p75', 'b3_dp0_p95',
                                        'b3_dp0_p99',

                                        'image_dt', 'direction', 'season',]]
        #'image']]

        df_drop_dp0_reformat.rename(columns={"uid_x": "uid",
                                             "image_dt": "dp0_dt",
                                             "direction": "dp0_dir",
                                             "season": "dp0_seas"}, inplace=True)

    else:
        pass

    if len(dp1_list) > 0:
        print("DP1", len(dp1_list))
        merge_dp1_list, merge_dp1_dropna_list = seasonal_file_export(dp1_list, tile_export, tile_indv_export,
                                                                     biomass_df, "dp1", False)

        df = pd.concat(merge_dp1_list)
        df_drop = pd.concat(merge_dp1_dropna_list)

        # todo remove test outputs
        df.to_csv(r"C:\Users\robot\projects\biomass\collated_zonal_stats\sr_fc\df_dp1.csv")
        df_drop.to_csv(r"C:\Users\robot\projects\biomass\collated_zonal_stats\sr_fc\df_dp1_dropna.csv")

        df_drop_dp1_reformat = df_drop[['uid_x', 'site_clean', 'date', 'lon_gda94', 'lat_gda94', 'geometry',
                                        'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha', 'bio_w_kg1ha',
                                        'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha', 'bio_agb_kg1ha',
                                        'c_l_kg1ha', 'c_t_kg1ha', 'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha',
                                        'c_s_kg1ha', 'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt',
                                        # -------------------------- b1 ---------------------------------
                                        'b1_dp1_min', 'b1_dp1_max', 'b1_dp1_mean', 'b1_dp1_std',
                                        'b1_dp1_med', 'b1_dp1_p25', 'b1_dp1_p50', 'b1_dp1_p75', 'b1_dp1_p95',
                                        'b1_dp1_p99',
                                        # -------------------------- b2 ---------------------------------
                                        'b2_dp1_min', 'b2_dp1_max', 'b2_dp1_mean', 'b2_dp1_std',
                                        'b2_dp1_med', 'b2_dp1_p25', 'b2_dp1_p50', 'b2_dp1_p75', 'b2_dp1_p95',
                                        'b2_dp1_p99',
                                        # -------------------------- b3 ---------------------------------
                                        'b3_dp1_min', 'b3_dp1_max', 'b3_dp1_mean', 'b3_dp1_std',
                                        'b3_dp1_med', 'b3_dp1_p25', 'b3_dp1_p50', 'b3_dp1_p75', 'b3_dp1_p95',
                                        'b3_dp1_p99',

                                        'image_dt', 'direction', 'season',]]
        #'image']]

        df_drop_dp1_reformat.rename(columns={"uid_x": "uid",
                                             "image_dt": "dp1_dt",
                                             "direction": "dp1_dir",
                                             "season": "dp1_seas"}, inplace=True)
    else:
        print("There are no dp1 records, goodbye....")
        pass

    # --------------------------------------- Fire mask ------------------------------------------

    if len(dbg_mask_list) > 0:
        print("DBG MASK", len(dbg_mask_list))
        merge_dbg_mask_list, merge_dbg_mask_dropna_list = file_fm_export(dbg_mask_list, tile_export, tile_indv_export,
                                                                         biomass_df, "dbg", True)

        df = pd.concat(merge_dbg_mask_list)
        df_drop = pd.concat(merge_dbg_mask_dropna_list)

        # todo remove test outputs
        df.to_csv(r"C:\Users\robot\projects\biomass\collated_zonal_stats\sr_fc\df_dbg_mask.csv")
        df_drop.to_csv(r"C:\Users\robot\projects\biomass\collated_zonal_stats\sr_fc\df_dbg_mask_dropna.csv")

        df_drop_dbg_mask_reformat = df_drop[['uid_x', 'site_clean', 'date', 'lon_gda94', 'lat_gda94', 'geometry',
                                             'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha', 'bio_w_kg1ha',
                                             'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha', 'bio_agb_kg1ha',
                                             'c_l_kg1ha', 'c_t_kg1ha', 'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha',
                                             'c_s_kg1ha', 'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt',
                                             # -------------------------- b1 ---------------------------------
                                             'b1_dbgfm_min', 'b1_dbgfm_max', 'b1_dbgfm_mean', 'b1_dbgfm_std',
                                             'b1_dbgfm_med', 'b1_dbgfm_p25', 'b1_dbgfm_p50', 'b1_dbgfm_p75',
                                             'b1_dbgfm_p95',
                                             'b1_dbgfm_p99',
                                             # -------------------------- b2 ---------------------------------
                                             'b2_dbgfm_min', 'b2_dbgfm_max', 'b2_dbgfm_mean', 'b2_dbgfm_std',
                                             'b2_dbgfm_med', 'b2_dbgfm_p25', 'b2_dbgfm_p50', 'b2_dbgfm_p75',
                                             'b2_dbgfm_p95',
                                             'b2_dbgfm_p99',
                                             # -------------------------- b3 ---------------------------------
                                             'b3_dbgfm_min', 'b3_dbgfm_max', 'b3_dbgfm_mean', 'b3_dbgfm_std',
                                             'b3_dbgfm_med', 'b3_dbgfm_p25', 'b3_dbgfm_p50', 'b3_dbgfm_p75',
                                             'b3_dbgfm_p95',
                                             'b3_dbgfm_p99',
                                             # -------------------------- b4 ---------------------------------
                                             'b4_dbgfm_min', 'b4_dbgfm_max', 'b4_dbgfm_mean', 'b4_dbgfm_std',
                                             'b4_dbgfm_med', 'b4_dbgfm_p25', 'b4_dbgfm_p50', 'b4_dbgfm_p75',
                                             'b4_dbgfm_p95',
                                             'b4_dbgfm_p99',
                                             # -------------------------- b5 ---------------------------------
                                             'b5_dbgfm_min', 'b5_dbgfm_max', 'b5_dbgfm_mean', 'b5_dbgfm_std',
                                             'b5_dbgfm_med', 'b5_dbgfm_p25', 'b5_dbgfm_p50', 'b5_dbgfm_p75',
                                             'b5_dbgfm_p95',
                                             'b5_dbgfm_p99',
                                             # -------------------------- b6 ---------------------------------
                                             'b6_dbgfm_min', 'b6_dbgfm_max', 'b6_dbgfm_mean', 'b6_dbgfm_std',
                                             'b6_dbgfm_med', 'b6_dbgfm_p25', 'b6_dbgfm_p50', 'b6_dbgfm_p75',
                                             'b6_dbgfm_p95',
                                             'b6_dbgfm_p99',

                                             'image_dt', 'direction', 'season',]]
        #'image']]

        df_drop_dbg_mask_reformat.rename(columns={"uid_x": "uid",
                                                  "image_dt": "dbgfm_dt",
                                                  "direction": "dbgfm_dir",
                                                  "season": "dbgfm_seas"}, inplace=True)

    else:
        pass

    if len(dbi_mask_list) > 0:
        print("DBI MASK", len(dbi_mask_list))
        merge_dbi_mask_list, merge_dbi_mask_dropna_list = seasonal_file_fm_export(dbi_mask_list, tile_export,
                                                                                  tile_indv_export,
                                                                                  biomass_df, "dbi", True)
        df = pd.concat(merge_dbi_mask_list)
        df_drop = pd.concat(merge_dbi_mask_dropna_list)

        # todo remove test outputs
        df.to_csv(r"C:\Users\robot\projects\biomass\collated_zonal_stats\sr_fc\df_dbi_mask.csv")
        df_drop.to_csv(r"C:\Users\robot\projects\biomass\collated_zonal_stats\sr_fc\df_dbi_mask_dropna.csv")

        df_drop_dbi_mask_reformat = df_drop[['uid_x', 'site_clean', 'date', 'lon_gda94', 'lat_gda94', 'geometry',
                                             'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha', 'bio_w_kg1ha',
                                             'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha', 'bio_agb_kg1ha',
                                             'c_l_kg1ha', 'c_t_kg1ha', 'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha',
                                             'c_s_kg1ha', 'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt',
                                             # -------------------------- b1 ---------------------------------
                                             'b1_dbifm_min', 'b1_dbifm_max', 'b1_dbifm_mean', 'b1_dbifm_std',
                                             'b1_dbifm_med', 'b1_dbifm_p25', 'b1_dbifm_p50', 'b1_dbifm_p75',
                                             'b1_dbifm_p95',
                                             'b1_dbifm_p99',
                                             # -------------------------- b2 ---------------------------------
                                             'b2_dbifm_min', 'b2_dbifm_max', 'b2_dbifm_mean', 'b2_dbifm_std',
                                             'b2_dbifm_med', 'b2_dbifm_p25', 'b2_dbifm_p50', 'b2_dbifm_p75',
                                             'b2_dbifm_p95',
                                             'b2_dbifm_p99',
                                             # -------------------------- b3 ---------------------------------
                                             'b3_dbifm_min', 'b3_dbifm_max', 'b3_dbifm_mean', 'b3_dbifm_std',
                                             'b3_dbifm_med', 'b3_dbifm_p25', 'b3_dbifm_p50', 'b3_dbifm_p75',
                                             'b3_dbifm_p95',
                                             'b3_dbifm_p99',
                                             # -------------------------- b4 ---------------------------------
                                             'b4_dbifm_min', 'b4_dbifm_max', 'b4_dbifm_mean', 'b4_dbifm_std',
                                             'b4_dbifm_med', 'b4_dbifm_p25', 'b4_dbifm_p50', 'b4_dbifm_p75',
                                             'b4_dbifm_p95',
                                             'b4_dbifm_p99',
                                             # -------------------------- b5 ---------------------------------
                                             'b5_dbifm_min', 'b5_dbifm_max', 'b5_dbifm_mean', 'b5_dbifm_std',
                                             'b5_dbifm_med', 'b5_dbifm_p25', 'b5_dbifm_p50', 'b5_dbifm_p75',
                                             'b5_dbifm_p95',
                                             'b5_dbifm_p99',
                                             # -------------------------- b6 ---------------------------------
                                             'b6_dbifm_min', 'b6_dbifm_max', 'b6_dbifm_mean', 'b6_dbifm_std',
                                             'b6_dbifm_med', 'b6_dbifm_p25', 'b6_dbifm_p50', 'b6_dbifm_p75',
                                             'b6_dbifm_p95',
                                             'b6_dbifm_p99',

                                             'image_dt', 'direction', 'season',]]
        #'image']]

        df_drop_dbi_mask_reformat.rename(columns={"uid_x": "uid",
                                                  "image_dt": "dbifm_dt",
                                                  "direction": "dbifm_dir",
                                                  "season": "dbifm_seas"}, inplace=True)

    else:
        pass

    if len(dp0_mask_list) > 0:
        print("DP0 MASK", len(dp0_mask_list))
        merge_dp0_mask_list, merge_dp0_mask_dropna_list = file_fm_export(dp0_mask_list, tile_export, tile_indv_export,
                                                                         biomass_df, "dp0", True)

        df = pd.concat(merge_dp0_mask_list)
        df_drop = pd.concat(merge_dp0_mask_dropna_list)

        # todo remove test outputs
        df.to_csv(r"C:\Users\robot\projects\biomass\collated_zonal_stats\sr_fc\df_dp0_mask.csv")
        df_drop.to_csv(r"C:\Users\robot\projects\biomass\collated_zonal_stats\sr_fc\df_dp0_mask_dropna.csv")

        df_drop_dp0_mask_reformat = df_drop[['uid_x', 'site_clean', 'date', 'lon_gda94', 'lat_gda94', 'geometry',
                                             'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha', 'bio_w_kg1ha',
                                             'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha', 'bio_agb_kg1ha',
                                             'c_l_kg1ha', 'c_t_kg1ha', 'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha',
                                             'c_s_kg1ha', 'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt',
                                             # -------------------------- b1 ---------------------------------
                                             'b1_dp0fm_min', 'b1_dp0fm_max', 'b1_dp0fm_mean', 'b1_dp0fm_std',
                                             'b1_dp0fm_med', 'b1_dp0fm_p25', 'b1_dp0fm_p50', 'b1_dp0fm_p75',
                                             'b1_dp0fm_p95',
                                             'b1_dp0fm_p99',
                                             # -------------------------- b2 ---------------------------------
                                             'b2_dp0fm_min', 'b2_dp0fm_max', 'b2_dp0fm_mean', 'b2_dp0fm_std',
                                             'b2_dp0fm_med', 'b2_dp0fm_p25', 'b2_dp0fm_p50', 'b2_dp0fm_p75',
                                             'b2_dp0fm_p95',
                                             'b2_dp0fm_p99',
                                             # -------------------------- b3 ---------------------------------
                                             'b3_dp0fm_min', 'b3_dp0fm_max', 'b3_dp0fm_mean', 'b3_dp0fm_std',
                                             'b3_dp0fm_med', 'b3_dp0fm_p25', 'b3_dp0fm_p50', 'b3_dp0fm_p75',
                                             'b3_dp0fm_p95',
                                             'b3_dp0fm_p99',

                                             'image_dt', 'direction', 'season',]]
        #'image']]

        df_drop_dp0_mask_reformat.rename(columns={"uid_x": "uid",
                                                  "image_dt": "dp0fm_dt",
                                                  "direction": "dp0fm_dir",
                                                  "season": "dp0fm_seas"}, inplace=True)



    else:
        pass

    if len(dp1_mask_list) > 0:
        print("DP1 MASK", len(dp1_mask_list))
        merge_dp1_mask_list, merge_dp1_mask_dropna_list = seasonal_file_fm_export(dp1_mask_list, tile_export,
                                                                                  tile_indv_export, biomass_df, "dp1",
                                                                                  True)
        df = pd.concat(merge_dp1_mask_list)
        df_drop = pd.concat(merge_dp1_mask_dropna_list)

        # todo remove test outputs
        df.to_csv(r"C:\Users\robot\projects\biomass\collated_zonal_stats\sr_fc\df_dp1_mask.csv")
        df_drop.to_csv(r"C:\Users\robot\projects\biomass\collated_zonal_stats\sr_fc\df_dp1_mask_dropna.csv")

        df_drop_dp1_mask_reformat = df_drop[['uid_x', 'site_clean', 'date', 'lon_gda94', 'lat_gda94', 'geometry',
                                             'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha', 'bio_w_kg1ha',
                                             'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha', 'bio_agb_kg1ha',
                                             'c_l_kg1ha', 'c_t_kg1ha', 'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha',
                                             'c_s_kg1ha', 'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt',
                                             # -------------------------- b1 ---------------------------------
                                             'b1_dp1fm_min', 'b1_dp1fm_max', 'b1_dp1fm_mean', 'b1_dp1fm_std',
                                             'b1_dp1fm_med', 'b1_dp1fm_p25', 'b1_dp1fm_p50', 'b1_dp1fm_p75',
                                             'b1_dp1fm_p95',
                                             'b1_dp1fm_p99',
                                             # -------------------------- b2 ---------------------------------
                                             'b2_dp1fm_min', 'b2_dp1fm_max', 'b2_dp1fm_mean', 'b2_dp1fm_std',
                                             'b2_dp1fm_med', 'b2_dp1fm_p25', 'b2_dp1fm_p50', 'b2_dp1fm_p75',
                                             'b2_dp1fm_p95',
                                             'b2_dp1fm_p99',
                                             # -------------------------- b3 ---------------------------------
                                             'b3_dp1fm_min', 'b3_dp1fm_max', 'b3_dp1fm_mean', 'b3_dp1fm_std',
                                             'b3_dp1fm_med', 'b3_dp1fm_p25', 'b3_dp1fm_p50', 'b3_dp1fm_p75',
                                             'b3_dp1fm_p95',
                                             'b3_dp1fm_p99',

                                             'image_dt', 'direction', 'season',]]
        #'image']]

        df_drop_dp1_mask_reformat.rename(columns={"uid_x": "uid",
                                                  "image_dt": "dp1fm_dt",
                                                  "direction": "dp1fm_dir",
                                                  "season": "dp1fm_seas"}, inplace=True)

    else:
        print("There are no dp1 mask records, goodbye....")
        pass


    # =========================================== Single ===============================================================

    df_drop_dbg_reformat.to_csv(r"C:\Users\robot\projects\outputs\scratch\test.csv")
    print("2 - 1111")
    # import sys
    # sys.exit()


    dbg_single = df_drop_dbg_reformat[df_drop_dbg_reformat["dbg_seas"] == "single"]
    dbg_mask_single = df_drop_dbg_mask_reformat[df_drop_dbg_mask_reformat["dbgfm_seas"] == "single"]

    # ----------------------------------------- dbg single with without mask-------------------------------------------

    dbg_both = pd.merge(right=dbg_single, left=dbg_mask_single, on=['uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94', 'geometry',
                                                                    'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                                                                    'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha',
                                                                    'bio_r_kg1ha',
                                                                    'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
                                                                    'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha',
                                                                    'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'])

    # todo remove test outputs
    #dbg_both.to_csv(r"C:\Users\robot\projects\biomass\collated_zonal_stats\sr_fc\dbg_both.csv", index=False)

    dp0_single = df_drop_dp0_reformat[df_drop_dp0_reformat["dp0_seas"] == "single"]
    dp0_mask_single = df_drop_dp0_mask_reformat[df_drop_dp0_mask_reformat["dp0fm_seas"] == "single"]


    # ----------------------------------------- dp0 single with without mask-------------------------------------------

    dp0_both = pd.merge(right=dp0_single, left=dp0_mask_single, on=['uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94', 'geometry',
                                                                    'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                                                                    'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha',
                                                                    'bio_r_kg1ha',
                                                                    'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
                                                                    'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha',
                                                                    'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'])

    # todo remove test outputs
    dp0_both.to_csv(r"C:\Users\robot\projects\biomass\collated_zonal_stats\sr_fc\dp0_both.csv", index=False)

    # ============================================= Seasonal ===========================================================

    dbi_dry = df_drop_dbi_reformat[df_drop_dbi_reformat["dbi_seas"] == "dry"]
    dbi_annual = df_drop_dbi_reformat[df_drop_dbi_reformat["dbi_seas"] == "annual"]

    dbi_dry.rename(columns=dict(  # ----------------- b1 -------------------------
        b1_dbi_min='b1_dbi_dry_min', b1_dbi_max='b1_dbi_dry_max', b1_dbi_mean='b1_dbi_dry_mean',
        b1_dbi_std='b1_dbi_dry_std', b1_dbi_med='b1_dbi_dry_med', b1_dbi_p25='b1_dbi_dry_p25',
        b1_dbi_p50='b1_dbi_dry_p50', b1_dbi_p75='b1_dbi_dry_p75', b1_dbi_p95='b1_dbi_dry_p95',
        b1_dbi_p99='b1_dbi_dry_p99',

        # ----------------- b2 -------------------------

        b2_dbi_min='b2_dbi_dry_min', b2_dbi_max='b2_dbi_dry_max', b2_dbi_mean='b2_dbi_dry_mean',
        b2_dbi_std='b2_dbi_dry_std', b2_dbi_med='b2_dbi_dry_med', b2_dbi_p25='b2_dbi_dry_p25',
        b2_dbi_p50='b2_dbi_dry_p50', b2_dbi_p75='b2_dbi_dry_p75', b2_dbi_p95='b2_dbi_dry_p95',
        b2_dbi_p99='b2_dbi_dry_p99',

        # ----------------- b3 -------------------------

        b3_dbi_min='b3_dbi_dry_min', b3_dbi_max='b3_dbi_dry_max', b3_dbi_mean='b3_dbi_dry_mean',
        b3_dbi_std='b3_dbi_dry_std', b3_dbi_med='b3_dbi_dry_med', b3_dbi_p25='b3_dbi_dry_p25',
        b3_dbi_p50='b3_dbi_dry_p50', b3_dbi_p75='b3_dbi_dry_p75', b3_dbi_p95='b3_dbi_dry_p95',
        b3_dbi_p99='b3_dbi_dry_p99',

        # ----------------- b4 -------------------------

        b4_dbi_min='b4_dbi_dry_min', b4_dbi_max='b4_dbi_dry_max', b4_dbi_mean='b4_dbi_dry_mean',
        b4_dbi_std='b4_dbi_dry_std', b4_dbi_med='b4_dbi_dry_med', b4_dbi_p25='b4_dbi_dry_p25',
        b4_dbi_p50='b4_dbi_dry_p50', b4_dbi_p75='b4_dbi_dry_p75', b4_dbi_p95='b4_dbi_dry_p95',
        b4_dbi_p99='b4_dbi_dry_p99',

        # ----------------- b5 -------------------------

        b5_dbi_min='b5_dbi_dry_min', b5_dbi_max='b5_dbi_dry_max', b5_dbi_mean='b5_dbi_dry_mean',
        b5_dbi_std='b5_dbi_dry_std', b5_dbi_med='b5_dbi_dry_med', b5_dbi_p25='b5_dbi_dry_p25',
        b5_dbi_p50='b5_dbi_dry_p50', b5_dbi_p75='b5_dbi_dry_p75', b5_dbi_p95='b5_dbi_dry_p95',
        b5_dbi_p99='b5_dbi_dry_p99',

        # ----------------- b6 -------------------------

        b6_dbi_min='b6_dbi_dry_min', b6_dbi_max='b6_dbi_dry_max', b6_dbi_mean='b6_dbi_dry_mean',
        b6_dbi_std='b6_dbi_dry_std', b6_dbi_med='b6_dbi_dry_med', b6_dbi_p25='b6_dbi_dry_p25',
        b6_dbi_p50='b6_dbi_dry_p50', b6_dbi_p75='b6_dbi_dry_p75', b6_dbi_p95='b6_dbi_dry_p95',
        b6_dbi_p99='b6_dbi_dry_p99',

        dbi_dt='dbi_dry_dt', dbi_dir='dbi_dry_dir',
        dbi_seas='dbi_dry_seas'), inplace=True)

    dbi_dry.to_csv(r"C:\Users\robot\projects\biomass\collated_zonal_stats\dry\dbi_dry.csv", index=False)
    dbi_annual.to_csv(r"C:\Users\robot\projects\biomass\collated_zonal_stats\annual\dbi_annual.csv", index=False)

    # ----------------------------------------- dbi both season NO mask -------------------------------------------

    dbi_both_season = pd.merge(right=dbi_dry, left=dbi_annual, on=['uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94', 'geometry',
                                                                   'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                                                                   'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha',
                                                                   'bio_r_kg1ha',
                                                                   'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
                                                                   'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha',
                                                                   'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'])

    # todo remove test outputs
    dbi_both_season.to_csv(r"C:\Users\robot\projects\biomass\collated_zonal_stats\sr_fc\dbi_both_season.csv", index=False)

    # ==================================================================================================================

    dbi_mask_dry = df_drop_dbi_mask_reformat[df_drop_dbi_mask_reformat["dbifm_seas"] == "dry"]
    dbi_mask_annual = df_drop_dbi_mask_reformat[df_drop_dbi_mask_reformat["dbifm_seas"] == "annual"]

    dbi_mask_dry.rename(columns=dict(  # ----------------- b1 -------------------------
        b1_dbifm_min='b1_dbifm_dry_min', b1_dbifm_max='b1_dbifm_dry_max', b1_dbifm_mean='b1_dbifm_dry_mean',
        b1_dbifm_std='b1_dbifm_dry_std', b1_dbifm_med='b1_dbifm_dry_med', b1_dbifm_p25='b1_dbifm_dry_p25',
        b1_dbifm_p50='b1_dbifm_dry_p50', b1_dbifm_p75='b1_dbifm_dry_p75', b1_dbifm_p95='b1_dbifm_dry_p95',
        b1_dbifm_p99='b1_dbifm_dry_p99',

        # ----------------- b2 -------------------------

        b2_dbifm_min='b2_dbifm_dry_min', b2_dbifm_max='b2_dbifm_dry_max', b2_dbifm_mean='b2_dbifm_dry_mean',
        b2_dbifm_std='b2_dbifm_dry_std', b2_dbifm_med='b2_dbifm_dry_med', b2_dbifm_p25='b2_dbifm_dry_p25',
        b2_dbifm_p50='b2_dbifm_dry_p50', b2_dbifm_p75='b2_dbifm_dry_p75', b2_dbifm_p95='b2_dbifm_dry_p95',
        b2_dbifm_p99='b2_dbifm_dry_p99',

        # ----------------- b3 -------------------------

        b3_dbifm_min='b3_dbifm_dry_min', b3_dbifm_max='b3_dbifm_dry_max', b3_dbifm_mean='b3_dbifm_dry_mean',
        b3_dbifm_std='b3_dbifm_dry_std', b3_dbifm_med='b3_dbifm_dry_med', b3_dbifm_p25='b3_dbifm_dry_p25',
        b3_dbifm_p50='b3_dbifm_dry_p50', b3_dbifm_p75='b3_dbifm_dry_p75', b3_dbifm_p95='b3_dbifm_dry_p95',
        b3_dbifm_p99='b3_dbifm_dry_p99',

        # ----------------- b4 -------------------------

        b4_dbifm_min='b4_dbifm_dry_min', b4_dbifm_max='b4_dbifm_dry_max', b4_dbifm_mean='b4_dbifm_dry_mean',
        b4_dbifm_std='b4_dbifm_dry_std', b4_dbifm_med='b4_dbifm_dry_med', b4_dbifm_p25='b4_dbifm_dry_p25',
        b4_dbifm_p50='b4_dbifm_dry_p50', b4_dbifm_p75='b4_dbifm_dry_p75', b4_dbifm_p95='b4_dbifm_dry_p95',
        b4_dbifm_p99='b4_dbifm_dry_p99',

        # ----------------- b5 -------------------------

        b5_dbifm_min='b5_dbifm_dry_min', b5_dbifm_max='b5_dbifm_dry_max', b5_dbifm_mean='b5_dbifm_dry_mean',
        b5_dbifm_std='b5_dbifm_dry_std', b5_dbifm_med='b5_dbifm_dry_med', b5_dbifm_p25='b5_dbifm_dry_p25',
        b5_dbifm_p50='b5_dbifm_dry_p50', b5_dbifm_p75='b5_dbifm_dry_p75', b5_dbifm_p95='b5_dbifm_dry_p95',
        b5_dbifm_p99='b5_dbifm_dry_p99',

        # ----------------- b6 -------------------------

        b6_dbifm_min='b6_dbifm_dry_min', b6_dbifm_max='b6_dbifm_dry_max', b6_dbifm_mean='b6_dbifm_dry_mean',
        b6_dbifm_std='b6_dbifm_dry_std', b6_dbifm_med='b6_dbifm_dry_med', b6_dbifm_p25='b6_dbifm_dry_p25',
        b6_dbifm_p50='b6_dbifm_dry_p50', b6_dbifm_p75='b6_dbifm_dry_p75', b6_dbifm_p95='b6_dbifm_dry_p95',
        b6_dbifm_p99='b6_dbifm_dry_p99',

        dbifm_dt='dbifm_dry_dt', dbifm_dir='dbifm_dry_dir',
        dbifm_seas='dbifm_dry_seas'), inplace=True)

    dbi_mask_dry.to_csv(r"C:\Users\robot\projects\biomass\collated_zonal_stats\dry_mask\dbi_mask_dry.csv", index=False)
    dbi_mask_annual.to_csv(r"C:\Users\robot\projects\biomass\collated_zonal_stats\annual_mask\dbi_mask_annual.csv", index=False)

    # ----------------------------------------- dbi both season with mask -------------------------------------------

    dbi_mask_both_season = pd.merge(right=dbi_mask_dry, left=dbi_mask_annual,
                                    on=['uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94', 'geometry',
                                        'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                                        'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha',
                                        'bio_r_kg1ha',
                                        'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
                                        'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha',
                                        'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'])

    # todo remove test outputs
    dbi_mask_both_season.to_csv(r"C:\Users\robot\projects\biomass\collated_zonal_stats\sr_fc\dbi_mask_both_season.csv", index=False)

    # ==================================================================================================================

    dp1_dry = df_drop_dp1_reformat[df_drop_dp1_reformat["dp1_seas"] == "dry"]
    dp1_annual = df_drop_dp1_reformat[df_drop_dp1_reformat["dp1_seas"] == "annual"]

    dp1_dry.rename(columns=dict(  # ----------------- b1 -------------------------
        b1_dp1_min='b1_dp1_dry_min', b1_dp1_max='b1_dp1_dry_max', b1_dp1_mean='b1_dp1_dry_mean',
        b1_dp1_std='b1_dp1_dry_std', b1_dp1_med='b1_dp1_dry_med', b1_dp1_p25='b1_dp1_dry_p25',
        b1_dp1_p50='b1_dp1_dry_p50', b1_dp1_p75='b1_dp1_dry_p75', b1_dp1_p95='b1_dp1_dry_p95',
        b1_dp1_p99='b1_dp1_dry_p99',

        # ----------------- b2 -------------------------

        b2_dp1_min='b2_dp1_dry_min', b2_dp1_max='b2_dp1_dry_max', b2_dp1_mean='b2_dp1_dry_mean',
        b2_dp1_std='b2_dp1_dry_std', b2_dp1_med='b2_dp1_dry_med', b2_dp1_p25='b2_dp1_dry_p25',
        b2_dp1_p50='b2_dp1_dry_p50', b2_dp1_p75='b2_dp1_dry_p75', b2_dp1_p95='b2_dp1_dry_p95',
        b2_dp1_p99='b2_dp1_dry_p99',

        # ----------------- b3 -------------------------

        b3_dp1_min='b3_dp1_dry_min', b3_dp1_max='b3_dp1_dry_max', b3_dp1_mean='b3_dp1_dry_mean',
        b3_dp1_std='b3_dp1_dry_std', b3_dp1_med='b3_dp1_dry_med', b3_dp1_p25='b3_dp1_dry_p25',
        b3_dp1_p50='b3_dp1_dry_p50', b3_dp1_p75='b3_dp1_dry_p75', b3_dp1_p95='b3_dp1_dry_p95',
        b3_dp1_p99='b3_dp1_dry_p99',

        dp1_dt='dp1_dry_dt', dp1_dir='dp1_dry_dir',
        dp1_seas='dp1_dry_seas'), inplace=True)

    dp1_dry.to_csv(r"C:\Users\robot\projects\biomass\collated_zonal_stats\dry\dp1_dry.csv", index=False)
    dp1_annual.to_csv(r"C:\Users\robot\projects\biomass\collated_zonal_stats\annual\dp1_annual.csv", index=False)

    # ----------------------------------------- dp1 both season NO mask -------------------------------------------

    dp1_both_season = pd.merge(right=dp1_dry, left=dp1_annual,
                               on=['uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94', 'geometry',
                                   'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                                   'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha',
                                   'bio_r_kg1ha',
                                   'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
                                   'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha',
                                   'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'])

    # todo remove test outputs
    dp1_both_season.to_csv(r"C:\Users\robot\projects\biomass\collated_zonal_stats\sr_fc\dp1_both_season.csv", index=False)

    # ==================================================================================================================

    dp1_mask_dry = df_drop_dp1_mask_reformat[df_drop_dp1_mask_reformat["dp1fm_seas"] == "dry"]
    dp1_mask_annual = df_drop_dp1_mask_reformat[df_drop_dp1_mask_reformat["dp1fm_seas"] == "annual"]

    dp1_mask_dry.rename(columns=dict(  # ----------------- b1 -------------------------
        b1_dp1fm_min='b1_dp1fm_dry_min', b1_dp1fm_max='b1_dp1fm_dry_max', b1_dp1fm_mean='b1_dp1fm_dry_mean',
        b1_dp1fm_std='b1_dp1fm_dry_std', b1_dp1fm_med='b1_dp1fm_dry_med', b1_dp1fm_p25='b1_dp1fm_dry_p25',
        b1_dp1fm_p50='b1_dp1fm_dry_p50', b1_dp1fm_p75='b1_dp1fm_dry_p75', b1_dp1fm_p95='b1_dp1fm_dry_p95',
        b1_dp1fm_p99='b1_dp1fm_dry_p99',

        # ----------------- b2 -------------------------

        b2_dp1fm_min='b2_dp1fm_dry_min', b2_dp1fm_max='b2_dp1fm_dry_max', b2_dp1fm_mean='b2_dp1fm_dry_mean',
        b2_dp1fm_std='b2_dp1fm_dry_std', b2_dp1fm_med='b2_dp1fm_dry_med', b2_dp1fm_p25='b2_dp1fm_dry_p25',
        b2_dp1fm_p50='b2_dp1fm_dry_p50', b2_dp1fm_p75='b2_dp1fm_dry_p75', b2_dp1fm_p95='b2_dp1fm_dry_p95',
        b2_dp1fm_p99='b2_dp1fm_dry_p99',

        # ----------------- b3 -------------------------

        b3_dp1fm_min='b3_dp1fm_dry_min', b3_dp1fm_max='b3_dp1fm_dry_max', b3_dp1fm_mean='b3_dp1fm_dry_mean',
        b3_dp1fm_std='b3_dp1fm_dry_std', b3_dp1fm_med='b3_dp1fm_dry_med', b3_dp1fm_p25='b3_dp1fm_dry_p25',
        b3_dp1fm_p50='b3_dp1fm_dry_p50', b3_dp1fm_p75='b3_dp1fm_dry_p75', b3_dp1fm_p95='b3_dp1fm_dry_p95',
        b3_dp1fm_p99='b3_dp1fm_dry_p99',

        dp1fm_dt='dp1fm_dry_dt', dp1fm_dir='dp1fm_dry_dir',
        dp1fm_seas='dp1fm_dry_seas'), inplace=True)

    dp1_mask_dry.to_csv(r"C:\Users\robot\projects\biomass\collated_zonal_stats\dry_mask\dp1_mask_dry.csv", index=False)
    dp1_mask_annual.to_csv(r"C:\Users\robot\projects\biomass\collated_zonal_stats\annual_mask\dp1_mask_annual.csv", index=False)

    # ----------------------------------------- dp1 both season with mask -------------------------------------------

    dp1_mask_both_season = pd.merge(right=dp1_mask_dry, left=dp1_mask_annual, how="outer",
                                    on=['uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94', 'geometry',
                                        'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                                        'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha',
                                        'bio_r_kg1ha',
                                        'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
                                        'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha',
                                        'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'])

    # todo remove test outputs
    dp1_mask_both_season.to_csv(r"C:\Users\robot\projects\biomass\collated_zonal_stats\sr_fc\dp1_mask_both_season.csv", index=False)

    # ================================================= MERGE ==========================================================

    # -------------------------------------------- DRY Season ----------------------------------

    dbi_dp1_dry = pd.merge(right=dbi_dry, left=dp1_dry, how="outer", on=['uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94',
                                                                         'geometry',
                                                                         'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                                                                         'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha',
                                                                         'bio_r_kg1ha',
                                                                         'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
                                                                         'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha',
                                                                         'c_s_kg1ha',
                                                                         'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'])

    dbi_dp1_dry.to_csv(r"C:\Users\robot\projects\biomass\collated_zonal_stats\dry\dbi_dp1_dry.csv", index=False)

    # -------------------------------------------- DRY Season Fire mask ----------------------------------

    dbi_dp1_mask_dry = pd.merge(right=dbi_mask_dry, left=dp1_mask_dry, how="outer",
                                on=['uid','site_clean', 'date', 'lon_gda94', 'lat_gda94',
                                    'geometry',
                                    'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                                    'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha',
                                    'bio_r_kg1ha',
                                    'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
                                    'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha',
                                    'c_s_kg1ha',
                                    'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'])

    dbi_dp1_mask_dry.to_csv(r"C:\Users\robot\projects\biomass\collated_zonal_stats\dry_mask\dbi_dp1_mask_dry.csv", index=False)

    # ==================================================================================================================

    # -------------------------------------------- Annual Season ----------------------------------

    dbi_dp1_annual = pd.merge(right=dbi_annual, left=dp1_annual, how="outer",
                              on=['uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94',
                                  'geometry',
                                  'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                                  'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha',
                                  'bio_r_kg1ha',
                                  'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
                                  'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha',
                                  'c_s_kg1ha',
                                  'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'])

    dbi_dp1_annual.to_csv(r"C:\Users\robot\projects\biomass\collated_zonal_stats\annual\dbi_dp1_annual.csv", index=False)

    # -------------------------------------------- DRY Season Fire mask ----------------------------------

    dbi_dp1_mask_annual = pd.merge(right=dbi_mask_annual, left=dp1_mask_annual, how="outer",
                                   on=['uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94',
                                       'geometry',
                                       'bio_l_kg1ha', 'bio_t_kg1ha',
                                       'bio_b_kg1ha',
                                       'bio_w_kg1ha', 'bio_br_kg1ha',
                                       'bio_s_kg1ha',
                                       'bio_r_kg1ha',
                                       'bio_agb_kg1ha', 'c_l_kg1ha',
                                       'c_t_kg1ha',
                                       'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha',
                                       'c_s_kg1ha',
                                       'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'])

    dbi_dp1_mask_annual.to_csv(r"C:\Users\robot\projects\biomass\collated_zonal_stats\annual_mask\dbi_dp1_mask_annual.csv", index=False)

    # ==================================================================================================================
    # ------------------------------------------------- Single date ----------------------------------------------------

    # -------------------------------------------- Single ----------------------------------

    dbg_dp0_single = pd.merge(right=df_drop_dbg_reformat, left=df_drop_dp0_reformat, how="outer",
                              on=['uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94',
                                  'geometry',
                                  'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                                  'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha',
                                  'bio_r_kg1ha',
                                  'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
                                  'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha',
                                  'c_s_kg1ha',
                                  'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'])

    dbg_dp0_single.to_csv(r"C:\Users\robot\projects\biomass\collated_zonal_stats\single\dbg_dp0_single.csv", index=False)

    # -------------------------------------------- Single mask ----------------------------------

    dbg_dp0_mask_single = pd.merge(right=df_drop_dbg_mask_reformat, left=df_drop_dp0_mask_reformat,
                                   how="outer", on=['uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94',
                                                    'geometry',
                                                    'bio_l_kg1ha', 'bio_t_kg1ha',
                                                    'bio_b_kg1ha',
                                                    'bio_w_kg1ha', 'bio_br_kg1ha',
                                                    'bio_s_kg1ha',
                                                    'bio_r_kg1ha',
                                                    'bio_agb_kg1ha', 'c_l_kg1ha',
                                                    'c_t_kg1ha',
                                                    'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha',
                                                    'c_s_kg1ha',
                                                    'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'])

    dbg_dp0_mask_single.to_csv(r"C:\Users\robot\projects\biomass\collated_zonal_stats\single_mask\dbg_dp0_mask_single.csv", index=False)

    # ==================================================================================================================
    # --------------------------------------- Merge SR ----------------------------------------

    dbg_dbi = pd.merge(right=dbg_both, left=dbi_both_season, how="outer", on=['uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94',
                                                                              'geometry',
                                                                              'bio_l_kg1ha', 'bio_t_kg1ha',
                                                                              'bio_b_kg1ha',
                                                                              'bio_w_kg1ha', 'bio_br_kg1ha',
                                                                              'bio_s_kg1ha',
                                                                              'bio_r_kg1ha',
                                                                              'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
                                                                              'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha',
                                                                              'c_s_kg1ha',
                                                                              'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'])

    dbg_dbi.to_csv(r"C:\Users\robot\projects\biomass\collated_zonal_stats\sr_fc\dbg_dbi.csv", index=False)

    dbg_dbi_dbi_mask = pd.merge(right=dbg_dbi, left=dbi_mask_both_season, how="outer",
                                on=['uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94', 'geometry',
                                    'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                                    'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha',
                                    'bio_r_kg1ha',
                                    'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
                                    'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha',
                                    'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'])

    # todo remove test outputs
    dbg_dbi_dbi_mask.to_csv(r"C:\Users\robot\projects\biomass\collated_zonal_stats\sr_fc\dbg_dbi_dbi_mask.csv", index=False)

    # --------------------------------------- Merge FC ----------------------------------------

    dp0_dp1 = pd.merge(right=dp0_both, left=dp1_both_season, how="outer",
                       on=['uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94', 'geometry',
                           'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                           'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha',
                           'bio_r_kg1ha',
                           'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
                           'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha',
                           'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'])

    # todo remove test outputs
    dp0_dp1.to_csv(r"C:\Users\robot\projects\biomass\collated_zonal_stats\sr_fc\dp0_dp1.csv", index=False)

    dp0_dp1_dp1_mask = pd.merge(right=dp0_dp1, left=dp1_mask_both_season, how="outer",
                                on=['uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94', 'geometry',
                                    'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                                    'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha',
                                    'bio_r_kg1ha',
                                    'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
                                    'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha',
                                    'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'])

    # todo remove test outputs
    dp0_dp1_dp1_mask.to_csv(r"C:\Users\robot\projects\biomass\collated_zonal_stats\sr_fc\dp0_dp1_dp1_mask.csv", index=False)

    # --------------------------------------- Merge SR and FC ----------------------------------------

    print(dbg_dbi_dbi_mask.columns.tolist())
    print(dp0_dp1_dp1_mask.columns.tolist())
    print("2 - 1531")
    # import sys
    # sys.exit()
    dbg_dbi_dbi_mask_dp0_dp1_dp1_mask = pd.merge(right=dbg_dbi_dbi_mask, left=dp0_dp1_dp1_mask, how="outer",
                                                 on=['uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94', 'geometry',
                                                     'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                                                     'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha',
                                                     'bio_r_kg1ha',
                                                     'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
                                                     'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha',
                                                     'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'])

    # todo remove test outputs
    dbg_dbi_dbi_mask_dp0_dp1_dp1_mask.to_csv(r"C:\Users\robot\projects\biomass\collated_zonal_stats\sr_fc\dbg_dbi_dbi_mask_dp0_dp1_dp1_mask.csv",
                                             index=False)

    print("final_list: ", list(dbg_dbi_dbi_mask_dp0_dp1_dp1_mask.columns))
    # print("2 - 1543")
    # import sys
    # sys.exit()

    dbg_dbi_dbi_mask_dp0_dp1_dp1_mask_clean = dbg_dbi_dbi_mask_dp0_dp1_dp1_mask[[
        'uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94', 'geometry',
        'bio_l_kg1ha', 'bio_t_kg1ha',
        'bio_b_kg1ha', 'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha', 'bio_agb_kg1ha', 'c_l_kg1ha',
        'c_t_kg1ha', 'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha', 'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt',
        # --------------------------------------- dp1 mask annual ----------------------------------
        'b1_dp1fm_min', 'b1_dp1fm_max', 'b1_dp1fm_mean', 'b1_dp1fm_std', 'b1_dp1fm_med', 'b1_dp1fm_p25', 'b1_dp1fm_p50',
        'b1_dp1fm_p75', 'b1_dp1fm_p95', 'b1_dp1fm_p99', 'b2_dp1fm_min', 'b2_dp1fm_max', 'b2_dp1fm_mean', 'b2_dp1fm_std',
        'b2_dp1fm_med', 'b2_dp1fm_p25', 'b2_dp1fm_p50', 'b2_dp1fm_p75', 'b2_dp1fm_p95',
        'b2_dp1fm_p99', 'b3_dp1fm_min', 'b3_dp1fm_max', 'b3_dp1fm_mean', 'b3_dp1fm_std',
        'b3_dp1fm_med', 'b3_dp1fm_p25', 'b3_dp1fm_p50', 'b3_dp1fm_p75', 'b3_dp1fm_p95',
        'b3_dp1fm_p99',

        # ---------------------------------------- dp1 mask dry --------------------------------
        'b1_dp1fm_dry_min', 'b1_dp1fm_dry_max', 'b1_dp1fm_dry_mean', 'b1_dp1fm_dry_std',
        'b1_dp1fm_dry_med', 'b1_dp1fm_dry_p25', 'b1_dp1fm_dry_p50', 'b1_dp1fm_dry_p75',
        'b1_dp1fm_dry_p95', 'b1_dp1fm_dry_p99', 'b2_dp1fm_dry_min', 'b2_dp1fm_dry_max',
        'b2_dp1fm_dry_mean', 'b2_dp1fm_dry_std', 'b2_dp1fm_dry_med', 'b2_dp1fm_dry_p25',
        'b2_dp1fm_dry_p50', 'b2_dp1fm_dry_p75', 'b2_dp1fm_dry_p95', 'b2_dp1fm_dry_p99',
        'b3_dp1fm_dry_min', 'b3_dp1fm_dry_max', 'b3_dp1fm_dry_mean', 'b3_dp1fm_dry_std',
        'b3_dp1fm_dry_med', 'b3_dp1fm_dry_p25', 'b3_dp1fm_dry_p50', 'b3_dp1fm_dry_p75',
        'b3_dp1fm_dry_p95', 'b3_dp1fm_dry_p99',

        # ------------------------------------------ dp1 annual -----------------------------------
        'b1_dp1_min', 'b1_dp1_max', 'b1_dp1_mean',
        'b1_dp1_std', 'b1_dp1_med', 'b1_dp1_p25', 'b1_dp1_p50', 'b1_dp1_p75',
        'b1_dp1_p95', 'b1_dp1_p99', 'b2_dp1_min', 'b2_dp1_max', 'b2_dp1_mean',
        'b2_dp1_std', 'b2_dp1_med', 'b2_dp1_p25', 'b2_dp1_p50', 'b2_dp1_p75',
        'b2_dp1_p95', 'b2_dp1_p99', 'b3_dp1_min', 'b3_dp1_max', 'b3_dp1_mean',
        'b3_dp1_std', 'b3_dp1_med', 'b3_dp1_p25', 'b3_dp1_p50', 'b3_dp1_p75',
        'b3_dp1_p95', 'b3_dp1_p99',

        # ---------------------- dp1 dry ---------------------
        'b1_dp1_dry_min', 'b1_dp1_dry_max', 'b1_dp1_dry_mean', 'b1_dp1_dry_std',
        'b1_dp1_dry_med', 'b1_dp1_dry_p25', 'b1_dp1_dry_p50', 'b1_dp1_dry_p75',
        'b1_dp1_dry_p95', 'b1_dp1_dry_p99', 'b2_dp1_dry_min', 'b2_dp1_dry_max',
        'b2_dp1_dry_mean', 'b2_dp1_dry_std', 'b2_dp1_dry_med', 'b2_dp1_dry_p25',
        'b2_dp1_dry_p50', 'b2_dp1_dry_p75', 'b2_dp1_dry_p95', 'b2_dp1_dry_p99',
        'b3_dp1_dry_min', 'b3_dp1_dry_max', 'b3_dp1_dry_mean', 'b3_dp1_dry_std',
        'b3_dp1_dry_med', 'b3_dp1_dry_p25', 'b3_dp1_dry_p50', 'b3_dp1_dry_p75',
        'b3_dp1_dry_p95', 'b3_dp1_dry_p99',

        # ------------------------ dp0 single date fire mask -----------------------

        'b1_dp0fm_min', 'b1_dp0fm_max', 'b1_dp0fm_mean',
        'b1_dp0fm_std', 'b1_dp0fm_med', 'b1_dp0fm_p25', 'b1_dp0fm_p50', 'b1_dp0fm_p75',
        'b1_dp0fm_p95', 'b1_dp0fm_p99', 'b2_dp0fm_min', 'b2_dp0fm_max', 'b2_dp0fm_mean',
        'b2_dp0fm_std', 'b2_dp0fm_med', 'b2_dp0fm_p25', 'b2_dp0fm_p50', 'b2_dp0fm_p75',
        'b2_dp0fm_p95', 'b2_dp0fm_p99', 'b3_dp0fm_min', 'b3_dp0fm_max', 'b3_dp0fm_mean',
        'b3_dp0fm_std', 'b3_dp0fm_med', 'b3_dp0fm_p25', 'b3_dp0fm_p50', 'b3_dp0fm_p75',
        'b3_dp0fm_p95', 'b3_dp0fm_p99',

        # -------------------------- dp0 single date ------------------------

        'b1_dp0_min', 'b1_dp0_max', 'b1_dp0_mean', 'b1_dp0_std',
        'b1_dp0_med', 'b1_dp0_p25', 'b1_dp0_p50', 'b1_dp0_p75', 'b1_dp0_p95',
        'b1_dp0_p99', 'b2_dp0_min', 'b2_dp0_max', 'b2_dp0_mean', 'b2_dp0_std',
        'b2_dp0_med', 'b2_dp0_p25', 'b2_dp0_p50', 'b2_dp0_p75', 'b2_dp0_p95',
        'b2_dp0_p99', 'b3_dp0_min', 'b3_dp0_max', 'b3_dp0_mean', 'b3_dp0_std',
        'b3_dp0_med', 'b3_dp0_p25', 'b3_dp0_p50', 'b3_dp0_p75', 'b3_dp0_p95',
        'b3_dp0_p99',

        # ------------------------- dbi fire mask ------------------------

        'b1_dbifm_min', 'b1_dbifm_max', 'b1_dbifm_mean', 'b1_dbifm_std', 'b1_dbifm_med', 'b1_dbifm_p25',
        'b1_dbifm_p50', 'b1_dbifm_p75', 'b1_dbifm_p95', 'b1_dbifm_p99', 'b2_dbifm_min',
        'b2_dbifm_max', 'b2_dbifm_mean', 'b2_dbifm_std', 'b2_dbifm_med', 'b2_dbifm_p25',
        'b2_dbifm_p50', 'b2_dbifm_p75', 'b2_dbifm_p95', 'b2_dbifm_p99', 'b3_dbifm_min',
        'b3_dbifm_max', 'b3_dbifm_mean', 'b3_dbifm_std', 'b3_dbifm_med', 'b3_dbifm_p25',
        'b3_dbifm_p50', 'b3_dbifm_p75', 'b3_dbifm_p95', 'b3_dbifm_p99', 'b4_dbifm_min',
        'b4_dbifm_max', 'b4_dbifm_mean', 'b4_dbifm_std', 'b4_dbifm_med', 'b4_dbifm_p25',
        'b4_dbifm_p50', 'b4_dbifm_p75', 'b4_dbifm_p95', 'b4_dbifm_p99', 'b5_dbifm_min',
        'b5_dbifm_max', 'b5_dbifm_mean', 'b5_dbifm_std', 'b5_dbifm_med', 'b5_dbifm_p25',
        'b5_dbifm_p50', 'b5_dbifm_p75', 'b5_dbifm_p95', 'b5_dbifm_p99', 'b6_dbifm_min',
        'b6_dbifm_max', 'b6_dbifm_mean', 'b6_dbifm_std', 'b6_dbifm_med', 'b6_dbifm_p25',
        'b6_dbifm_p50', 'b6_dbifm_p75', 'b6_dbifm_p95', 'b6_dbifm_p99',

        # ------------------------------ dbi dry fire mask ----------------------
        'b1_dbifm_dry_min', 'b1_dbifm_dry_max', 'b1_dbifm_dry_mean', 'b1_dbifm_dry_std', 'b1_dbifm_dry_med',
        'b1_dbifm_dry_p25', 'b1_dbifm_dry_p50', 'b1_dbifm_dry_p75', 'b1_dbifm_dry_p95',
        'b1_dbifm_dry_p99', 'b2_dbifm_dry_min', 'b2_dbifm_dry_max', 'b2_dbifm_dry_mean',
        'b2_dbifm_dry_std', 'b2_dbifm_dry_med', 'b2_dbifm_dry_p25', 'b2_dbifm_dry_p50',
        'b2_dbifm_dry_p75', 'b2_dbifm_dry_p95', 'b2_dbifm_dry_p99', 'b3_dbifm_dry_min',
        'b3_dbifm_dry_max', 'b3_dbifm_dry_mean', 'b3_dbifm_dry_std', 'b3_dbifm_dry_med',
        'b3_dbifm_dry_p25', 'b3_dbifm_dry_p50', 'b3_dbifm_dry_p75', 'b3_dbifm_dry_p95',
        'b3_dbifm_dry_p99', 'b4_dbifm_dry_min', 'b4_dbifm_dry_max', 'b4_dbifm_dry_mean',
        'b4_dbifm_dry_std', 'b4_dbifm_dry_med', 'b4_dbifm_dry_p25', 'b4_dbifm_dry_p50',
        'b4_dbifm_dry_p75', 'b4_dbifm_dry_p95', 'b4_dbifm_dry_p99', 'b5_dbifm_dry_min',
        'b5_dbifm_dry_max', 'b5_dbifm_dry_mean', 'b5_dbifm_dry_std', 'b5_dbifm_dry_med',
        'b5_dbifm_dry_p25', 'b5_dbifm_dry_p50', 'b5_dbifm_dry_p75', 'b5_dbifm_dry_p95',
        'b5_dbifm_dry_p99', 'b6_dbifm_dry_min', 'b6_dbifm_dry_max', 'b6_dbifm_dry_mean',
        'b6_dbifm_dry_std', 'b6_dbifm_dry_med', 'b6_dbifm_dry_p25', 'b6_dbifm_dry_p50',
        'b6_dbifm_dry_p75', 'b6_dbifm_dry_p95', 'b6_dbifm_dry_p99',

        # ---------------------------------- dbi annual fire mask ----------------------------
        'b1_dbi_min', 'b1_dbi_max',
        'b1_dbi_mean', 'b1_dbi_std', 'b1_dbi_med', 'b1_dbi_p25', 'b1_dbi_p50',
        'b1_dbi_p75', 'b1_dbi_p95', 'b1_dbi_p99', 'b2_dbi_min', 'b2_dbi_max',
        'b2_dbi_mean', 'b2_dbi_std', 'b2_dbi_med', 'b2_dbi_p25', 'b2_dbi_p50',
        'b2_dbi_p75', 'b2_dbi_p95', 'b2_dbi_p99', 'b3_dbi_min', 'b3_dbi_max',
        'b3_dbi_mean', 'b3_dbi_std', 'b3_dbi_med', 'b3_dbi_p25', 'b3_dbi_p50',
        'b3_dbi_p75', 'b3_dbi_p95', 'b3_dbi_p99', 'b4_dbi_min', 'b4_dbi_max',
        'b4_dbi_mean', 'b4_dbi_std', 'b4_dbi_med', 'b4_dbi_p25', 'b4_dbi_p50',
        'b4_dbi_p75', 'b4_dbi_p95', 'b4_dbi_p99', 'b5_dbi_min', 'b5_dbi_max',
        'b5_dbi_mean', 'b5_dbi_std', 'b5_dbi_med', 'b5_dbi_p25', 'b5_dbi_p50',
        'b5_dbi_p75', 'b5_dbi_p95', 'b5_dbi_p99', 'b6_dbi_min', 'b6_dbi_max',
        'b6_dbi_mean', 'b6_dbi_std', 'b6_dbi_med', 'b6_dbi_p25', 'b6_dbi_p50',
        'b6_dbi_p75', 'b6_dbi_p95', 'b6_dbi_p99',
        # ---------------------------- dbi dry -----------------------------

        'b1_dbi_dry_min', 'b1_dbi_dry_max', 'b1_dbi_dry_mean',
        'b1_dbi_dry_std', 'b1_dbi_dry_med', 'b1_dbi_dry_p25', 'b1_dbi_dry_p50',
        'b1_dbi_dry_p75', 'b1_dbi_dry_p95', 'b1_dbi_dry_p99', 'b2_dbi_dry_min',
        'b2_dbi_dry_max', 'b2_dbi_dry_mean', 'b2_dbi_dry_std', 'b2_dbi_dry_med',
        'b2_dbi_dry_p25', 'b2_dbi_dry_p50', 'b2_dbi_dry_p75', 'b2_dbi_dry_p95',
        'b2_dbi_dry_p99', 'b3_dbi_dry_min', 'b3_dbi_dry_max', 'b3_dbi_dry_mean',
        'b3_dbi_dry_std', 'b3_dbi_dry_med', 'b3_dbi_dry_p25', 'b3_dbi_dry_p50',
        'b3_dbi_dry_p75', 'b3_dbi_dry_p95', 'b3_dbi_dry_p99', 'b4_dbi_dry_min',
        'b4_dbi_dry_max', 'b4_dbi_dry_mean', 'b4_dbi_dry_std', 'b4_dbi_dry_med',
        'b4_dbi_dry_p25', 'b4_dbi_dry_p50', 'b4_dbi_dry_p75', 'b4_dbi_dry_p95',
        'b4_dbi_dry_p99', 'b5_dbi_dry_min', 'b5_dbi_dry_max', 'b5_dbi_dry_mean',
        'b5_dbi_dry_std', 'b5_dbi_dry_med', 'b5_dbi_dry_p25', 'b5_dbi_dry_p50',
        'b5_dbi_dry_p75', 'b5_dbi_dry_p95', 'b5_dbi_dry_p99', 'b6_dbi_dry_min',
        'b6_dbi_dry_max', 'b6_dbi_dry_mean', 'b6_dbi_dry_std', 'b6_dbi_dry_med',
        'b6_dbi_dry_p25', 'b6_dbi_dry_p50', 'b6_dbi_dry_p75', 'b6_dbi_dry_p95',
        'b6_dbi_dry_p99',

        # ---------------------------- dbg fire mask ---------------------------

        'b1_dbgfm_min', 'b1_dbgfm_max', 'b1_dbgfm_mean',
        'b1_dbgfm_std', 'b1_dbgfm_med', 'b1_dbgfm_p25', 'b1_dbgfm_p50', 'b1_dbgfm_p75',
        'b1_dbgfm_p95', 'b1_dbgfm_p99', 'b2_dbgfm_min', 'b2_dbgfm_max', 'b2_dbgfm_mean',
        'b2_dbgfm_std', 'b2_dbgfm_med', 'b2_dbgfm_p25', 'b2_dbgfm_p50', 'b2_dbgfm_p75',
        'b2_dbgfm_p95', 'b2_dbgfm_p99', 'b3_dbgfm_min', 'b3_dbgfm_max', 'b3_dbgfm_mean',
        'b3_dbgfm_std', 'b3_dbgfm_med', 'b3_dbgfm_p25', 'b3_dbgfm_p50', 'b3_dbgfm_p75',
        'b3_dbgfm_p95', 'b3_dbgfm_p99', 'b4_dbgfm_min', 'b4_dbgfm_max', 'b4_dbgfm_mean',
        'b4_dbgfm_std', 'b4_dbgfm_med', 'b4_dbgfm_p25', 'b4_dbgfm_p50', 'b4_dbgfm_p75',
        'b4_dbgfm_p95', 'b4_dbgfm_p99', 'b5_dbgfm_min', 'b5_dbgfm_max', 'b5_dbgfm_mean',
        'b5_dbgfm_std', 'b5_dbgfm_med', 'b5_dbgfm_p25', 'b5_dbgfm_p50', 'b5_dbgfm_p75',
        'b5_dbgfm_p95', 'b5_dbgfm_p99', 'b6_dbgfm_min', 'b6_dbgfm_max', 'b6_dbgfm_mean',
        'b6_dbgfm_std', 'b6_dbgfm_med', 'b6_dbgfm_p25', 'b6_dbgfm_p50', 'b6_dbgfm_p75',
        'b6_dbgfm_p95', 'b6_dbgfm_p99',

        # ------------------------------------ dbg single date  ---------------------------

        'b1_dbg_min', 'b1_dbg_max', 'b1_dbg_mean', 'b1_dbg_std',
        'b1_dbg_med', 'b1_dbg_p25', 'b1_dbg_p50', 'b1_dbg_p75', 'b1_dbg_p95',
        'b1_dbg_p99', 'b2_dbg_min', 'b2_dbg_max', 'b2_dbg_mean', 'b2_dbg_std',
        'b2_dbg_med', 'b2_dbg_p25', 'b2_dbg_p50', 'b2_dbg_p75', 'b2_dbg_p95',
        'b2_dbg_p99', 'b3_dbg_min', 'b3_dbg_max', 'b3_dbg_mean', 'b3_dbg_std',
        'b3_dbg_med', 'b3_dbg_p25', 'b3_dbg_p50', 'b3_dbg_p75', 'b3_dbg_p95',
        'b3_dbg_p99', 'b4_dbg_min', 'b4_dbg_max', 'b4_dbg_mean', 'b4_dbg_std',
        'b4_dbg_med', 'b4_dbg_p25', 'b4_dbg_p50', 'b4_dbg_p75', 'b4_dbg_p95',
        'b4_dbg_p99', 'b5_dbg_min', 'b5_dbg_max', 'b5_dbg_mean', 'b5_dbg_std',
        'b5_dbg_med', 'b5_dbg_p25', 'b5_dbg_p50', 'b5_dbg_p75', 'b5_dbg_p95',
        'b5_dbg_p99', 'b6_dbg_min', 'b6_dbg_max', 'b6_dbg_mean', 'b6_dbg_std',
        'b6_dbg_med', 'b6_dbg_p25', 'b6_dbg_p50', 'b6_dbg_p75', 'b6_dbg_p95',
        'b6_dbg_p99',
    ]]

    dbg_dbi_dbi_mask_dp0_dp1_dp1_mask_clean.rename(columns={'site_clean_x_x': 'site_clean'}, inplace=True)

    # todo remove test outputs
    dbg_dbi_dbi_mask_dp0_dp1_dp1_mask_clean.to_csv(
        r"C:\Users\robot\projects\biomass\collated_zonal_stats\sr_fc\dbg_dbi_dbi_mask_dp0_dp1_dp1_mask_clean.csv", index=False)
    biomass_df.to_csv(r"C:\Users\robot\projects\biomass\collated_zonal_stats\biomass_df.csv", index=False)

    return biomass_df, dbg_dbi_dbi_mask_dp0_dp1_dp1_mask, \
        dbg_dbi_dbi_mask_dp0_dp1_dp1_mask_clean, \
        dbi_dp1_dry, dbi_dp1_dry, dbi_dp1_mask_dry, dbi_dp1_annual, \
        dbi_dp1_mask_annual, dbg_dp0_single, dbg_dp0_mask_single


if __name__ == '__main__':
    main_routine()
