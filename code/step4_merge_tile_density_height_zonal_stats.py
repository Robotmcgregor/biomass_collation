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
    return df


def seasonal_image_date(df):
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

    return df


def workflow(dir_):
    """ os walks through the tile zonal stats directory and locates all zonal stats csv files.
    Ensures that all date fields are labeled 'im_date' (i.e. image date).
    Creates a start and end image field for seasonal data (i.e. s_date and e_date).
    Appends files to file specific lists.

    :param dir_: string object containing the path to the tile zonal stats directory.
    :return: ccw_list, h99_list, hcv_list, seasonal_list
    """
    single_list = []
    seasonal_list = []
    ccw_list = []
    fdc_list = []
    h99_list = []
    hcv_list = []
    hmc_list = []
    hsd_list = []
    n17_list = []
    wdc_list = []
    wfp_list = []

    # ---- mask ----
    ccw_mask_list = []
    fdc_mask_list = []
    h99_mask_list = []
    hcv_mask_list = []
    hmc_mask_list = []
    hsd_mask_list = []
    n17_mask_list = []
    wdc_mask_list = []
    wfp_mask_list = []

    print(dir_)
    sub_list = next(os.walk(dir_))[1]
    for i in sub_list:
        # print("i: ", i)
        sub_list_path = os.path.join(dir_, i)

        sub_sub_list = next(os.walk(sub_list_path))[1]
        # print(sub_sub_list)
        for n in sub_sub_list:
            # print("n: ", n)
            sub_sub_list_path = os.path.join(sub_list_path, n)
            # print(sub_sub_list_path)

            for file_ in glob(os.path.join(sub_sub_list_path, "*zonal_stats.csv")):
                print("file: ", file_)

                df1 = pd.read_csv(file_)
                print("workflow df1:", df1.columns)

                if "date" in df1.columns and "im_date" not in df1.columns:
                    # print(df1.columns)
                    df1.rename(columns={"date": "im_date"}, inplace=True)

                elif "date" not in df1.columns and "im_date" not in df1.columns:
                    if "year" in df1.columns and "month" in df1.columns and "day" in df1.columns:

                        im_date_list = []
                        for index, row in df1.iterrows():
                            # print(row["year"])
                            im_date_list.append(str(row["year"]) + str(row["month"]) + str(row["day"]))

                        df1["im_date"] = im_date_list

                # print(df1.columns)

                path_, f = os.path.split(file_)
                print("path_: ", path_)
                print("f:", f)
                name_list = f.split("_")
                print("len name list: ", len(name_list))
                print("Name list: ", name_list)
                print("-" * 30)

                if len(name_list) == 6:
                    # print("no fire mask")
                    # print("name_list: ", name_list)
                    type_ = name_list[-3]
                    print("type_", type_)

                    if type_ == "ccw":
                        print("CCW_" * 100)
                        print(df1.columns)
                        ccw = seasonal_image_date(df1)
                        ccw_list.append(ccw)
                        print("ccw: ", ccw)

                    elif type_ == "fdc":
                        print("FDC_" * 100)
                        print(df1.columns)
                        fdc = seasonal_image_date(df1)
                        fdc_list.append(fdc)
                        print("fdc seasonal image: ", fdc.columns)

                    elif type_ == "h99":
                        print("h99_" * 100)
                        print(df1.columns)
                        h99 = seasonal_image_date(df1)
                        h99_list.append(h99)

                    elif type_ == "hcv":
                        print("HCV_" * 100)
                        print(df1.columns)
                        hcv = seasonal_image_date(df1)
                        hcv_list.append(hcv)

                    elif type_ == "hmc":
                        print("HMC_" * 100)
                        print(df1.columns)
                        hmc = seasonal_image_date(df1)
                        hmc_list.append(hmc)

                    elif type_ == "hsd":
                        print("HSD_" * 100)
                        print(df1.columns)
                        hsd = seasonal_image_date(df1)
                        hsd_list.append(hsd)

                    elif type_ == "n17":
                        print("N17_" * 100)
                        print(df1.columns)
                        n17 = seasonal_image_date(df1)
                        n17_list.append(n17)

                    elif type_ == "wdc":
                        print("WDC_" * 100)
                        print(df1.columns)
                        wdc = seasonal_image_date(df1)
                        wdc_list.append(wdc)

                    elif type_ == "wfp":
                        print("WFP_" * 100)
                        print(df1.columns)
                        wfp = seasonal_image_date(df1)
                        wfp_list.append(wfp)

                    else:
                        pass


                else:
                    pass

    print("fdc columns: ", fdc.columns)

    return ccw_list, fdc_list, h99_list, hcv_list, hmc_list, hsd_list, n17_list, wdc_list, wfp_list


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
    print(df.shape)
    print(df.columns)
    df.dropna(subset=['b1_{0}_minor'.format(type_)], inplace=True)
    print(df.info())

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

        print("merged height: ", merged_df.columns)

        print("delete height 370")
        import sys
        sys.exit()

        merged_df["direction"] = m_type

        if mask == False:
            output_path = os.path.join(output_dir, "merged_slats_field_agb_{0}_mask_{1}_tile.csv".format(type_, m_type))
        else:
            output_path = os.path.join(output_dir, "merged_slats_field_agb_{0}_{1}_tile.csv".format(type_, m_type))
        merged_df.to_csv(os.path.join(output_path), index=False)
        print("File output to: ", output_path)

        merge_df_list.append(merged_df)

        merged_df.dropna(subset=["site_y"], inplace=True)

        # todo added this line
        merged_df.dropna(subset=["image_dt"], inplace=True)

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
    # dp1_list, tile_export, tile_indv_export, biomass_df, "dp1", False
    dp1 = pd.concat(list_, axis=0)

    dp1.sort_values(by="image_dt", inplace=True)
    dp1.dropna(subset=['b1_{0}_minor'.format(type_)], inplace=True)
    'b1_{0}_minor'.format(type_)

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

        print("seasonal_columns: ", df.columns)
        # sort df on datetime data
        df.sort_values(by="image_dt", inplace=True)
        df.dropna(subset=['b1_{0}_minor'.format(type_)], inplace=True)

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

            # todo added this line
            start_merged_df.dropna(subset=["image_dt"], inplace=True)

            start_merged_df.to_csv(os.path.join(output_path), index=False)
            print("File output to: ", output_path)

            merge_df_dropna_list.append(start_merged_df)

    return merge_df_list, merge_df_dropna_list


def density_file_export(list_, output_dir, output_indv_dir, biomass_df, type_, mask):
    print("density file export" * 100)
    # dp1_list, tile_export, tile_indv_export, biomass_df, "dp1", False
    dp1 = pd.concat(list_, axis=0)
    print("dp1: ", dp1.columns)
    dp1.sort_values(by="image_dt", inplace=True)
    print("density_file_export: ", dp1.columns)
    dp1.dropna(subset=['b1_{0}_minor'.format(type_)], inplace=True)
    'b1_{0}_minor'.format(type_)
    print("density_file_export after drop minor: ", dp1.columns)

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

    # print(dp1)

    dry = dp1[dp1["s_month"] == 5]
    annual = dp1[dp1["s_month"] == 1]

    df_list = [dry, annual]
    df_str_list = ["dry", "annual"]

    merge_df_list = []
    merge_df_dropna_list = []

    for f_type, df in zip(df_str_list, df_list):

        print("-" * 50)
        print("seasonal_columns: ", df.columns)
        # sort df on datetime data
        df.sort_values(by="image_dt", inplace=True)
        df.dropna(subset=['b1_{0}_minor'.format(type_)], inplace=True)

        print(df.shape)

        # correct df site name
        site_list = []
        site_name = df["site"].tolist()

        for i in site_name:
            n = i.replace("_1ha", "")
            site_list.append(n)

        df["site_clean"] = site_list
        # print(df)

        list_merge = ["nearest"]  # , "forward", "backward"]

        print("-" * 50)
        print("biomass_df.columns: ", biomass_df.columns)
        print("df.columns: ", df.columns)

        df1 = df[['b1_{0}_major'.format(type_), 'b1_{0}_minor'.format(type_),
                  'image_dt', 'image', 'site_clean']]

        for m_type in list_merge:
            start_merged_df = pd.merge_asof(biomass_df, df1, left_on="basal_dt", right_on="image_dt", by="site_clean",
                                            direction=m_type)
            print(start_merged_df.shape)
            print("start_merged_df: ", start_merged_df.columns)
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

            print(start_merged_df.columns)

            # todo added this line
            start_merged_df.dropna(subset=["image_dt"], inplace=True)
            print("start_merged_df: ", start_merged_df.columns)

            # start_merged_df.dropna(subset=["site_y"], inplace=True)

            start_merged_df.to_csv(os.path.join(output_path), index=False)
            print("start_merged_df file output to: ", output_path)

            merge_df_dropna_list.append(start_merged_df)

    print(merge_df_list, merge_df_dropna_list)

    return merge_df_list, merge_df_dropna_list


def height_file_export(list_, output_dir, output_indv_dir, biomass_df, type_, mask):
    print("list_: ", list_)

    dp1 = pd.concat(list_, axis=0)
    print(dp1.shape)

    dp1.sort_values(by="image_dt", inplace=True)
    print(dp1.columns)
    dp1.dropna(subset=['b1_{0}_min'.format(type_)], inplace=True)
    'b1_{0}_min'.format(type_)
    print("dropna shape: ", dp1.shape)

    # export concatenated dp1 zonal stats
    output_path = os.path.join(output_dir, "{0}_zonal_concat.csv".format(type_))
    dp1.to_csv(os.path.join(output_path), index=False)
    print("File output to: ", output_path)

    # correct dp1 site name
    site_list = []
    site_name = dp1["site"].tolist()

    for i in site_name:
        print(i)
        n = i.replace("_1ha", "")
        site_list.append(n)

    print(site_list)
    dp1["site_clean"] = site_list

    # print(dp1)

    dry = dp1[dp1["s_month"] == 5]
    annual = dp1[dp1["s_month"] == 1]

    df_list = [dry, annual]
    df_str_list = ["dry", "annual"]

    merge_df_list = []
    merge_df_dropna_list = []

    for f_type, df in zip(df_str_list, df_list):

        print("-" * 50)
        print("f_type: ", f_type)
        print("seasonal_columns: ", df.columns)
        # sort df on datetime data
        df.sort_values(by="image_dt", inplace=True)
        df.dropna(subset=['b1_{0}_min'.format(type_)], inplace=True)

        print(df.shape)

        if not df.empty:
            # correct df site name
            site_list = []
            site_name = df["site"].tolist()

            for i in site_name:
                n = i.replace("_1ha", "")
                site_list.append(n)

            df["site_clean"] = site_list
            print("seasonal list: ", site_list)

            list_merge = ["nearest"]  # , "forward", "backward"]

            print("-" * 50)
            print("biomass_df.columns: ", biomass_df.columns)
            print("+" * 100)
            print("684")
            print("687 df.columns: ", df.columns)

            df1 = df[['b1_{0}_min'.format(type_), 'b1_{0}_max'.format(type_),
                      'b1_{0}_mean'.format(type_), 'b1_{0}_std'.format(type_), 'b1_{0}_med'.format(type_),
                      'b1_{0}_p25'.format(type_),
                      'b1_{0}_p50'.format(type_), 'b1_{0}_p75'.format(type_), 'b1_{0}_p95'.format(type_),
                      'b1_{0}_p99'.format(type_),
                      'image_dt', 'site_clean', 'image']]

            print("h99 df1: ", df1)
            print("+" * 100)
            print("df1 columns", df1.columns)

            for m_type in list_merge:
                start_merged_df = pd.merge_asof(biomass_df, df1, left_on="basal_dt", right_on="image_dt",
                                                by="site_clean",
                                                direction=m_type)
                print(start_merged_df.shape)
                print("start_merged_df: ", start_merged_df.columns)
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
                                               "merged_slats_field_agb_start_{0}_{1}_mask_{2}_tile_dropna.csv".format(
                                                   type_, f_type, m_type))

                print(start_merged_df.columns)
                # start_merged_df.dropna(subset=["site_y"], inplace=True)

                # todo added this line
                start_merged_df.dropna(subset=["image_dt"], inplace=True)

                start_merged_df.to_csv(os.path.join(output_path), index=False)
                print("File output to: ", output_path)

                merge_df_dropna_list.append(start_merged_df)

    print(merge_df_list, merge_df_dropna_list)

    return merge_df_list, merge_df_dropna_list


def height_class_file_export(list_, output_dir, output_indv_dir, biomass_df, type_, mask):
    print("list_: ", list_)
    dp1 = pd.concat(list_, axis=0)
    print(dp1.shape)

    dp1.sort_values(by="image_dt", inplace=True)
    print(dp1.columns)
    dp1.dropna(subset=['b1_{0}_minor'.format(type_)], inplace=True)
    'b1_{0}_minor'.format(type_)

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

    # print(dp1)

    dry = dp1[dp1["s_month"] == 5]
    annual = dp1[dp1["s_month"] == 1]

    df_list = [dry, annual]
    df_str_list = ["dry", "annual"]

    merge_df_list = []
    merge_df_dropna_list = []

    for f_type, df in zip(df_str_list, df_list):

        print("-" * 50)
        print("f_type class: ", f_type)
        print("seasonal_columns: ", df.columns)
        # sort df on datetime data
        df.sort_values(by="image_dt", inplace=True)
        df.dropna(subset=['b1_{0}_minor'.format(type_)], inplace=True)

        print(df.shape)

        if not df.empty:
            # correct df site name
            site_list = []
            site_name = df["site"].tolist()

            for i in site_name:
                n = i.replace("_1ha", "")
                site_list.append(n)

            df["site_clean"] = site_list
            # print(df)

            list_merge = ["nearest"]  # , "forward", "backward"]

            print("-" * 50)
            print("biomass_df.columns: ", biomass_df.columns)
            print("df.columns: ", df.columns)

            df1 = df[['b1_{0}_minor'.format(type_), 'b1_{0}_major'.format(type_),
                      # 'b1_{0}_mean'.format(type_),'b1_{0}_std'.format(type_), 'b1_{0}_med'.format(type_), 'b1_{0}_p25'.format(type_),
                      # 'b1_{0}_p50'.format(type_), 'b1_{0}_p75'.format(type_), 'b1_{0}_p95'.format(type_), 'b1_{0}_p99'.format(type_),
                      'image_dt', 'image', 'site_clean']]

            print("class df1: ", df1.columns)

            for m_type in list_merge:
                start_merged_df = pd.merge_asof(biomass_df, df1, left_on="basal_dt", right_on="image_dt",
                                                by="site_clean",
                                                direction=m_type)
                print(start_merged_df.shape)
                print("start_merged_df: ", start_merged_df.columns)
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
                                               "merged_slats_field_agb_start_{0}_{1}_mask_{2}_tile_dropna.csv".format(
                                                   type_, f_type, m_type))

                print(start_merged_df.columns)
                # start_merged_df.dropna(subset=["site_y"], inplace=True)

                # todo added this line
                start_merged_df.dropna(subset=["image_dt"], inplace=True)

                start_merged_df.to_csv(os.path.join(output_path), index=False)
                print("File output to: ", output_path)

                merge_df_dropna_list.append(start_merged_df)

    print(merge_df_list, merge_df_dropna_list)

    return merge_df_list, merge_df_dropna_list


def main_routine(biomass_csv, tile_dir, output_dir, dp0_dbg_si, dp0_dbg_si_mask, dp1_dbi_si_dry, dp1_dbi_si_mask_dry, \
                 dp1_dbi_si_annual, dp1_dbi_si_mask_annual):
    # Dictionary identifies the data structure of the reference image

    tile_export = os.path.join(output_dir, "tile_concat")
    tile_indv_export = os.path.join(output_dir, "tile_indv_site")
    if not os.path.isdir(tile_export):
        os.mkdir(tile_export)

    if not os.path.isdir(tile_indv_export):
        os.mkdir(tile_indv_export)

    # ------------------------------------------------------------------------------------------------------------------

    # Read in biomass csv
    biomass_df = pd.read_csv(biomass_csv)

    # Call the convert to datetime function
    biomass_df = convert_to_datetime(biomass_df, "date", "basal_dt")
    biomass_df.sort_values(by='basal_dt', inplace=True)

    # correct biomass site name
    site_list = []
    site_name = biomass_df["site"].tolist()
    print("biomass site_name: ", site_name)

    for i in site_name:
        n = i.replace("_", "")
        m = n[-4:]
        x = n[:-4] + "." + m

        site_list.append(x)

    print("biomass site list: ", site_list)
    biomass_df["site_clean"] = site_list

    print("biomass_df: ", biomass_df)

    ccw_list, fdc_list, h99_list, hcv_list, hmc_list, hsd_list, n17_list, wdc_list, wfp_list = workflow(tile_dir)

    # --------------------------------------- no fire mask ------------------------------------------

    if len(ccw_list) > 0:
        print(ccw_list)

        merge_ccw_list, merge_ccw_dropna_list = height_file_export(ccw_list, tile_export, tile_indv_export, biomass_df,
                                                                   "ccw", False)
        df = pd.concat(merge_ccw_list)
        df_drop = pd.concat(merge_ccw_dropna_list)
        print(df.shape)
        print(df.columns)
        print(df)
        df.to_csv(r"C:\Users\robot\projects\biomasss\collated_zonal_stats\density_height\df_ccw.csv")
        print(df_drop.shape)
        print(df_drop.shape)
        df_drop.to_csv(r"C:\Users\robot\projects\biomasss\collated_zonal_stats\density_height\df_ccw_dropna.csv")
        print(list(df_drop.columns))

        df_drop_ccw_reformat = df_drop[['uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94', 'geometry',
                                        'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha', 'bio_w_kg1ha',
                                        'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha', 'bio_agb_kg1ha',
                                        'c_l_kg1ha', 'c_t_kg1ha', 'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha',
                                        'c_s_kg1ha', 'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt',
                                        'b1_ccw_min', 'b1_ccw_max', 'b1_ccw_mean', 'b1_ccw_std',
                                        'b1_ccw_med', 'b1_ccw_p25', 'b1_ccw_p50', 'b1_ccw_p75', 'b1_ccw_p95',
                                        'b1_ccw_p99', 'image_dt', 'image', 'direction', 'season']]

        df_drop_ccw_reformat.rename(columns={"image_dt": "ccw_dt",
                                             "direction": "ccw_dir",
                                             "season": "ccw_seas"}, inplace=True)

    else:
        pass

    if len(fdc_list) > 0:
        print("*" * 100)
        # print("FDC - list: ", fdc_list)
        merge_fdc_list, merge_fdc_dropna_list = density_file_export(fdc_list, tile_export, tile_indv_export,
                                                                    biomass_df, "fdc", False)

        df = pd.concat(merge_fdc_list)
        df_drop = pd.concat(merge_fdc_dropna_list)
        print("fdc columns: ", df.columns)
        df.to_csv(r"C:\Users\robot\projects\biomasss\collated_zonal_stats\density_height\df_fdc.csv")
        print(df_drop.shape)
        df_drop.to_csv(r"C:\Users\robot\projects\biomasss\collated_zonal_stats\density_height\df_fdc_dropna.csv")
        print(list(df_drop.columns))

        df_drop_fdc_reformat = df_drop[['uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94', 'geometry',
                                        'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                                        'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha', 'bio_agb_kg1ha',
                                        'c_l_kg1ha', 'c_t_kg1ha',
                                        'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha', 'c_r_kg1ha', 'c_agb_kg1ha',
                                        'basal_dt',
                                        'b1_fdc_major', 'b1_fdc_minor', 'image_dt', 'image', 'direction', 'season']]

        df_drop_fdc_reformat.rename(columns={"image_dt": "fdc_dt",
                                             "direction": "fdc_dir",
                                             "season": "fdc_seas"}, inplace=True)

    else:
        pass

    if len(h99_list) > 0:
        print(h99_list[0].columns)
        merge_h99_list, merge_h99_dropna_list = height_file_export(h99_list, tile_export, tile_indv_export, biomass_df,
                                                                   "h99", False)

        df = pd.concat(merge_h99_list)
        df_drop = pd.concat(merge_h99_dropna_list)
        print(df.shape)
        print(df.columns)
        print(df)
        df.to_csv(r"C:\Users\robot\projects\biomasss\collated_zonal_stats\density_height\df_h99.csv")
        print(df_drop.shape)
        print(df_drop.shape)
        df_drop.to_csv(r"C:\Users\robot\projects\biomasss\collated_zonal_stats\density_height\df_h99_dropna.csv")
        print(list(df_drop.columns))

        df_drop_h99_reformat = df_drop[
            ['uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94', 'geometry', 'bio_l_kg1ha',
             'bio_t_kg1ha', 'bio_b_kg1ha',
             'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha', 'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
             'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha', 'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt',
             'b1_h99_min', 'b1_h99_max', 'b1_h99_mean', 'b1_h99_std', 'b1_h99_med', 'b1_h99_p25',
             'b1_h99_p50', 'b1_h99_p75', 'b1_h99_p95', 'b1_h99_p99', 'image_dt', 'image', 'direction', 'season']]

        df_drop_h99_reformat.rename(columns={"image_dt": "h99_dt",
                                             "direction": "h99_dir",
                                             "season": "h99_seas"}, inplace=True)

    else:
        pass

    if len(hcv_list) > 0:
        merge_hcv_list, merge_hcv_dropna_list = height_file_export(hcv_list, tile_export, tile_indv_export, biomass_df,
                                                                   "hcv", False)

        df = pd.concat(merge_hcv_list)
        df_drop = pd.concat(merge_hcv_dropna_list)
        print(df.shape)
        print(df.columns)
        print(df)
        df.to_csv(r"C:\Users\robot\projects\biomasss\collated_zonal_stats\density_height\df_hcv.csv")
        print(df_drop.shape)
        print(df_drop.shape)
        df_drop.to_csv(r"C:\Users\robot\projects\biomasss\collated_zonal_stats\density_height\df_hcv_dropna.csv")
        print(list(df_drop.columns))

        df_drop_hcv_reformat = df_drop[['uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94', 'geometry',
                                        'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                                        'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha', 'bio_agb_kg1ha',
                                        'c_l_kg1ha', 'c_t_kg1ha',
                                        'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha', 'c_r_kg1ha', 'c_agb_kg1ha',
                                        'basal_dt',
                                        'b1_hcv_min', 'b1_hcv_max', 'b1_hcv_mean', 'b1_hcv_std',
                                        'b1_hcv_med', 'b1_hcv_p25',
                                        'b1_hcv_p50', 'b1_hcv_p75', 'b1_hcv_p95', 'b1_hcv_p99', 'image_dt', 'image',
                                        'direction',
                                        'season']]

        df_drop_hcv_reformat.rename(columns={"image_dt": "hcv_dt",
                                             "direction": "hcv_dir",
                                             "season": "hcv_seas"}, inplace=True)

    else:
        print("There are no hcv records, goodbye....")
        pass

    if len(hmc_list) > 0:
        merge_hmc_list, merge_hmc_dropna_list = height_file_export(hmc_list, tile_export, tile_indv_export,
                                                                   biomass_df, "hmc", False)

        df = pd.concat(merge_hmc_list)
        df_drop = pd.concat(merge_hmc_dropna_list)
        print(df.shape)
        print(df.columns)
        print(df)
        df.to_csv(r"C:\Users\robot\projects\biomasss\collated_zonal_stats\density_height\df_hmc.csv")
        print(df_drop.shape)
        print(df_drop.shape)
        df_drop.to_csv(r"C:\Users\robot\projects\biomasss\collated_zonal_stats\density_height\df_hmc_dropna.csv")
        print(list(df_drop.columns))

        df_drop_hmc_reformat = df_drop[['uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94', 'geometry',
                                        'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                                        'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha', 'bio_agb_kg1ha',
                                        'c_l_kg1ha', 'c_t_kg1ha',
                                        'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha', 'c_r_kg1ha', 'c_agb_kg1ha',
                                        'basal_dt',
                                        'b1_hmc_min', 'b1_hmc_max', 'b1_hmc_mean', 'b1_hmc_std',
                                        'b1_hmc_med', 'b1_hmc_p25',
                                        'b1_hmc_p50', 'b1_hmc_p75', 'b1_hmc_p95', 'b1_hmc_p99', 'image_dt', 'image',
                                        'direction',
                                        'season']]

        df_drop_hmc_reformat.rename(columns={"image_dt": "hmc_dt",
                                             "direction": "hmc_dir",
                                             "season": "hmc_seas"}, inplace=True)
    else:
        print("There are no hmc records, goodbye....")
        pass

    if len(hsd_list) > 0:
        merge_hsd_list, merge_hsd_dropna_list = height_file_export(hsd_list, tile_export, tile_indv_export,
                                                                   biomass_df, "hsd", False)

        df = pd.concat(merge_hsd_list)
        df_drop = pd.concat(merge_hsd_dropna_list)
        print(df.shape)
        print(df.columns)
        print(df)
        df.to_csv(r"C:\Users\robot\projects\biomasss\collated_zonal_stats\density_height\df_hsd.csv")
        print(df_drop.shape)
        print(df_drop.shape)
        df_drop.to_csv(r"C:\Users\robot\projects\biomasss\collated_zonal_stats\density_height\df_hsd_dropna.csv")
        print(list(df_drop.columns))

        df_drop_hsd_reformat = df_drop[['uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94', 'geometry',
                                        'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha', 'bio_w_kg1ha',
                                        'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha', 'bio_agb_kg1ha',
                                        'c_l_kg1ha', 'c_t_kg1ha', 'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha',
                                        'c_s_kg1ha', 'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt',
                                        'b1_hsd_min', 'b1_hsd_max', 'b1_hsd_mean', 'b1_hsd_std',
                                        'b1_hsd_med', 'b1_hsd_p25', 'b1_hsd_p50', 'b1_hsd_p75', 'b1_hsd_p95',
                                        'b1_hsd_p99', 'image_dt', 'image', 'direction', 'season']]

        df_drop_hsd_reformat.rename(columns={"image_dt": "hsd_dt",
                                             "direction": "hsd_dir",
                                             "season": "hsd_seas"}, inplace=True)

    else:
        print("There are no hsd records, goodbye....")
        pass

    if len(n17_list) > 0:
        merge_n17_list, merge_n17_dropna_list = height_class_file_export(n17_list, tile_export, tile_indv_export,
                                                                         biomass_df, "n17", False)

        df = pd.concat(merge_n17_list)
        df_drop = pd.concat(merge_n17_dropna_list)
        print(df.shape)
        print(df.columns)
        print(df)
        df.to_csv(r"C:\Users\robot\projects\biomasss\collated_zonal_stats\density_height\df_n17.csv")
        print(df_drop.shape)
        print(df_drop.shape)
        df_drop.to_csv(r"C:\Users\robot\projects\biomasss\collated_zonal_stats\density_height\df_n17_dropna.csv")
        print(list(df_drop.columns))

        df_drop_n17_reformat = df_drop[['uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94', 'geometry',
                                        'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                                        'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha', 'bio_agb_kg1ha',
                                        'c_l_kg1ha', 'c_t_kg1ha',
                                        'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha', 'c_r_kg1ha', 'c_agb_kg1ha',
                                        'basal_dt',
                                        'b1_n17_major', 'b1_n17_minor', 'image_dt', 'image', 'direction', 'season']]

        df_drop_n17_reformat.rename(columns={"image_dt": "n17_dt",
                                             "direction": "n17_dir",
                                             "season": "n17_seas"}, inplace=True)
    else:
        print("There are no n17 records, goodbye....")
        pass

    if len(wdc_list) > 0:
        merge_wdc_list, merge_wdc_dropna_list = height_class_file_export(wdc_list, tile_export, tile_indv_export,
                                                                         biomass_df, "wdc", False)

        df = pd.concat(merge_wdc_list)
        df_drop = pd.concat(merge_wdc_dropna_list)
        print(df.shape)
        print(df.columns)
        print(df)
        df.to_csv(r"C:\Users\robot\projects\biomasss\collated_zonal_stats\density_height\df_wdc.csv")
        print(df_drop.shape)
        print(df_drop.shape)
        df_drop.to_csv(r"C:\Users\robot\projects\biomasss\collated_zonal_stats\density_height\df_wdc_dropna.csv")
        print(list(df_drop.columns))

        df_drop_wdc_reformat = df_drop[['uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94', 'geometry',
                                        'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                                        'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha', 'bio_agb_kg1ha',
                                        'c_l_kg1ha', 'c_t_kg1ha',
                                        'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha', 'c_r_kg1ha', 'c_agb_kg1ha',
                                        'basal_dt',
                                        'b1_wdc_major', 'b1_wdc_minor', 'image_dt', 'image', 'direction', 'season']]

        df_drop_wdc_reformat.rename(columns={"image_dt": "wdc_dt",
                                             "direction": "wdc_dir",
                                             "season": "wdc_seas"}, inplace=True)
    else:
        print("There are no wdc records, goodbye....")
        pass

    if len(wfp_list) > 0:
        merge_wfp_list, merge_wfp_dropna_list = height_file_export(wfp_list, tile_export, tile_indv_export,
                                                                   biomass_df, "wfp", False)

        df = pd.concat(merge_wfp_list)
        df_drop = pd.concat(merge_wfp_dropna_list)
        print(df.shape)
        print(df.columns)
        print(df)
        df.to_csv(r"C:\Users\robot\projects\biomasss\collated_zonal_stats\density_height\df_wfp.csv")
        print(df_drop.shape)
        print(df_drop.shape)
        df_drop.to_csv(r"C:\Users\robot\projects\biomasss\collated_zonal_stats\density_height\df_wfp_dropna.csv")
        print(list(df_drop.columns))

        df_drop_wfp_reformat = df_drop[['uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94', 'geometry',
                                        'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha', 'bio_w_kg1ha',
                                        'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha', 'bio_agb_kg1ha',
                                        'c_l_kg1ha', 'c_t_kg1ha', 'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha',
                                        'c_s_kg1ha', 'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt',
                                        'b1_wfp_min', 'b1_wfp_max', 'b1_wfp_mean', 'b1_wfp_std',
                                        'b1_wfp_med', 'b1_wfp_p25', 'b1_wfp_p50', 'b1_wfp_p75', 'b1_wfp_p95',
                                        'b1_wfp_p99', 'image_dt', 'image', 'direction', 'season']]

        df_drop_wfp_reformat.rename(columns={"image_dt": "wfp_dt",
                                             "direction": "wfp_dir",
                                             "season": "wfp_seas"}, inplace=True)

    else:
        print("There are no wfp records, goodbye....")
        pass

    print("-" * 100)

    # ------------------------------------------ Separate to seasons ----------------------------------------

    ccw_dry = df_drop_ccw_reformat[df_drop_ccw_reformat["ccw_seas"] == "dry"]

    ccw_annual = df_drop_ccw_reformat[df_drop_ccw_reformat["ccw_seas"] == "annual"]

    ccw_dry.rename(columns=dict(b1_ccw_min='b1_ccw_dry_min', b1_ccw_max='b1_ccw_dry_max', b1_ccw_mean='b1_ccw_dry_mean',
                                b1_ccw_std='b1_ccw_dry_std', b1_ccw_med='b1_ccw_dry_med', b1_ccw_p25='b1_ccw_dry_p25',
                                b1_ccw_p50='b1_ccw_dry_p50', b1_ccw_p75='b1_ccw_dry_p75', b1_ccw_p95='b1_ccw_dry_p95',
                                b1_ccw_p99='b1_ccw_dry_p99', ccw_dt='ccw_dry_dt', ccw_dir='ccw_dry_dir',
                                ccw_seas='ccw_dry_seas'), inplace=True)

    fdc_dry = df_drop_fdc_reformat[df_drop_fdc_reformat["fdc_seas"] == "dry"]
    fdc_annual = df_drop_fdc_reformat[df_drop_fdc_reformat["fdc_seas"] == "annual"]

    print(fdc_dry.columns)

    fdc_dry.rename(columns=dict({'b1_fdc_major': 'b1_fdc_dry_major',
                                 'b1_fdc_minor': 'b1_fdc_dry_minor',
                                 'fdc_dt': 'fdc_dry_dt',
                                 'fdc_dir': 'fdc_dry_dir',
                                 'fdc_seas': 'fdc_dry_seas'}), inplace=True)

    # --------------------------------------------------------------------------------------

    n17_dry = df_drop_n17_reformat[df_drop_n17_reformat["n17_seas"] == "dry"]
    n17_annual = df_drop_n17_reformat[df_drop_n17_reformat["n17_seas"] == "annual"]

    print(n17_dry.columns)

    n17_dry.rename(columns=dict({'b1_n17_major': 'b1_n17_dry_major',
                                 'b1_n17_minor': 'b1_n17_dry_minor',
                                 'n17_dt': 'n17_dry_dt',
                                 'n17_dir': 'n17_dry_dir',
                                 'n17_seas': 'n17_dry_seas'}), inplace=True)

    # ------------------------------------------------------------------------------------------------------------------

    wdc_dry = df_drop_wdc_reformat[df_drop_wdc_reformat["wdc_seas"] == "dry"]
    wdc_annual = df_drop_wdc_reformat[df_drop_wdc_reformat["wdc_seas"] == "annual"]

    print(wdc_dry.columns)

    wdc_dry.rename(columns=dict({'b1_wdc_major': 'b1_wdc_dry_major',
                                 'b1_wdc_minor': 'b1_wdc_dry_minor',
                                 'wdc_dt': 'wdc_dry_dt',
                                 'wdc_dir': 'wdc_dry_dir',
                                 'wdc_seas': 'wdc_dry_seas'}), inplace=True)

    # ------------------------------------------ WFP ----------------------------------------

    wfp_dry = df_drop_wfp_reformat[df_drop_wfp_reformat["wfp_seas"] == "dry"]

    wfp_annual = df_drop_wfp_reformat[df_drop_wfp_reformat["wfp_seas"] == "annual"]

    wfp_dry.rename(columns=dict(b1_wfp_min='b1_wfp_dry_min', b1_wfp_max='b1_wfp_dry_max', b1_wfp_mean='b1_wfp_dry_mean',
                                b1_wfp_std='b1_wfp_dry_std', b1_wfp_med='b1_wfp_dry_med', b1_wfp_p25='b1_wfp_dry_p25',
                                b1_wfp_p50='b1_wfp_dry_p50', b1_wfp_p75='b1_wfp_dry_p75', b1_wfp_p95='b1_wfp_dry_p95',
                                b1_wfp_p99='b1_wfp_dry_p99', wfp_dt='wfp_dry_dt', wfp_dir='wfp_dry_dir',
                                wfp_seas='wfp_dry_seas'), inplace=True)

    # ==================================================================================================================

    # Data is only a single season

    h99_annual = df_drop_h99_reformat[df_drop_h99_reformat["h99_seas"] == "annual"]

    hcv_annual = df_drop_hcv_reformat[df_drop_hcv_reformat["hcv_seas"] == "annual"]

    hmc_annual = df_drop_hmc_reformat[df_drop_hmc_reformat["hmc_seas"] == "annual"]

    hsd_annual = df_drop_hsd_reformat[df_drop_hsd_reformat["hsd_seas"] == "annual"]

    # ------------------------------------------- ccw dry and annual --------------------------------------------------

    print(list(ccw_dry.columns))
    print(list(ccw_annual))

    ccw_both = pd.merge(right=ccw_dry, left=ccw_annual, how="outer",
                        on=['uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94', 'geometry',
                            'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                            'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha',
                            'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
                            'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha',
                            'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'])

    ccw_both.to_csv(r"C:\Users\robot\projects\biomasss\collated_zonal_stats\density_height\ccw_both.csv", index=False)

    # ------------------------------------------- fdc dry and annual --------------------------------------------------

    fdc_both = pd.merge(right=fdc_dry, left=fdc_annual, how="outer",
                        on=['uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94', 'geometry',
                            'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                            'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha',
                            'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
                            'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha',
                            'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'])

    fdc_both.to_csv(r"C:\Users\robot\projects\biomasss\collated_zonal_stats\density_height\fdc_both.csv", index=False)

    # ----------------------------------------- ccw and fdc ------------------------------------------------------

    ccw_fdc_both = pd.merge(right=ccw_both, left=fdc_both, how="outer",
                            on=['uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94', 'geometry',
                                'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                                'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha',
                                'bio_r_kg1ha', 'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
                                'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha',
                                'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'])

    ccw_fdc_both.to_csv(r"C:\Users\robot\projects\biomasss\collated_zonal_stats\density_height\ccw_fdc_both.csv", index=False)

    # -------------------------------------- Add h99 Annual only ---------------------------------------------------

    ccw_fdc_h99_dry = pd.merge(right=ccw_fdc_both, left=h99_annual, how="outer",
                               on=['uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94', 'geometry',
                                   'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                                   'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha', 'bio_agb_kg1ha',
                                   'c_l_kg1ha', 'c_t_kg1ha',
                                   'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha', 'c_r_kg1ha', 'c_agb_kg1ha',
                                   'basal_dt'])

    ccw_fdc_h99_dry.to_csv(r"C:\Users\robot\projects\biomasss\collated_zonal_stats\density_height\ccw_fdc_h99_dry.csv", index=False)

    # --------------------------------------- Add hcv annual only ------------------------------------------------------

    ccw_fdc_h99_hcv = pd.merge(right=ccw_fdc_h99_dry, left=hcv_annual, how="outer",
                               on=['uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94', 'geometry',
                                   'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                                   'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha', 'bio_agb_kg1ha',
                                   'c_l_kg1ha', 'c_t_kg1ha',
                                   'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha', 'c_r_kg1ha', 'c_agb_kg1ha',
                                   'basal_dt'])

    ccw_fdc_h99_hcv.to_csv(r"C:\Users\robot\projects\biomasss\collated_zonal_stats\density_height\ccw_fdc_h99_hcv.csv", index=False)

    # --------------------------------------------- Add hmc annual only ---------------------------

    ccw_fdc_h99_hcv_hmc = pd.merge(right=ccw_fdc_h99_hcv, left=hmc_annual, how="outer",
                                   on=['uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94', 'geometry',
                                       'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                                       'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha', 'bio_agb_kg1ha',
                                       'c_l_kg1ha', 'c_t_kg1ha',
                                       'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha', 'c_r_kg1ha',
                                       'c_agb_kg1ha', 'basal_dt'])

    ccw_fdc_h99_hcv_hmc.to_csv(r"C:\Users\robot\projects\biomasss\collated_zonal_stats\density_height\ccw_fdc_h99_hcv_hmc.csv", index=False)

    # --------------------------------------------- Add hsd annual only ---------------------------

    ccw_fdc_h99_hcv_hmc_hsd = pd.merge(right=ccw_fdc_h99_hcv_hmc, left=hsd_annual, how="outer",
                                       on=['uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94', 'geometry',
                                           'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                                           'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha', 'bio_agb_kg1ha',
                                           'c_l_kg1ha', 'c_t_kg1ha',
                                           'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha', 'c_r_kg1ha',
                                           'c_agb_kg1ha', 'basal_dt'])

    ccw_fdc_h99_hcv_hmc_hsd.to_csv(r"C:\Users\robot\projects\biomasss\collated_zonal_stats\density_height\ccw_fdc_h99_hcv_hmc_hsd.csv", index=False)

    # ------------------------------------------- n17 dry and annual --------------------------------------------------

    n17_both = pd.merge(right=n17_dry, left=n17_annual, how="outer",
                        on=['uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94', 'geometry',
                            'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                            'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha',
                            'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
                            'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha',
                            'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'])

    n17_both.to_csv(r"C:\Users\robot\projects\biomasss\collated_zonal_stats\density_height\n17_both.csv", index=False)

    # ----------------------------------------------- Add n17 both ----------------------------------------------------

    ccw_fdc_h99_hcv_hmc_hsd_n17 = pd.merge(right=ccw_fdc_h99_hcv_hmc_hsd, left=n17_both, how="outer",
                                           on=['uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94', 'geometry',
                                               'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                                               'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha',
                                               'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
                                               'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha',
                                               'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'])

    ccw_fdc_h99_hcv_hmc_hsd_n17.to_csv(r"C:\Users\robot\projects\biomasss\collated_zonal_stats\density_height\ccw_fdc_h99_hcv_hmc_hsd_n17.csv",
                                       index=False)

    # ------------------------------------------- wdc dry and annual --------------------------------------------------

    wdc_both = pd.merge(right=wdc_dry, left=wdc_annual, how="outer",
                        on=['uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94', 'geometry',
                            'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                            'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha',
                            'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
                            'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha',
                            'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'])

    wdc_both.to_csv(r"C:\Users\robot\projects\biomasss\collated_zonal_stats\density_height\wdc_both.csv", index=False)

    # ----------------------------------------------- Add wdc both ----------------------------------------------------

    ccw_fdc_h99_hcv_hmc_hsd_n17_wdc = pd.merge(right=ccw_fdc_h99_hcv_hmc_hsd_n17, left=wdc_both, how="outer",
                                               on=['uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94', 'geometry',
                                                   'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                                                   'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha',
                                                   'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
                                                   'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha',
                                                   'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'])

    ccw_fdc_h99_hcv_hmc_hsd_n17_wdc.to_csv(r"C:\Users\robot\projects\biomasss\collated_zonal_stats\density_height\ccw_fdc_h99_hcv_hmc_hsd_n17_wdc.csv",
                                           index=False)

    # ------------------------------------------- wfp dry and annual --------------------------------------------------

    wfp_both = pd.merge(right=wfp_dry, left=wfp_annual, how="outer",
                        on=['uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94', 'geometry',
                            'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                            'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha',
                            'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
                            'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha',
                            'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'])

    wfp_both.to_csv(r"C:\Users\robot\projects\biomasss\collated_zonal_stats\density_height\wfp_both.csv", index=False)

    # ----------------------------------------------- Add wfp both ----------------------------------------------------

    ccw_fdc_h99_hcv_hmc_hsd_n17_wdc_wfp = pd.merge(right=ccw_fdc_h99_hcv_hmc_hsd_n17_wdc, left=wfp_both, how="outer",
                                                   on=['uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94',
                                                       'geometry',
                                                       'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                                                       'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha',
                                                       'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
                                                       'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha',
                                                       'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'])

    ccw_fdc_h99_hcv_hmc_hsd_n17_wdc_wfp.to_csv(
        r"C:\Users\robot\projects\biomasss\collated_zonal_stats\density_height\ccw_fdc_h99_hcv_hmc_hsd_n17_wdc_wfp.csv",
        index=False)

    # print(list(ccw_fdc_h99_hcv_hmc_hsd_n17_wdc_wfp))

    ccw_fdc_h99_hcv_hmc_hsd_n17_wdc_wfp_clean = ccw_fdc_h99_hcv_hmc_hsd_n17_wdc_wfp[[
        'uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94', 'geometry', 'bio_l_kg1ha', 'bio_t_kg1ha',
        'bio_b_kg1ha',
        'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha', 'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
        'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha', 'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt',
        # ----------------------------------- wfp annual-----------------------------

        'b1_wfp_min', 'b1_wfp_max', 'b1_wfp_mean', 'b1_wfp_std', 'b1_wfp_med', 'b1_wfp_p25', 'b1_wfp_p50', 'b1_wfp_p75',
        'b1_wfp_p95', 'b1_wfp_p99',
        'wfp_dt', 'wfp_dir', 'wfp_seas',
        # ----------------------------------- wfp dry-----------------------------
        'b1_wfp_dry_min',
        'b1_wfp_dry_max', 'b1_wfp_dry_mean', 'b1_wfp_dry_std', 'b1_wfp_dry_med', 'b1_wfp_dry_p25', 'b1_wfp_dry_p50',
        'b1_wfp_dry_p75', 'b1_wfp_dry_p95', 'b1_wfp_dry_p99',

        # ------------------------------------- wdc annual -----------------------------
        'b1_wdc_major', 'b1_wdc_minor',
        # ------------------------------------- wdc dry ---------------------------------------------
        'b1_wdc_dry_major', 'b1_wdc_dry_minor',
        # ------------------------------------- n17 annual -----------------------------
        'b1_n17_major', 'b1_n17_minor',
        # ------------------------------------- n17 dry ------------------------------------
        'b1_n17_dry_major', 'b1_n17_dry_minor',
        # ----------------------------------- hsd annual-----------------------------
        'b1_hsd_min', 'b1_hsd_max', 'b1_hsd_mean', 'b1_hsd_std', 'b1_hsd_med', 'b1_hsd_p25',
        'b1_hsd_p50', 'b1_hsd_p75', 'b1_hsd_p95',
        'b1_hsd_p99',
        # ----------------------------------- hmc annual-----------------------------
        'b1_hmc_min', 'b1_hmc_max',
        'b1_hmc_mean', 'b1_hmc_std', 'b1_hmc_med', 'b1_hmc_p25', 'b1_hmc_p50', 'b1_hmc_p75',
        'b1_hmc_p95', 'b1_hmc_p99',
        # ----------------------------------- hcv annual-----------------------------
        'b1_hcv_min', 'b1_hcv_max', 'b1_hcv_mean',
        'b1_hcv_std', 'b1_hcv_med', 'b1_hcv_p25', 'b1_hcv_p50', 'b1_hcv_p75', 'b1_hcv_p95',
        'b1_hcv_p99',  # 'hcv_dt',

        # ----------------------------------- hcv annual-----------------------------
        'b1_h99_min', 'b1_h99_max', 'b1_h99_mean', 'b1_h99_std',
        'b1_h99_med', 'b1_h99_p25', 'b1_h99_p50', 'b1_h99_p75', 'b1_h99_p95', 'b1_h99_p99',

        # ----------------------------------- fdc annual -----------------------------
        'b1_fdc_major', 'b1_fdc_minor',
        # ------------------------------------ fdc dry -------------------------------------
        'b1_fdc_dry_major', 'b1_fdc_dry_minor',
        # ----------------------------------- ccw annual -----------------------------
        'b1_ccw_min', 'b1_ccw_max', 'b1_ccw_mean', 'b1_ccw_std', 'b1_ccw_med', 'b1_ccw_p25',
        'b1_ccw_p50', 'b1_ccw_p75', 'b1_ccw_p95', 'b1_ccw_p99', 'ccw_dt', 'ccw_dir', 'ccw_seas',
        # ----------------------------------- ccw dry ---------------------------------------------
        'b1_ccw_dry_min', 'b1_ccw_dry_max', 'b1_ccw_dry_mean', 'b1_ccw_dry_std', 'b1_ccw_dry_med',
        'b1_ccw_dry_p25', 'b1_ccw_dry_p50', 'b1_ccw_dry_p75', 'b1_ccw_dry_p95', 'b1_ccw_dry_p99',
    ]]

    print("before: ", list(ccw_fdc_h99_hcv_hmc_hsd_n17_wdc_wfp_clean.columns))
    ccw_fdc_h99_hcv_hmc_hsd_n17_wdc_wfp_clean.rename(
        columns={'site_clean_x_x': 'site_clean'}, inplace=True)

    print("after: ", list(ccw_fdc_h99_hcv_hmc_hsd_n17_wdc_wfp_clean.columns))
    # import sys
    # sys.exit()

    ccw_fdc_h99_hcv_hmc_hsd_n17_wdc_wfp_clean.to_csv(
        r"C:\Users\robot\projects\biomasss\collated_zonal_stats\density_height\ccw_fdc_h99_hcv_hmc_hsd_n17_wdc_wfp_clean.csv", index=False)

    # ===================================================================================================================

    annual = ccw_fdc_h99_hcv_hmc_hsd_n17_wdc_wfp[[
        'uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94', 'geometry', 'bio_l_kg1ha', 'bio_t_kg1ha',
        'bio_b_kg1ha',
        'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha', 'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
        'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha', 'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt',
        # ----------------------------------- wfp annual-----------------------------

        'b1_wfp_min', 'b1_wfp_max', 'b1_wfp_mean', 'b1_wfp_std', 'b1_wfp_med', 'b1_wfp_p25', 'b1_wfp_p50', 'b1_wfp_p75',
        'b1_wfp_p95', 'b1_wfp_p99',
        'wfp_dt', 'wfp_dir', 'wfp_seas',
        # ------------------------------------- wdc annual -----------------------------
        'b1_wdc_major', 'b1_wdc_minor',
        # ------------------------------------- n17 annual -----------------------------
        'b1_n17_major', 'b1_n17_minor',
        # ----------------------------------- hsd annual-----------------------------
        'b1_hsd_min', 'b1_hsd_max', 'b1_hsd_mean', 'b1_hsd_std', 'b1_hsd_med', 'b1_hsd_p25',
        'b1_hsd_p50', 'b1_hsd_p75', 'b1_hsd_p95',
        'b1_hsd_p99',
        # ----------------------------------- hmc annual-----------------------------
        'b1_hmc_min', 'b1_hmc_max',
        'b1_hmc_mean', 'b1_hmc_std', 'b1_hmc_med', 'b1_hmc_p25', 'b1_hmc_p50', 'b1_hmc_p75',
        'b1_hmc_p95', 'b1_hmc_p99',
        # ----------------------------------- hcv annual-----------------------------
        'b1_hcv_min', 'b1_hcv_max', 'b1_hcv_mean',
        'b1_hcv_std', 'b1_hcv_med', 'b1_hcv_p25', 'b1_hcv_p50', 'b1_hcv_p75', 'b1_hcv_p95',
        'b1_hcv_p99',  # 'hcv_dt',
        # ----------------------------------- hcv annual-----------------------------
        'b1_h99_min', 'b1_h99_max', 'b1_h99_mean', 'b1_h99_std',
        'b1_h99_med', 'b1_h99_p25', 'b1_h99_p50', 'b1_h99_p75', 'b1_h99_p95', 'b1_h99_p99',
        # ----------------------------------- fdc annual -----------------------------
        'b1_fdc_major', 'b1_fdc_minor',
        # ----------------------------------- ccw annual -----------------------------
        'b1_ccw_min', 'b1_ccw_max', 'b1_ccw_mean', 'b1_ccw_std', 'b1_ccw_med', 'b1_ccw_p25',
        'b1_ccw_p50', 'b1_ccw_p75', 'b1_ccw_p95', 'b1_ccw_p99', 'ccw_dt', 'ccw_dir', 'ccw_seas']]

    dry_season = ccw_fdc_h99_hcv_hmc_hsd_n17_wdc_wfp[[
        'uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94', 'geometry', 'bio_l_kg1ha', 'bio_t_kg1ha',
        'bio_b_kg1ha',
        'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha', 'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
        'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha', 'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt',
        # ----------------------------------- wfp dry-----------------------------
        'b1_wfp_dry_min',
        'b1_wfp_dry_max', 'b1_wfp_dry_mean', 'b1_wfp_dry_std', 'b1_wfp_dry_med', 'b1_wfp_dry_p25', 'b1_wfp_dry_p50',
        'b1_wfp_dry_p75', 'b1_wfp_dry_p95', 'b1_wfp_dry_p99',
        # ------------------------------------- wdc dry ---------------------------------------------
        'b1_wdc_dry_major', 'b1_wdc_dry_minor',
        # ------------------------------------- n17 dry ------------------------------------
        'b1_n17_dry_major', 'b1_n17_dry_minor',
        # ----------------------------------- hsd annual-----------------------------
        'b1_hsd_min', 'b1_hsd_max', 'b1_hsd_mean', 'b1_hsd_std', 'b1_hsd_med', 'b1_hsd_p25',
        'b1_hsd_p50', 'b1_hsd_p75', 'b1_hsd_p95',
        'b1_hsd_p99',
        # ----------------------------------- hmc annual-----------------------------
        'b1_hmc_min', 'b1_hmc_max',
        'b1_hmc_mean', 'b1_hmc_std', 'b1_hmc_med', 'b1_hmc_p25', 'b1_hmc_p50', 'b1_hmc_p75',
        'b1_hmc_p95', 'b1_hmc_p99',
        # ----------------------------------- hcv annual-----------------------------
        'b1_hcv_min', 'b1_hcv_max', 'b1_hcv_mean',
        'b1_hcv_std', 'b1_hcv_med', 'b1_hcv_p25', 'b1_hcv_p50', 'b1_hcv_p75', 'b1_hcv_p95',
        'b1_hcv_p99',  # 'hcv_dt',

        # ----------------------------------- hcv annual-----------------------------
        'b1_h99_min', 'b1_h99_max', 'b1_h99_mean', 'b1_h99_std',
        'b1_h99_med', 'b1_h99_p25', 'b1_h99_p50', 'b1_h99_p75', 'b1_h99_p95', 'b1_h99_p99',

        # ------------------------------------ fdc dry -------------------------------------
        'b1_fdc_dry_major', 'b1_fdc_dry_minor',
        # ----------------------------------- ccw dry ---------------------------------------------
        'b1_ccw_dry_min', 'b1_ccw_dry_max', 'b1_ccw_dry_mean', 'b1_ccw_dry_std', 'b1_ccw_dry_med',
        'b1_ccw_dry_p25', 'b1_ccw_dry_p50', 'b1_ccw_dry_p75', 'b1_ccw_dry_p95', 'b1_ccw_dry_p99']]

    # ==================================================================================================================
    # --------------------------------------------- annual merge -------------------------------------------------------
    dp0_dbg_si_single_annual_density = pd.merge(right=dp0_dbg_si, left=annual, how="outer",
                                                on=['uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94', 'geometry',
                                                    'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                                                    'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha',
                                                    'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
                                                    'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha',
                                                    'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'])

    dp0_dbg_si_single_annual_density.to_csv(
        r"C:\Users\robot\projects\biomasss\collated_zonal_stats\single\dp0_dbg_si_single_annual_density.csv",
        index=False)

    dp0_dbg_si_mask_single_annual_density = pd.merge(right=dp0_dbg_si_mask, left=annual, how="outer",
                                                     on=['uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94',
                                                         'geometry',
                                                         'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                                                         'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha',
                                                         'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
                                                         'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha',
                                                         'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'])

    dp0_dbg_si_mask_single_annual_density.to_csv(
        r"C:\Users\robot\projects\biomasss\collated_zonal_stats\single_mask\dp0_dbg_si_mask_single_annual_density.csv",
        index=False)

    # ------------------------------------------------------ dry merge -------------------------------------------------

    dp0_dbg_si_single_dry_density = pd.merge(right=dp0_dbg_si, left=dry_season, how="outer",
                                             on=['uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94', 'geometry',
                                                 'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                                                 'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha',
                                                 'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
                                                 'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha',
                                                 'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'])

    dp0_dbg_si_single_dry_density.to_csv(
        r"C:\Users\robot\projects\biomasss\collated_zonal_stats\single\dp0_dbg_si_single_dry_density.csv",
        index=False)

    dp0_dbg_si_mask_single_dry_density = pd.merge(right=dp0_dbg_si_mask, left=dry_season, how="outer",
                                                  on=['uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94', 'geometry',
                                                      'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                                                      'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha',
                                                      'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
                                                      'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha',
                                                      'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'])

    dp0_dbg_si_mask_single_dry_density.to_csv(
        r"C:\Users\robot\projects\biomasss\collated_zonal_stats\single_mask\dp0_dbg_si_mask_single_dry_density.csv",
        index=False)

    # ------------------------------------------------------ annual merge dp1 bbi --------------------------------------

    dp1_dbi_si_annual_density = pd.merge(right=dp1_dbi_si_annual, left=annual, how="outer",
                                         on=['uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94', 'geometry',
                                             'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                                             'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha',
                                             'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
                                             'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha',
                                             'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'])

    dp1_dbi_si_annual_density.to_csv(
        r"C:\Users\robot\projects\biomasss\collated_zonal_stats\annual\dp1_dbi_si_annual_density.csv",
        index=False)

    dp1_dbi_si_annual_mask_density = pd.merge(right=dp1_dbi_si_mask_annual, left=annual, how="outer",
                                              on=['uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94', 'geometry',
                                                  'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                                                  'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha',
                                                  'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
                                                  'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha',
                                                  'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'])

    dp1_dbi_si_annual_mask_density.to_csv(
        r"C:\Users\robot\projects\biomasss\collated_zonal_stats\annual_mask\dp1_dbi_si_annual_mask_density.csv",
        index=False)

    # ---------------------------------------------- dry merge dp1 bbi -------------------------------------------------

    dp1_dbi_si_dry_density = pd.merge(right=dp1_dbi_si_dry, left=dry_season, how="outer",
                                      on=['uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94', 'geometry',
                                          'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                                          'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha',
                                          'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
                                          'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha',
                                          'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'])

    dp1_dbi_si_dry_density.to_csv(
        r"C:\Users\robot\projects\biomasss\collated_zonal_stats\dry\dp1_dbi_si_dry_density.csv",
        index=False)

    dp1_dbi_si_dry_mask_density = pd.merge(right=dp1_dbi_si_mask_dry, left=dry_season, how="outer",
                                           on=['uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94', 'geometry',
                                               'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                                               'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha',
                                               'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
                                               'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha',
                                               'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'])

    dp1_dbi_si_dry_mask_density.to_csv(
        r"C:\Users\robot\projects\biomasss\collated_zonal_stats\dry_mask\dp1_dbi_si_dry_mask_density.csv",
        index=False)

    return biomass_df, ccw_fdc_h99_hcv_hmc_hsd_n17_wdc_wfp, ccw_fdc_h99_hcv_hmc_hsd_n17_wdc_wfp_clean, \
           dp0_dbg_si_single_annual_density, dp0_dbg_si_mask_single_annual_density, dp0_dbg_si_single_dry_density, \
           dp0_dbg_si_mask_single_dry_density, dp1_dbi_si_annual_density, dp1_dbi_si_annual_mask_density, \
           dp1_dbi_si_dry_density, dp1_dbi_si_dry_mask_density


if __name__ == '__main__':
    main_routine()
