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
import geopandas as gpd
from numpy import random
import numpy as np
from scipy.stats import poisson
# import plotting and stats modules
# import matplotlib.pyplot as plt
# import seaborn as sns
import scipy
import scipy.stats as sc
import matplotlib.pyplot as plt


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
    for i in df.im_date:
        print(i)
        #         list_name = i.split("_")
        #         date = str(i) + "0101" #list_name[-2]
        st_date = str(i) + "01"
        start_date = start_seasonal_date(st_date)
        st_date_list.append(start_date)

        e_date = str(i) + "12"
        print(e_date)
        end_date = end_seasonal_date(e_date)
        e_date_list.append(end_date)

    df["s_date"] = st_date_list
    df["e_date"] = e_date_list

    return df


def convert_to_datetime(df, col_nm_s, col_nm_d):
    """ Read in the column 'col_nm_s' from the input df (expected format is YYYYmmdd) and convert to date-time object.
        Appendd date-time objects to list, then create new df feature with list.

    :param df: pandas dataframe object.
    :param col_nm_s: string object containing existing column name.
    :param col_nm_d: string object containing new column name where date-time object is stored.
    :return df: Pandas df object with new column.
    """
    date_list = []
    for i in df[col_nm_s]:
        # print(i)
        datetime_object = datetime.strptime(str(i), '%Y%m%d')
        date_list.append(datetime_object)
        print(datetime_object)
        # df[col_nm_d] =  pd.to_datetime(df[col_nm_s], format='%Y%m%d.%f')
        # date_time = now.strftime("%m/%d/%Y, %H:%M:%S")
    df[col_nm_d] = date_list
    return df


def convert_to_dt_year(df, col_nm_s, col_nm_d):
    date_list = []
    for i in df[col_nm_s]:
        # print(i)
        datetime_object = datetime.strptime(str(i), '%Y')
        date_list.append(datetime_object)
        print(datetime_object)
        # df[col_nm_d] =  pd.to_datetime(df[col_nm_s], format='%Y%m%d.%f')
        # date_time = now.strftime("%m/%d/%Y, %H:%M:%S")
    df[col_nm_d] = date_list
    return df


# def fire_percent_fn(df):
#     """ Calculate the percent cover burnt by fire. """
#     df.fillna(0, inplace=True)
#     df["area_ha"] = (df.dka_count * (30 * 30) * 0.0001)
#     df["jan_per"] = (df.jan / df.dka_count * 100)  # (30 *30)/ 1000)
#     df["feb_per"] = (df.feb / df.dka_count * 100)
#     df["mar_per"] = (df.mar / df.dka_count * 100)
#     df["april_per"] = (df.april / df.dka_count * 100)
#     df["may_per"] = (df.may / df.dka_count * 100)
#     df["june_per"] = (df.june / df.dka_count * 100)
#     df["july_per"] = (df.july / df.dka_count * 100)
#     df["aug_per"] = (df.aug / df.dka_count * 100)
#     df["sep_per"] = (df.sep / df.dka_count * 100)
#     df["oct_per"] = (df.oct / df.dka_count * 100)
#     df["nov_per"] = (df.nov / df.dka_count * 100)
#     df["dec_per"] = (df.dec / df.dka_count * 100)
#
#     return df

def fire_percent_fn(df, var_):
    """ Calculate the percent cover burnt by fire. """

    print("Calculating fire percentage")
    print(var_)
    df.fillna(0, inplace=True)
    df["area_ha"] = (df[f"{var_}_count"] * (30 * 30) * 0.0001)
    df["jan_per"] = (df.jan / df[f"{var_}_count"] * 100)  # (30 *30)/ 1000)
    df["feb_per"] = (df.feb / df[f"{var_}_count"] * 100)
    df["mar_per"] = (df.mar / df[f"{var_}_count"] * 100)
    df["april_per"] = (df.april / df[f"{var_}_count"] * 100)
    df["may_per"] = (df.may / df[f"{var_}_count"] * 100)
    df["june_per"] = (df.june / df[f"{var_}_count"] * 100)
    df["july_per"] = (df.july / df[f"{var_}_count"] * 100)
    df["aug_per"] = (df.aug / df[f"{var_}_count"] * 100)
    df["sep_per"] = (df.sep / df[f"{var_}_count"] * 100)
    df["oct_per"] = (df.oct / df[f"{var_}_count"] * 100)
    df["nov_per"] = (df.nov / df[f"{var_}_count"] * 100)
    df["dec_per"] = (df.dec / df[f"{var_}_count"] * 100)

    return df


# def fire_yn_fn(df):
#     """ Score if fire occured during the year 0 = No, 1 = yes. """
#     fire_1_0 = []
#
#     for index, row in df.iterrows():
#
#         if row.dka_major == 0:
#             fire_1_0.append(0)
#         else:
#             fire_1_0.append(1)
#
#     df['burnt'] = fire_1_0
#
#     return df

def fire_yn_fn(df, var_):
    """ Score if fire occured during the year 0 = No, 1 = yes. """
    fire_1_0 = []
    fire_label = []

    for index, row in df.iterrows():

        if row[f"{var_}_major"] == 0:
            fire_1_0.append(0)
            fire_label.append("no fire")
        else:
            fire_1_0.append(1)
            fire_label.append("fire")

    df['burnt_enco'] = fire_1_0
    df['burnt_cat'] = fire_label

    return df


def fire_intensity_fn(df, var_):
    """ Score fire intensity by majority burnt 0 = no fire, 1 = Jan - June, 2 July - December """

    list_ = []
    label_list = []

    for index, row in df.iterrows():

        if row[f"{var_}_major"] == 0:
            list_.append(0)
            label_list.append("no fire")

        #todo - what about november december - fire intensity
        elif row[f"{var_}_major"] >= 1 and row[f"{var_}_major"] < 7:
            list_.append(1)
            label_list.append("early")
        elif row[f"{var_}_major"] >= 11:
            list_.append(1)
            label_list.append("early")
        else:
            list_.append(2)
            label_list.append("late")

    df['fire_sn_ord'] = list_
    df['fire_sn_cat'] = label_list

    return df


def ratio_fire_year_fn(x, y, p, n):
    # data number of fires per data length of time = x/y
    # revised time frame i.e. number of fires per time restriction = p/n

    final = (x * n) / (p * y)

    return final


def prop_fire_freq_fn(df):
    print("prop - fire frequency....")
    #todo what is the methodology used for fire frequency
    list_ = []
    for i in df.site_clean.unique():
        df1 = df[df["site_clean"] == i]

        burnt_sum = df1.burnt_enco.sum()
        print("burnt sum: ", burnt_sum)
        # calculate average time between fires over years of data capture
        if burnt_sum > 0:
            print("burnt sum > 0")

            freq = ratio_fire_year_fn(1, 10, burnt_sum, (len(df1.index)))

            frequency = round(freq, 5)  # round((len(df1.index)) / burnt_sum, 5)
        else:
            frequency = round(0 / 10, 5)  # round(len(df1.index), 5)

        df1["fire_f"] = frequency
        df1["fire_tot"] = burnt_sum = df1.burnt_enco.sum()

        list_.append(df1)

    df2 = pd.concat(list_, axis=0)
    return df2


def fire_previous_year(df):
    print("fire_previous_year")

    list_ = []
    df.dropna(inplace=True)
    for i in df.site_clean.unique():
        years_since_list = []
        print(i)
        df1 = df[df["site_clean"] == i]

        df1.sort_values(by="s_date", inplace=True, ascending=True)
        #         print(df1)

        no_fire_list = []

        loop_x = 1
        for index, row in df1.iterrows():
            x = row["burnt_enco"]
            print(loop_x)
            print("burnt: ", x)
            #             print("len of list: ", len(no_fire_list))
            if x == 0:
                if loop_x == 1:
                    print(
                        f"No fire recorded on {str(row['s_date'])}, it is unknown if a fire occurred the year "
                        f"before - nan appended")
                    years_since_list.append(np.nan)
                else:

                    no_fire_list.append(1)
                    print(
                        f"No fire recorded on {str(row['s_date'])}, however, fire was recorded {str(len(no_fire_list))}"
                        f" year ago - {str(len(no_fire_list))} appended")
                    years_since_list.append(len(no_fire_list))

            else:
                if loop_x == 1:
                    print(
                        f"Fire recorded on {str(row['s_date'])}, it is unknown if a fire occurred "
                        f"the year before - nan appended")
                    years_since_list.append(np.nan)
                else:
                    print(
                        f"Fire recorded on {str(row['s_date'])}, and fire was recorded {str(len(no_fire_list) + 1)} "
                        f"year ago - {str(len(no_fire_list) + 1)} appended")
                    years_since_list.append(len(no_fire_list) + 1)
                    no_fire_list = []
            loop_x += 1

        print("years_since: ", years_since_list)
        df1["since_fire"] = years_since_list
        list_.append(df1)

    df3 = pd.concat(list_, axis=0)
    return (df3)

    return (df3)


def fire_gap_fn(df):
    list_ = []
    df.dropna(inplace=True)

    for i in df.site_clean.unique():
        years_since_list = []
        print(i)
        df1 = df[df["site_clean"] == i]

        x = df1.since_fire.mean()
        print("x: ", x)

        df1["fire_gap"] = x

        list_.append(df1)

    df2 = pd.concat(list_, axis=0)

    return df2


def ratio_fire_year_fn(x, y, p, n):
    # data number of fires per data length of time = x/y
    # revised time frame i.e. number of fires per time restriction = p/n

    final = (x * n) / (p * y)

    return final


def poisson_fn(df, p, n):
    list_ = []
    df.dropna(inplace=True)

    for i in df.site_clean.unique():
        years_since_list = []
        print(i)
        df1 = df[df["site_clean"] == i]

        x = df1["fire_tot"].tolist()[0]  # average number of fires per data total years
        y = len(df1.index) + 1  # total number of years
        n = n  # number of year time frame
        p = p  # how many fires per n

        k = np.arange(0, n + 1)
        # print(k)

        m = ratio_fire_year_fn(x, y, p, n)
        print(m)

        pmf = poisson.pmf(k, mu=m)
        pmf = np.round(pmf, 5)

        print(pmf)

        for val, prob in zip(k, pmf):
            if val == p:
                print(f"Within a {n} period, there is a {prob * 100} that {val} fires will occur.")

        df1[f"fire_pois{p}_{n}"] = prob * 100

        list_.append(df1)

    df2 = pd.concat(list_, axis=0)

    return df2


def double_digit_month_fn(d, year_):
    print(d)

    if int(d) < 10:
        month_ = f"0{d}"
    else:
        month_ = f"{d}"
    year_month = f"{str(year_)}{month_}"

    return year_month


def fire_scar_year_month_fn(df, month_list, month_d_list):
    # todo this is where _1ha is coming back in
    """ Calculate the percent cover burnt by fire. """
    df.fillna(0, inplace=True)
    final_site_list = []
    burnt_start_list = []
    burnt_end_list = []
    burnt_year_list = []

    print(df.columns)
    # correct df site name
    site_list = []
    site_name = df["site"].tolist()
    for i in site_name:
        print(i)
        n = i.replace("_1ha", "")
        print("new_name: ", n)
        site_list.append(n)

    df["site_clean"] = site_list

    print("firescar : ", df)
    print("site list", site_list)

    for index, row in df.iterrows():
        month__ = []
        final_site_list.append(row.site_clean)
        burnt_year_list.append(str(row.im_date))
        burnt_month_list = []
        for month, d in zip(month_list, month_d_list):

            if int(row[f"{month}"]) > 0:

                month__.append(d)

                if d < 10:
                    month_ = f"0{d}"
                else:
                    month_ = f"{d}"
                year_month = f"{str(row.im_date)}{month_}"
                burnt_month_list.append(year_month)

        acend_month_list = sorted(month__, reverse=False)

        if month__:
            if len(month__) > 1:
                year_month = double_digit_month_fn(str(month__[0]), str(row.im_date))
                print(f"{row.site} has the following fire scars: {acend_month_list}")
                burnt_start_list.append(year_month)
                burnt_end_list.append(year_month)


            else:
                # calculate first fire scar
                year_month = double_digit_month_fn(str(month__[0]), str(row.im_date))
                print(f"{row.site} has the following fire scars: {acend_month_list}")
                burnt_start_list.append(year_month)

                # calculate last fire scar
                year_month = double_digit_month_fn(str(month__[-1]), str(row.im_date))
                print(f"{row.site} has the following fire scars: {acend_month_list}")
                burnt_end_list.append(year_month)

        else:
            burnt_start_list.append(0)
            burnt_end_list.append(0)

    return final_site_list, burnt_start_list, burnt_end_list, burnt_year_list


def export_csv_file_fn(df, dir_, file_name):
    output_path = os.path.join(dir_, file_name)
    df.to_csv(os.path.join(output_path), index=False)
    print("File output to: ", output_path)


def fire_month_mask(df, output_dir):
    list_ = []
    site_list = []
    st_list = []
    end_list = []
    y_list = []
    ym_list = []

    fire_mask_dir = os.path.join(output_dir, "fire_mask")

    mk_dir_fn(fire_mask_dir)
    #     if not os.path.isdir(fire_mask_dir):
    #         os.mkdir(fire_mask_dir)

    for i in df.site_clean.unique():

        df_site = df[df["site_clean"] == i]

        for index, row in df_site.iterrows():
            st = (row['bnt_st_ym'])
            end = (row['bn_end_ym'])
            year = (row['bn_year'])

            if st == 0 and end == 0:
                #                 print(i)
                #                 print(st)

                for n in range(1, 13):

                    if n < 10:
                        z = "0" + str(n)
                    else:
                        z = str(n)

                    ym = str(year) + z

                    # load values to lists
                    site_list.append(str(i))
                    st_list.append(int(st))
                    end_list.append(int(end))
                    y_list.append(int(year))
                    ym_list.append(int(ym))

            else:
                # convert to string
                st_ = str(st)
                end_ = str(end)

                # separate month
                st_month = st_[4:]
                end_month = end_[4:]

                # convert_to_int
                st_int_month = int(st_month)
                end_int_month = int(end_month)

                # start date
                if st_int_month > 10:
                    st_m_str = st_month[1:]
                else:
                    st_m_str = st_month

                for n in range(1, int(st_m_str)):
                    #                     print(n)

                    if n < 10:
                        z = "0" + str(n)
                    else:
                        z = str(n)

                    ym = str(year) + z

                    # load values to lists
                    site_list.append(str(i))
                    st_list.append(int(st))
                    end_list.append(int(end))
                    y_list.append(int(year))
                    ym_list.append(int(ym))

    fire_ym_mask = pd.DataFrame()

    fire_ym_mask["site_clean"] = site_list
    fire_ym_mask["st_fs"] = st_list
    fire_ym_mask["end_fs"] = end_list
    fire_ym_mask["year"] = y_list
    fire_ym_mask["ym_bfr_fs"] = ym_list

    output_path = os.path.join(fire_mask_dir, "fire_ym_before_fire_scar.csv")
    fire_ym_mask.to_csv(os.path.join(output_path), index=False)
    print("File output to: ", output_path)

    return fire_ym_mask


def seasonal_fpca2_ym(df):
    list_ = []

    uid_list = []
    site_list = []
    image_list = []
    st_list = []
    end_list = []
    y_list = []
    ym_list = []

    count_list = []
    min_list = []
    max_list = []
    mean_list = []
    med_list = []
    std_list = []
    p25_list = []
    p50_list = []
    p75_list = []
    p95_list = []
    p99_list = []
    range_list = []
    image_s_dt_list = []
    dt_year_list = []

    print(df.columns)
    print("init seasonal_fpca2_" * 20)
    for i in df.site_clean.unique():

        df_site = df[df["site_clean"] == i]
        print('working on site: ', i)
        for index, row in df_site.iterrows():

            st = (row['s_month'])
            end = (row['e_month'])
            year = (row['s_year'])
            uid = (row['uid'])
            image = (row['image'])
            count_ = (row['b1_fpca2_count'])
            min_ = (row['b1_fpca2_min'])
            max_ = (row['b1_fpca2_max'])
            mean_ = (row['b1_fpca2_mean'])
            med_ = (row['b1_fpca2_med'])
            std_ = (row['b1_fpca2_std'])
            p25_ = (row['b1_fpca2_p25'])
            p50_ = (row['b1_fpca2_p50'])
            p75_ = (row['b1_fpca2_p75'])
            p95_ = (row['b1_fpca2_p95'])
            p99_ = (row['b1_fpca2_p99'])
            range_ = (row['b1_fpca2_range'])
            image_s_dt = (row['image_s_dt'])
            dt_year = (row['dt_year'])

            # convert to string
            st_month = str(st)
            end_month = str(end)

            print("st_month: ", st_month)

            # convert_to_int
            st_int_month = int(st_month)
            end_int_month = int(end_month)

            print("st_int_month: ", str(st_int_month))
            for n in range(1, int(st_int_month)):
                print(n)

                if n < 10:
                    z = "0" + str(n)
                else:
                    z = str(n)

                ym = str(year) + z

                # load values to lists
                site_list.append(str(i))
                st_list.append(int(st))
                end_list.append(int(end))
                y_list.append(int(year))
                ym_list.append(int(ym))
                uid_list.append(int(uid))
                image_list = str(image)
                count_list.append(int(count_))
                min_list.append(float(min_))
                max_list.append(float(max_))
                mean_list.append(float(mean_))
                med_list.append(float(med_))
                std_list.append(float(std_))
                p25_list.append(float(p25_))
                p50_list.append(float(p50_))
                p75_list.append(float(p75_))
                p95_list.append(float(p95_))
                p99_list.append(float(p99_))
                range_list.append(float(range_))
                image_s_dt_list.append(str(image_s_dt))
                dt_year_list.append(int(dt_year))

        data = {'uid': uid_list,
                'site_clean': site_list,
                'image': image_list,
                'image_s_dt': image_s_dt_list,
                'dt_year': dt_year_list,
                'dt_ym': ym_list,
                'fpca2_count': count_list,
                'fpca2_min': min_list,
                'fpca2_max': max_list,
                'fpca2_mean': mean_list,
                'fpca2_med': med_list,
                'fpca2_std': std_list,
                'fpca2_p25': p25_list,
                'fpca2_p50': p50_list,
                'fpca2_p75': p75_list,
                'fpca2_p95': p95_list,
                'fpca2_p99': p99_list,
                'fpca2_range': range_list,

                }

    if len(mean_list) > 0:
        out_df = pd.DataFrame.from_dict(data, orient='columns')
        print("out_df len : ", out_df.shape)
        list_.append(out_df)

        print("out_df: ", out_df)
    else:
        out_df = out_df = pd.DataFrame()

    return out_df


def seasonal_var_ym(df, var_):
    list_ = []

    uid_list = []
    site_list = []
    image_list = []
    st_list = []
    end_list = []
    y_list = []
    ym_list = []

    count_list = []
    min_list = []
    max_list = []
    mean_list = []
    med_list = []
    std_list = []
    p25_list = []
    p50_list = []
    p75_list = []
    p95_list = []
    p99_list = []
    range_list = []
    image_s_dt_list = []
    dt_year_list = []

    print(df.columns)
    print("init seasonal_var_" * 20)
    print("var_: ", var_)
    print(df)
    for i in df.site_clean.unique():

        df_site = df[df["site_clean"] == i]
        print('working on site: ', i)
        for index, row in df_site.iterrows():

            st = (row['s_month'])
            end = (row['e_month'])
            year = (row['s_year'])
            uid = (row['uid'])

            if var_ == "dis":
                image = (row[f'{var_}_image'])
            else:

                image = (row['image'])
            count_ = (row[f'b1_{var_}_count'])
            min_ = (row[f'b1_{var_}_min'])
            max_ = (row[f'b1_{var_}_max'])
            mean_ = (row[f'b1_{var_}_mean'])
            med_ = (row[f'b1_{var_}_med'])
            std_ = (row[f'b1_{var_}_std'])
            p25_ = (row[f'b1_{var_}_p25'])
            p50_ = (row[f'b1_{var_}_p50'])
            p75_ = (row[f'b1_{var_}_p75'])
            p95_ = (row[f'b1_{var_}_p95'])
            p99_ = (row[f'b1_{var_}_p99'])
            range_ = (row[f'b1_{var_}_range'])
            image_s_dt = (row[f'image_s_dt'])
            dt_year = (row['dt_year'])

            # convert to string
            st_month = str(st)
            end_month = str(end)

            print("st_month: ", st_month)

            # convert_to_int
            st_int_month = int(st_month)
            end_int_month = int(end_month)

            print("st_int_month: ", str(st_int_month))
            for n in range(1, int(st_int_month)):
                print(n)

                if n < 10:
                    z = "0" + str(n)
                else:
                    z = str(n)

                ym = str(year) + z

                # load values to lists
                site_list.append(str(i))
                st_list.append(int(st))
                end_list.append(int(end))
                y_list.append(int(year))
                ym_list.append(int(ym))
                uid_list.append(int(uid))
                image_list = str(image)
                count_list.append(int(count_))
                min_list.append(float(min_))
                max_list.append(float(max_))
                mean_list.append(float(mean_))
                med_list.append(float(med_))
                std_list.append(float(std_))
                p25_list.append(float(p25_))
                p50_list.append(float(p50_))
                p75_list.append(float(p75_))
                p95_list.append(float(p95_))
                p99_list.append(float(p99_))
                range_list.append(float(range_))
                image_s_dt_list.append(str(image_s_dt))
                dt_year_list.append(int(dt_year))

    data = {'uid': uid_list,
            'site_clean': site_list,
            'image': image_list,
            'image_s_dt': image_s_dt_list,
            'dt_year': dt_year_list,
            'dt_ym': ym_list,
            f'{var_}_count': count_list,
            f'{var_}_min': min_list,
            f'{var_}_max': max_list,
            f'{var_}_mean': mean_list,
            f'{var_}_med': med_list,
            f'{var_}_std': std_list,
            f'{var_}_p25': p25_list,
            f'{var_}_p50': p50_list,
            f'{var_}_p75': p75_list,
            f'{var_}_p95': p95_list,
            f'{var_}_p99': p99_list,
            f'{var_}_range': range_list,

            }

    if len(mean_list) > 0:
        out_df = pd.DataFrame.from_dict(data, orient='columns')
        print("out_df len : ", out_df.shape)
        list_.append(out_df)

        print("out_df: ", out_df)
    else:
        out_df = out_df = pd.DataFrame()

    return out_df


def seasonal_dis_ym(df, var_):
    list_ = []

    uid_list = []
    site_list = []
    image_list = []
    st_list = []
    end_list = []
    y_list = []
    ym_list = []

    count_list = []
    min_list = []
    max_list = []
    mean_list = []
    med_list = []
    std_list = []
    major_list = []
    minor_list = []
    #     range_list = []
    one_list = []
    two_list = []
    three_list = []
    four_list = []
    five_list = []
    six_list = []
    seven_list = []
    eight_list = []
    nine_list = []
    ten_list = []
    image_s_dt_list = []
    dt_year_list = []

    print(df.columns)
    print("init seasonal_var_" * 20)
    print("var_: ", var_)
    print(df)
    for i in df.site_clean.unique():

        df_site = df[df["site_clean"] == i]
        print('working on site: ', i)
        for index, row in df_site.iterrows():

            st = (row['s_month'])
            end = (row['e_month'])
            year = (row['s_year'])
            uid = (row['uid'])
            image = (row[f'{var_}_image'])
            count_ = (row[f'{var_}_count'])
            min_ = (row[f'{var_}_min'])
            max_ = (row[f'{var_}_max'])
            mean_ = (row[f'{var_}_mean'])
            med_ = (row[f'{var_}_med'])
            std_ = (row[f'{var_}_std'])
            major_ = (row[f'{var_}_major'])
            minor_ = (row[f'{var_}_minor'])
            #             range_ = (row[f'{var_}_range'])
            one_ = (row[f'{var_}_one'])
            two_ = (row[f'{var_}_two'])
            three_ = (row[f'{var_}_three'])
            four_ = (row[f'{var_}_four'])
            five_ = (row[f'{var_}_five'])
            six_ = (row[f'{var_}_six'])
            seven_ = (row[f'{var_}_seven'])
            eight_ = (row[f'{var_}_eight'])
            nine_ = (row[f'{var_}_nine'])
            ten_ = (row[f'{var_}_ten'])
            image_s_dt = (row[f'image_s_dt'])
            dt_year = (row['dt_year'])

            # convert to string
            st_month = str(st)
            end_month = str(end)

            print("st_month: ", st_month)

            # convert_to_int
            st_int_month = int(st_month)
            end_int_month = int(end_month)

            print("st_int_month: ", str(st_int_month))
            for n in range(1, int(st_int_month)):
                print(n)

                if n < 10:
                    z = "0" + str(n)
                else:
                    z = str(n)

                ym = str(year) + z

                # load values to lists
                site_list.append(str(i))
                st_list.append(int(st))
                end_list.append(int(end))
                y_list.append(int(year))
                ym_list.append(int(ym))
                uid_list.append(int(uid))
                image_list = str(image)
                count_list.append(int(count_))
                min_list.append(float(min_))
                max_list.append(float(max_))
                mean_list.append(float(mean_))
                med_list.append(float(med_))
                std_list.append(float(std_))
                major_list.append(float(major_))
                minor_list.append(float(minor_))
                one_list.append(float(one_))
                two_list.append(float(two_))
                three_list.append(float(three_))
                four_list.append(float(four_))
                five_list.append(float(five_))
                six_list.append(float(six_))
                seven_list.append(float(seven_))
                eight_list.append(float(eight_))
                nine_list.append(float(nine_))
                ten_list.append(float(ten_))
                #                 range_list.append(float(range_))
                image_s_dt_list.append(str(image_s_dt))
                dt_year_list.append(int(dt_year))

        data = {'uid': uid_list,
                'site_clean': site_list,
                'image': image_list,
                'image_s_dt': image_s_dt_list,
                'dt_year': dt_year_list,
                'dt_ym': ym_list,
                f'{var_}_count': count_list,
                f'{var_}_min': min_list,
                f'{var_}_max': max_list,
                f'{var_}_mean': mean_list,
                f'{var_}_med': med_list,
                f'{var_}_std': std_list,
                f'{var_}_major': major_list,
                f'{var_}_minor': minor_list,
                #                     f'{var_}_range' : range_list,
                f'{var_}_one': one_list,
                f'{var_}_two': two_list,
                f'{var_}_three': three_list,
                f'{var_}_four': four_list,
                f'{var_}_five': five_list,
                f'{var_}_six': six_list,
                f'{var_}_seven': seven_list,
                f'{var_}_eight': eight_list,
                f'{var_}_nine': nine_list,
                f'{var_}_ten': ten_list,

                }

    if len(mean_list) > 0:
        out_df = pd.DataFrame.from_dict(data, orient='columns')
        print("out_df len : ", out_df.shape)
        list_.append(out_df)

        print("out_df: ", out_df)
    else:
        out_df = out_df = pd.DataFrame()

    print('-' * 50)
    print(out_df.columns)
    print('-' * 50)

    return out_df


def seasonal_dbi_ym(df):
    print(df.columns)

    list_ = []

    uid_list = []
    site_list = []
    image_list = []
    st_list = []
    end_list = []
    y_list = []
    ym_list = []

    b1_count_list = []
    b1_min_list = []
    b1_max_list = []
    b1_mean_list = []
    b1_med_list = []
    b1_std_list = []
    b1_p25_list = []
    b1_p50_list = []
    b1_p75_list = []
    b1_p95_list = []
    b1_p99_list = []
    b1_range_list = []

    b2_count_list = []
    b2_min_list = []
    b2_max_list = []
    b2_mean_list = []
    b2_med_list = []
    b2_std_list = []
    b2_p25_list = []
    b2_p50_list = []
    b2_p75_list = []
    b2_p95_list = []
    b2_p99_list = []
    b2_range_list = []

    b3_count_list = []
    b3_min_list = []
    b3_max_list = []
    b3_mean_list = []
    b3_med_list = []
    b3_std_list = []
    b3_p25_list = []
    b3_p50_list = []
    b3_p75_list = []
    b3_p95_list = []
    b3_p99_list = []
    b3_range_list = []

    b4_count_list = []
    b4_min_list = []
    b4_max_list = []
    b4_mean_list = []
    b4_med_list = []
    b4_std_list = []
    b4_p25_list = []
    b4_p50_list = []
    b4_p75_list = []
    b4_p95_list = []
    b4_p99_list = []
    b4_range_list = []

    b5_count_list = []
    b5_min_list = []
    b5_max_list = []
    b5_mean_list = []
    b5_med_list = []
    b5_std_list = []
    b5_p25_list = []
    b5_p50_list = []
    b5_p75_list = []
    b5_p95_list = []
    b5_p99_list = []
    b5_range_list = []

    b6_count_list = []
    b6_min_list = []
    b6_max_list = []
    b6_mean_list = []
    b6_med_list = []
    b6_std_list = []
    b6_p25_list = []
    b6_p50_list = []
    b6_p75_list = []
    b6_p95_list = []
    b6_p99_list = []
    b6_range_list = []

    image_s_dt_list = []
    dt_year_list = []


    print(df.columns)
    print("init seasonal_fpca2_" * 20)
    for i in df.site_clean.unique():

        df_site = df[df["site_clean"] == i]
        print('working on site: ', i)
        for index, row in df_site.iterrows():

            st = (row['s_month'])
            end = (row['e_month'])
            year = (row['s_year'])
            uid = (row['uid'])
            image = (row['image'])
            count_1 = (row['b1_dbi_count'])
            min_1 = (row['b1_dbi_min'])
            max_1 = (row['b1_dbi_max'])
            mean_1 = (row['b1_dbi_mean'])
            med_1 = (row['b1_dbi_med'])
            std_1 = (row['b1_dbi_std'])
            p25_1 = (row['b1_dbi_p25'])
            p50_1 = (row['b1_dbi_p50'])
            p75_1 = (row['b1_dbi_p75'])
            p95_1 = (row['b1_dbi_p95'])
            p99_1 = (row['b1_dbi_p99'])
            range_1 = (row['b1_dbi_range'])

            count_2 = (row['b2_dbi_count'])
            min_2 = (row['b2_dbi_min'])
            max_2 = (row['b2_dbi_max'])
            mean_2 = (row['b2_dbi_mean'])
            med_2 = (row['b2_dbi_med'])
            std_2 = (row['b2_dbi_std'])
            p25_2 = (row['b2_dbi_p25'])
            p50_2 = (row['b2_dbi_p50'])
            p75_2 = (row['b2_dbi_p75'])
            p95_2 = (row['b2_dbi_p95'])
            p99_2 = (row['b2_dbi_p99'])
            range_2 = (row['b2_dbi_range'])

            count_3 = (row['b3_dbi_count'])
            min_3 = (row['b3_dbi_min'])
            max_3 = (row['b3_dbi_max'])
            mean_3 = (row['b3_dbi_mean'])
            med_3 = (row['b3_dbi_med'])
            std_3 = (row['b3_dbi_std'])
            p25_3 = (row['b3_dbi_p25'])
            p50_3 = (row['b3_dbi_p50'])
            p75_3 = (row['b3_dbi_p75'])
            p95_3 = (row['b3_dbi_p95'])
            p99_3 = (row['b3_dbi_p99'])
            range_3 = (row['b3_dbi_range'])

            count_4 = (row['b4_dbi_count'])
            min_4 = (row['b4_dbi_min'])
            max_4 = (row['b4_dbi_max'])
            mean_4 = (row['b4_dbi_mean'])
            med_4 = (row['b4_dbi_med'])
            std_4 = (row['b4_dbi_std'])
            p25_4 = (row['b4_dbi_p25'])
            p50_4 = (row['b4_dbi_p50'])
            p75_4 = (row['b4_dbi_p75'])
            p95_4 = (row['b4_dbi_p95'])
            p99_4 = (row['b4_dbi_p99'])
            range_4 = (row['b4_dbi_range'])

            count_5 = (row['b5_dbi_count'])
            min_5 = (row['b5_dbi_min'])
            max_5 = (row['b5_dbi_max'])
            mean_5 = (row['b5_dbi_mean'])
            med_5 = (row['b5_dbi_med'])
            std_5 = (row['b5_dbi_std'])
            p25_5 = (row['b5_dbi_p25'])
            p50_5 = (row['b5_dbi_p50'])
            p75_5 = (row['b5_dbi_p75'])
            p95_5 = (row['b5_dbi_p95'])
            p99_5 = (row['b5_dbi_p99'])
            range_5 = (row['b5_dbi_range'])

            count_6 = (row['b6_dbi_count'])
            min_6 = (row['b6_dbi_min'])
            max_6 = (row['b6_dbi_max'])
            mean_6 = (row['b6_dbi_mean'])
            med_6 = (row['b6_dbi_med'])
            std_6 = (row['b6_dbi_std'])
            p25_6 = (row['b6_dbi_p25'])
            p50_6 = (row['b6_dbi_p50'])
            p75_6 = (row['b6_dbi_p75'])
            p95_6 = (row['b6_dbi_p95'])
            p99_6 = (row['b6_dbi_p99'])
            range_6 = (row['b6_dbi_range'])

            image_s_dt = (row['image_s_dt'])
            dt_year = (row['dt_year'])

            # convert to string
            st_month = str(st)
            end_month = str(end)

            print("st_month: ", st_month)

            # convert_to_int
            st_int_month = int(st_month)
            end_int_month = int(end_month)

            print("st_int_month: ", str(st_int_month))
            for n in range(1, int(st_int_month)):
                print(n)

                if n < 10:
                    z = "0" + str(n)
                else:
                    z = str(n)

                ym = str(year) + z

                # load values to lists
                site_list.append(str(i))
                st_list.append(int(st))
                end_list.append(int(end))
                y_list.append(int(year))
                ym_list.append(int(ym))
                uid_list.append(int(uid))
                image_list = str(image)
                b1_count_list.append(int(count_1))
                b1_min_list.append(float(min_1))
                b1_max_list.append(float(max_1))
                b1_mean_list.append(float(mean_1))
                b1_med_list.append(float(med_1))
                b1_std_list.append(float(std_1))
                b1_p25_list.append(float(p25_1))
                b1_p50_list.append(float(p50_1))
                b1_p75_list.append(float(p75_1))
                b1_p95_list.append(float(p95_1))
                b1_p99_list.append(float(p99_1))
                b1_range_list.append(float(range_1))

                b2_count_list.append(int(count_2))
                b2_min_list.append(float(min_2))
                b2_max_list.append(float(max_2))
                b2_mean_list.append(float(mean_2))
                b2_med_list.append(float(med_2))
                b2_std_list.append(float(std_2))
                b2_p25_list.append(float(p25_2))
                b2_p50_list.append(float(p50_2))
                b2_p75_list.append(float(p75_2))
                b2_p95_list.append(float(p95_2))
                b2_p99_list.append(float(p99_2))
                b2_range_list.append(float(range_2))

                b3_count_list.append(int(count_3))
                b3_min_list.append(float(min_3))
                b3_max_list.append(float(max_3))
                b3_mean_list.append(float(mean_3))
                b3_med_list.append(float(med_3))
                b3_std_list.append(float(std_3))
                b3_p25_list.append(float(p25_3))
                b3_p50_list.append(float(p50_3))
                b3_p75_list.append(float(p75_3))
                b3_p95_list.append(float(p95_3))
                b3_p99_list.append(float(p99_3))
                b3_range_list.append(float(range_3))

                b4_count_list.append(int(count_4))
                b4_min_list.append(float(min_4))
                b4_max_list.append(float(max_4))
                b4_mean_list.append(float(mean_4))
                b4_med_list.append(float(med_4))
                b4_std_list.append(float(std_4))
                b4_p25_list.append(float(p25_4))
                b4_p50_list.append(float(p50_4))
                b4_p75_list.append(float(p75_4))
                b4_p95_list.append(float(p95_4))
                b4_p99_list.append(float(p99_4))
                b4_range_list.append(float(range_4))

                b5_count_list.append(int(count_5))
                b5_min_list.append(float(min_5))
                b5_max_list.append(float(max_5))
                b5_mean_list.append(float(mean_5))
                b5_med_list.append(float(med_5))
                b5_std_list.append(float(std_5))
                b5_p25_list.append(float(p25_5))
                b5_p50_list.append(float(p50_5))
                b5_p75_list.append(float(p75_5))
                b5_p95_list.append(float(p95_5))
                b5_p99_list.append(float(p99_5))
                b5_range_list.append(float(range_5))

                b6_count_list.append(int(count_6))
                b6_min_list.append(float(min_6))
                b6_max_list.append(float(max_6))
                b6_mean_list.append(float(mean_6))
                b6_med_list.append(float(med_6))
                b6_std_list.append(float(std_6))
                b6_p25_list.append(float(p25_6))
                b6_p50_list.append(float(p50_6))
                b6_p75_list.append(float(p75_6))
                b6_p95_list.append(float(p95_6))
                b6_p99_list.append(float(p99_6))
                b6_range_list.append(float(range_6))

                image_s_dt_list.append(str(image_s_dt))
                dt_year_list.append(int(dt_year))

        data = {'uid': uid_list,
                'site_clean': site_list,
                'image': image_list,
                'image_s_dt': image_s_dt_list,
                'dt_year': dt_year_list,
                'dt_ym': ym_list,
                'b1_dbi_count': b1_count_list,
                'b1_dbi_min': b1_min_list,
                'b1_dbi_max': b1_max_list,
                'b1_dbi_mean': b1_mean_list,
                'b1_dbi_med': b1_med_list,
                'b1_dbi_std': b1_std_list,
                'b1_dbi_p25': b1_p25_list,
                'b1_dbi_p50': b1_p50_list,
                'b1_dbi_p75': b1_p75_list,
                'b1_dbi_p95': b1_p95_list,
                'b1_dbi_p99': b1_p99_list,
                'b1_dbi_range': b1_range_list,

                'b2_dbi_count': b2_count_list,
                'b2_dbi_min': b2_min_list,
                'b2_dbi_max': b2_max_list,
                'b2_dbi_mean': b2_mean_list,
                'b2_dbi_med': b2_med_list,
                'b2_dbi_std': b2_std_list,
                'b2_dbi_p25': b2_p25_list,
                'b2_dbi_p50': b2_p50_list,
                'b2_dbi_p75': b2_p75_list,
                'b2_dbi_p95': b2_p95_list,
                'b2_dbi_p99': b2_p99_list,
                'b2_dbi_range': b2_range_list,

                'b3_dbi_count': b3_count_list,
                'b3_dbi_min': b3_min_list,
                'b3_dbi_max': b3_max_list,
                'b3_dbi_mean': b3_mean_list,
                'b3_dbi_med': b3_med_list,
                'b3_dbi_std': b3_std_list,
                'b3_dbi_p25': b3_p25_list,
                'b3_dbi_p50': b3_p50_list,
                'b3_dbi_p75': b3_p75_list,
                'b3_dbi_p95': b3_p95_list,
                'b3_dbi_p99': b3_p99_list,
                'b3_dbi_range': b3_range_list,

                'b4_dbi_count': b4_count_list,
                'b4_dbi_min': b4_min_list,
                'b4_dbi_max': b4_max_list,
                'b4_dbi_mean': b4_mean_list,
                'b4_dbi_med': b4_med_list,
                'b4_dbi_std': b4_std_list,
                'b4_dbi_p25': b4_p25_list,
                'b4_dbi_p50': b4_p50_list,
                'b4_dbi_p75': b4_p75_list,
                'b4_dbi_p95': b4_p95_list,
                'b4_dbi_p99': b4_p99_list,
                'b4_dbi_range': b4_range_list,

                'b5_dbi_count': b5_count_list,
                'b5_dbi_min': b5_min_list,
                'b5_dbi_max': b5_max_list,
                'b5_dbi_mean': b5_mean_list,
                'b5_dbi_med': b5_med_list,
                'b5_dbi_std': b5_std_list,
                'b5_dbi_p25': b5_p25_list,
                'b5_dbi_p50': b5_p50_list,
                'b5_dbi_p75': b5_p75_list,
                'b5_dbi_p95': b5_p95_list,
                'b5_dbi_p99': b5_p99_list,
                'b5_dbi_range': b5_range_list,

                'b6_dbi_count': b6_count_list,
                'b6_dbi_min': b6_min_list,
                'b6_dbi_max': b6_max_list,
                'b6_dbi_mean': b6_mean_list,
                'b6_dbi_med': b6_med_list,
                'b6_dbi_std': b6_std_list,
                'b6_dbi_p25': b6_p25_list,
                'b6_dbi_p50': b6_p50_list,
                'b6_dbi_p75': b6_p75_list,
                'b6_dbi_p95': b6_p95_list,
                'b6_dbi_p99': b6_p99_list,
                'b6_dbi_range': b6_range_list,

                }

    if len(b1_mean_list) > 0:
        out_df = pd.DataFrame.from_dict(data, orient='columns')
        print("out_df len : ", out_df.shape)
        list_.append(out_df)

        print("out_df: ", out_df)
    else:
        out_df = out_df = pd.DataFrame()

    return out_df


def seasonal_dim_ym(df):
    print(df.columns)

    list_ = []

    uid_list = []
    site_list = []
    image_list = []
    st_list = []
    end_list = []
    y_list = []
    ym_list = []

    b1_count_list = []
    b1_min_list = []
    b1_max_list = []
    b1_mean_list = []
    b1_med_list = []
    b1_std_list = []
    b1_p25_list = []
    b1_p50_list = []
    b1_p75_list = []
    b1_p95_list = []
    b1_p99_list = []
    b1_range_list = []

    b2_count_list = []
    b2_min_list = []
    b2_max_list = []
    b2_mean_list = []
    b2_med_list = []
    b2_std_list = []
    b2_p25_list = []
    b2_p50_list = []
    b2_p75_list = []
    b2_p95_list = []
    b2_p99_list = []
    b2_range_list = []

    b3_count_list = []
    b3_min_list = []
    b3_max_list = []
    b3_mean_list = []
    b3_med_list = []
    b3_std_list = []
    b3_p25_list = []
    b3_p50_list = []
    b3_p75_list = []
    b3_p95_list = []
    b3_p99_list = []
    b3_range_list = []

    image_s_dt_list = []
    dt_year_list = []

    print(df.columns)
    print("init seasonal_fpca2_" * 20)
    for i in df.site_clean.unique():

        df_site = df[df["site_clean"] == i]
        print('working on site: ', i)
        for index, row in df_site.iterrows():

            st = (row['s_month'])
            end = (row['e_month'])
            year = (row['s_year'])
            uid = (row['uid'])
            image = (row['image'])
            count_1 = (row['b1_dim_count'])
            min_1 = (row['b1_dim_min'])
            max_1 = (row['b1_dim_max'])
            mean_1 = (row['b1_dim_mean'])
            med_1 = (row['b1_dim_med'])
            std_1 = (row['b1_dim_std'])
            p25_1 = (row['b1_dim_p25'])
            p50_1 = (row['b1_dim_p50'])
            p75_1 = (row['b1_dim_p75'])
            p95_1 = (row['b1_dim_p95'])
            p99_1 = (row['b1_dim_p99'])
            range_1 = (row['b1_dim_range'])

            count_2 = (row['b2_dim_count'])
            min_2 = (row['b2_dim_min'])
            max_2 = (row['b2_dim_max'])
            mean_2 = (row['b2_dim_mean'])
            med_2 = (row['b2_dim_med'])
            std_2 = (row['b2_dim_std'])
            p25_2 = (row['b2_dim_p25'])
            p50_2 = (row['b2_dim_p50'])
            p75_2 = (row['b2_dim_p75'])
            p95_2 = (row['b2_dim_p95'])
            p99_2 = (row['b2_dim_p99'])
            range_2 = (row['b2_dim_range'])

            count_3 = (row['b3_dim_count'])
            min_3 = (row['b3_dim_min'])
            max_3 = (row['b3_dim_max'])
            mean_3 = (row['b3_dim_mean'])
            med_3 = (row['b3_dim_med'])
            std_3 = (row['b3_dim_std'])
            p25_3 = (row['b3_dim_p25'])
            p50_3 = (row['b3_dim_p50'])
            p75_3 = (row['b3_dim_p75'])
            p95_3 = (row['b3_dim_p95'])
            p99_3 = (row['b3_dim_p99'])
            range_3 = (row['b3_dim_range'])

            image_s_dt = (row['image_s_dt'])
            dt_year = (row['dt_year'])

            # convert to string
            st_month = str(st)
            end_month = str(end)

            print("st_month: ", st_month)

            # convert_to_int
            st_int_month = int(st_month)
            end_int_month = int(end_month)

            print("st_int_month: ", str(st_int_month))
            for n in range(1, int(st_int_month)):
                print(n)

                if n < 10:
                    z = "0" + str(n)
                else:
                    z = str(n)

                ym = str(year) + z

                # load values to lists
                site_list.append(str(i))
                st_list.append(int(st))
                end_list.append(int(end))
                y_list.append(int(year))
                ym_list.append(int(ym))
                uid_list.append(int(uid))
                image_list = str(image)
                b1_count_list.append(int(count_1))
                b1_min_list.append(float(min_1))
                b1_max_list.append(float(max_1))
                b1_mean_list.append(float(mean_1))
                b1_med_list.append(float(med_1))
                b1_std_list.append(float(std_1))
                b1_p25_list.append(float(p25_1))
                b1_p50_list.append(float(p50_1))
                b1_p75_list.append(float(p75_1))
                b1_p95_list.append(float(p95_1))
                b1_p99_list.append(float(p99_1))
                b1_range_list.append(float(range_1))

                b2_count_list.append(int(count_2))
                b2_min_list.append(float(min_2))
                b2_max_list.append(float(max_2))
                b2_mean_list.append(float(mean_2))
                b2_med_list.append(float(med_2))
                b2_std_list.append(float(std_2))
                b2_p25_list.append(float(p25_2))
                b2_p50_list.append(float(p50_2))
                b2_p75_list.append(float(p75_2))
                b2_p95_list.append(float(p95_2))
                b2_p99_list.append(float(p99_2))
                b2_range_list.append(float(range_2))

                b3_count_list.append(int(count_3))
                b3_min_list.append(float(min_3))
                b3_max_list.append(float(max_3))
                b3_mean_list.append(float(mean_3))
                b3_med_list.append(float(med_3))
                b3_std_list.append(float(std_3))
                b3_p25_list.append(float(p25_3))
                b3_p50_list.append(float(p50_3))
                b3_p75_list.append(float(p75_3))
                b3_p95_list.append(float(p95_3))
                b3_p99_list.append(float(p99_3))
                b3_range_list.append(float(range_3))

                image_s_dt_list.append(str(image_s_dt))
                dt_year_list.append(int(dt_year))

        data = {'uid': uid_list,
                'site_clean': site_list,
                'image': image_list,
                'image_s_dt': image_s_dt_list,
                'dt_year': dt_year_list,
                'dt_ym': ym_list,
                'b1_dim_count': b1_count_list,
                'b1_dim_min': b1_min_list,
                'b1_dim_max': b1_max_list,
                'b1_dim_mean': b1_mean_list,
                'b1_dim_med': b1_med_list,
                'b1_dim_std': b1_std_list,
                'b1_dim_p25': b1_p25_list,
                'b1_dim_p50': b1_p50_list,
                'b1_dim_p75': b1_p75_list,
                'b1_dim_p95': b1_p95_list,
                'b1_dim_p99': b1_p99_list,
                'b1_dim_range': b1_range_list,

                'b2_dim_count': b2_count_list,
                'b2_dim_min': b2_min_list,
                'b2_dim_max': b2_max_list,
                'b2_dim_mean': b2_mean_list,
                'b2_dim_med': b2_med_list,
                'b2_dim_std': b2_std_list,
                'b2_dim_p25': b2_p25_list,
                'b2_dim_p50': b2_p50_list,
                'b2_dim_p75': b2_p75_list,
                'b2_dim_p95': b2_p95_list,
                'b2_dim_p99': b2_p99_list,
                'b2_dim_range': b2_range_list,

                'b3_dim_count': b3_count_list,
                'b3_dim_min': b3_min_list,
                'b3_dim_max': b3_max_list,
                'b3_dim_mean': b3_mean_list,
                'b3_dim_med': b3_med_list,
                'b3_dim_std': b3_std_list,
                'b3_dim_p25': b3_p25_list,
                'b3_dim_p50': b3_p50_list,
                'b3_dim_p75': b3_p75_list,
                'b3_dim_p95': b3_p95_list,
                'b3_dim_p99': b3_p99_list,
                'b3_dim_range': b3_range_list,

                }

    if len(b1_mean_list) > 0:
        out_df = pd.DataFrame.from_dict(data, orient='columns')
        print("out_df len : ", out_df.shape)
        list_.append(out_df)

        print("out_df: ", out_df)
    else:
        out_df = out_df = pd.DataFrame()

    return out_df


def seasonal_dp1_ym(df):
    print(df.columns)

    list_ = []

    uid_list = []
    site_list = []
    image_list = []
    st_list = []
    end_list = []
    y_list = []
    ym_list = []

    b1_count_list = []
    b1_min_list = []
    b1_max_list = []
    b1_mean_list = []
    b1_med_list = []
    b1_std_list = []
    b1_p25_list = []
    b1_p50_list = []
    b1_p75_list = []
    b1_p95_list = []
    b1_p99_list = []
    b1_range_list = []

    b2_count_list = []
    b2_min_list = []
    b2_max_list = []
    b2_mean_list = []
    b2_med_list = []
    b2_std_list = []
    b2_p25_list = []
    b2_p50_list = []
    b2_p75_list = []
    b2_p95_list = []
    b2_p99_list = []
    b2_range_list = []

    b3_count_list = []
    b3_min_list = []
    b3_max_list = []
    b3_mean_list = []
    b3_med_list = []
    b3_std_list = []
    b3_p25_list = []
    b3_p50_list = []
    b3_p75_list = []
    b3_p95_list = []
    b3_p99_list = []
    b3_range_list = []

    image_s_dt_list = []
    dt_year_list = []

    print(df.columns)
    print("init seasonal_fpca2_" * 20)
    for i in df.site_clean.unique():

        df_site = df[df["site_clean"] == i]
        print('working on site: ', i)
        for index, row in df_site.iterrows():

            st = (row['s_month'])
            end = (row['e_month'])
            year = (row['s_year'])
            uid = (row['uid'])
            image = (row['image'])
            count_1 = (row['b1_dp1_count'])
            min_1 = (row['b1_dp1_min'])
            max_1 = (row['b1_dp1_max'])
            mean_1 = (row['b1_dp1_mean'])
            med_1 = (row['b1_dp1_med'])
            std_1 = (row['b1_dp1_std'])
            p25_1 = (row['b1_dp1_p25'])
            p50_1 = (row['b1_dp1_p50'])
            p75_1 = (row['b1_dp1_p75'])
            p95_1 = (row['b1_dp1_p95'])
            p99_1 = (row['b1_dp1_p99'])
            range_1 = (row['b1_dp1_range'])

            count_2 = (row['b2_dp1_count'])
            min_2 = (row['b2_dp1_min'])
            max_2 = (row['b2_dp1_max'])
            mean_2 = (row['b2_dp1_mean'])
            med_2 = (row['b2_dp1_med'])
            std_2 = (row['b2_dp1_std'])
            p25_2 = (row['b2_dp1_p25'])
            p50_2 = (row['b2_dp1_p50'])
            p75_2 = (row['b2_dp1_p75'])
            p95_2 = (row['b2_dp1_p95'])
            p99_2 = (row['b2_dp1_p99'])
            range_2 = (row['b2_dp1_range'])

            count_3 = (row['b3_dp1_count'])
            min_3 = (row['b3_dp1_min'])
            max_3 = (row['b3_dp1_max'])
            mean_3 = (row['b3_dp1_mean'])
            med_3 = (row['b3_dp1_med'])
            std_3 = (row['b3_dp1_std'])
            p25_3 = (row['b3_dp1_p25'])
            p50_3 = (row['b3_dp1_p50'])
            p75_3 = (row['b3_dp1_p75'])
            p95_3 = (row['b3_dp1_p95'])
            p99_3 = (row['b3_dp1_p99'])
            range_3 = (row['b3_dp1_range'])

            image_s_dt = (row['image_s_dt'])
            dt_year = (row['dt_year'])

            # convert to string
            st_month = str(st)
            end_month = str(end)

            print("st_month: ", st_month)

            # convert_to_int
            st_int_month = int(st_month)
            end_int_month = int(end_month)

            print("st_int_month: ", str(st_int_month))
            for n in range(1, int(st_int_month)):
                print(n)

                if n < 10:
                    z = "0" + str(n)
                else:
                    z = str(n)

                ym = str(year) + z

                # load values to lists
                site_list.append(str(i))
                st_list.append(int(st))
                end_list.append(int(end))
                y_list.append(int(year))
                ym_list.append(int(ym))
                uid_list.append(int(uid))
                image_list = str(image)
                b1_count_list.append(int(count_1))
                b1_min_list.append(float(min_1))
                b1_max_list.append(float(max_1))
                b1_mean_list.append(float(mean_1))
                b1_med_list.append(float(med_1))
                b1_std_list.append(float(std_1))
                b1_p25_list.append(float(p25_1))
                b1_p50_list.append(float(p50_1))
                b1_p75_list.append(float(p75_1))
                b1_p95_list.append(float(p95_1))
                b1_p99_list.append(float(p99_1))
                b1_range_list.append(float(range_1))

                b2_count_list.append(int(count_2))
                b2_min_list.append(float(min_2))
                b2_max_list.append(float(max_2))
                b2_mean_list.append(float(mean_2))
                b2_med_list.append(float(med_2))
                b2_std_list.append(float(std_2))
                b2_p25_list.append(float(p25_2))
                b2_p50_list.append(float(p50_2))
                b2_p75_list.append(float(p75_2))
                b2_p95_list.append(float(p95_2))
                b2_p99_list.append(float(p99_2))
                b2_range_list.append(float(range_2))

                b3_count_list.append(int(count_3))
                b3_min_list.append(float(min_3))
                b3_max_list.append(float(max_3))
                b3_mean_list.append(float(mean_3))
                b3_med_list.append(float(med_3))
                b3_std_list.append(float(std_3))
                b3_p25_list.append(float(p25_3))
                b3_p50_list.append(float(p50_3))
                b3_p75_list.append(float(p75_3))
                b3_p95_list.append(float(p95_3))
                b3_p99_list.append(float(p99_3))
                b3_range_list.append(float(range_3))

                image_s_dt_list.append(str(image_s_dt))
                dt_year_list.append(int(dt_year))

        data = {'uid': uid_list,
                'site_clean': site_list,
                'image': image_list,
                'image_s_dt': image_s_dt_list,
                'dt_year': dt_year_list,
                'dt_ym': ym_list,
                'b1_dp1_count': b1_count_list,
                'b1_dp1_min': b1_min_list,
                'b1_dp1_max': b1_max_list,
                'b1_dp1_mean': b1_mean_list,
                'b1_dp1_med': b1_med_list,
                'b1_dp1_std': b1_std_list,
                'b1_dp1_p25': b1_p25_list,
                'b1_dp1_p50': b1_p50_list,
                'b1_dp1_p75': b1_p75_list,
                'b1_dp1_p95': b1_p95_list,
                'b1_dp1_p99': b1_p99_list,
                'b1_dp1_range': b1_range_list,

                'b2_dp1_count': b2_count_list,
                'b2_dp1_min': b2_min_list,
                'b2_dp1_max': b2_max_list,
                'b2_dp1_mean': b2_mean_list,
                'b2_dp1_med': b2_med_list,
                'b2_dp1_std': b2_std_list,
                'b2_dp1_p25': b2_p25_list,
                'b2_dp1_p50': b2_p50_list,
                'b2_dp1_p75': b2_p75_list,
                'b2_dp1_p95': b2_p95_list,
                'b2_dp1_p99': b2_p99_list,
                'b2_dp1_range': b2_range_list,

                'b3_dp1_count': b3_count_list,
                'b3_dp1_min': b3_min_list,
                'b3_dp1_max': b3_max_list,
                'b3_dp1_mean': b3_mean_list,
                'b3_dp1_med': b3_med_list,
                'b3_dp1_std': b3_std_list,
                'b3_dp1_p25': b3_p25_list,
                'b3_dp1_p50': b3_p50_list,
                'b3_dp1_p75': b3_p75_list,
                'b3_dp1_p95': b3_p95_list,
                'b3_dp1_p99': b3_p99_list,
                'b3_dp1_range': b3_range_list,

                }

    if len(b1_mean_list) > 0:
        out_df = pd.DataFrame.from_dict(data, orient='columns')
        print("out_df len : ", out_df.shape)
        list_.append(out_df)

        print("out_df: ", out_df)
    else:
        out_df = out_df = pd.DataFrame()

    return out_df


def year_month_fn(var_filt, var_col):
    ym_list = []
    y_list = []
    for i in var_filt[var_col].tolist():
        n = int(i.month)
        if n < 10:
            z = "0" + str(n)
        else:
            z = str(n)
        x = str(i.year) + z
        ym_list.append(int(x))
        y_list.append(int(i.year))

    return ym_list, y_list


def clean_y_fn(var_df, fire_occ, i):
    # filter variable df by same site_clean
    var_filt = var_df[var_df["site_clean"] == i]
    # test = convert_to_dt_year(test, "s_year", "bn_y")
    var_filt['dt_year'] = var_filt['s_year'].astype(int)
    var_filt.sort_values(by="dt_year", inplace=True)

    # merge fire scar and variable data on the nearest unburnt date
    y_merge = pd.merge(fire_occ, var_filt, on=["site_clean", "dt_year"], how="inner")

    return y_merge, var_filt


def append_y_merge(merge_df, merge_list, column_dict):
    merge_fire_mask = merge_df[merge_list]
    merge_fire_mask.rename(columns=column_dict, inplace=True)

    return merge_fire_mask


def clean_ym_fn(df, var_filt):
    #print(df)

    df.dropna(subset=["image_s_dt"], inplace=True)
    df.sort_values(by="image_s_dt", inplace=True)

    ym_list, y_list = year_month_fn(df, "image_s_dt")

    df.loc[:, "dt_ym"] = ym_list
    df.loc[:, "dt_ym"] = var_filt["dt_ym"].astype(int)
    df.loc[:, "dt_year"] = y_list
    df.loc[:, "dt_year"] = var_filt["dt_year"].astype(int)

    df_ym = df.copy(deep=True)
    #print("var_filt_ym: ", df_ym.shape)

    return (df_ym)


def export_var_fire_scar_zonal_fn(df, dir_, var_, site_clean):
    exp_dir = os.path.join(dir_, var_)
    mk_dir_fn(exp_dir)
    output_path = os.path.join(exp_dir, f"{site_clean}_agb_nt_mosaic_{var_}_fire_scar_zonal.csv")
    df.to_csv((output_path), index=False)


def export_csv_fn(list_, dir_, file_name):
    if len(list_) > 0:
        df_final = pd.concat(list_, axis=0)
        output_path = os.path.join(dir_, file_name)
        df_final.to_csv(os.path.join(output_path), index=False)
        print("File output to: ", output_path)

    else:
        df_final = None

    return df_final


def basal_merge_fire_year(biomass_df, var_df, var_col, var_, loop_site):
    print("biomass_df: ", biomass_df.columns.tolist())
    print("var_df: ", var_df.columns.tolist(), "var_df shape: ", var_df.shape)
    print("var_col: ", var_col)

    # correct df site name
    site_list = []
    site_name = var_df["site_clean"].tolist()
    for x in site_name:
        #print(x)
        n = x.replace("_1ha", "")
        #print(n)
        site_list.append(n)

    var_df["site_clean"] = site_list

    basal_filt = biomass_df[biomass_df["site_clean"] == loop_site]
    # merge previous output with basal

    #print("var_df: ", var_df.shape, "basal_filt: ", basal_filt.shape)

    # todo up to here

    basal_nfy = pd.merge_asof(basal_filt, var_df, left_on="basal_dt", right_on=var_col, by="site_clean",
                              direction="nearest")
    column_dict = {'uid_x': 'uid', 'uid_y': f'uid_{var_}'}
    basal_nfy.rename(columns=column_dict, inplace=True)
    #print(basal_nfy.columns)

    return basal_nfy


def basal_dbi_merge_fire_year(biomass_df, var_df, var_col, var_, i):
    site_list = []
    site_name = var_df["site_clean"].tolist()
    for i in site_name:
        #print(i)
        n = i.replace("_1ha", "")
        #print(n)
        site_list.append(n)

    var_df["site_clean"] = site_list

    basal_filt = biomass_df[biomass_df["site_clean"] == i]

    # merge previous output with basal
    basal_nfy = pd.merge_asof(basal_filt, var_df, left_on="basal_dt", right_on=var_col, by="site_clean",
                              direction="nearest")
    column_dict = {'uid_x': 'uid', 'uid_y': f'uid_{var_}'}
    basal_nfy.rename(columns=column_dict, inplace=True)
    #print(basal_nfy.columns)

    return basal_nfy


def concat_export_csv_fn(list_, dir_, file_name):
    if len(list_) > 0:

        df_final = pd.concat(list_, axis=0)
        #print("df_final: ", df_final)
        output_path = os.path.join(dir_, file_name)
        df_final.to_csv(os.path.join(output_path), index=False)
        #print("File output to: ", output_path)

    else:
        df_final = None

    return df_final

def fire_major_fn(df, month_list, month_d_list):

    df.to_csv(r"U:\biomass\collated_zonal_stats\issue\fire_major.csv", index=False)

    print("fire major: ", df['fire_major'])
    # Convert numeric month to datetime

    month_ = []
    for i in df['fire_major']:
        print(i)
        for n, m in zip(month_d_list, month_list):
            if float(i) == float(n):
                month_.append(m)

    df['fire_major_cls'] = month_ #pd.to_datetime(df['fire_major'], format='%m').dt.month_name()

    return df


def main_routine(biomass_df, mosaic_dir, output_dir, dp0_dbg_si_single_annual_density_near_met_si, \
        dp0_dbg_si_mask_single_annual_density_near_met_si, \
        dp0_dbg_si_single_dry_density_near_met_si, \
        dp0_dbg_si_mask_single_dry_density_near_met_si, \
        dp1_dbi_si_annual_density_near_met_si, \
        dp1_dbi_si_annual_mask_density_near_met_si, \
        dp1_dbi_si_dry_density_near_met_si, \
        dp1_dbi_si_dry_mask_density_near_met_si):
    """

    :param biomass_df: pandas dataframe object containing all biomass site information.
    :param mosaic_dir: string object containing the path to the NT mosaic zonal stats.
    :param output_dir: string object containing the path to teh export directory.
    :return:
    """
    mosaic_export = os.path.join(output_dir, "mosaic_concat")

    if not os.path.isdir(mosaic_export):
        os.mkdir(mosaic_export)

    # Call the convert to datetime function
    biomass_df = convert_to_datetime(biomass_df, "date", "basal_dt")
    biomass_df.sort_values(by='basal_dt', inplace=True)

    # Call the convert to datetime function
    biomass_df = convert_to_datetime(biomass_df, "date", "basal_dt")
    biomass_df.sort_values(by='basal_dt', inplace=True)

    # no_fire_scar_dir = os.path.join(mosaic_export, "no_fire_scar_zonal")
    fire_scar_dir = os.path.join(mosaic_export, "fire_scar_zonal_stats")
    # fire_mask_dir = os.path.join(mosaic_export, "fire_mask")
    # fms_dir = os.path.join(fire_mask_dir, "site_clean")
    # no_fire_scar_basal_dir = os.path.join(mosaic_export, "fire_mask_applied_basal")
    # w_no_fire_scar_basal_dir = os.path.join(mosaic_export, "fire_mask_NOT_applied_basal")
    # revised_fire_scar_dir = os.path.join(mosaic_export, "initial_asof_merge_fmna")
    # szs_dir = os.path.join(mosaic_export, "seasonal_zonal_stats")
    # ftzs_dir = os.path.join(mosaic_export, "file_type_zonal_stats")

    mk_dir_fn(mosaic_export)
    # mk_dir_fn(no_fire_scar_dir)
    mk_dir_fn(fire_scar_dir)
    # mk_dir_fn(fire_mask_dir)
    # mk_dir_fn(fms_dir)
    # mk_dir_fn(no_fire_scar_basal_dir)
    # mk_dir_fn(w_no_fire_scar_basal_dir)
    # mk_dir_fn(revised_fire_scar_dir)
    # mk_dir_fn(szs_dir)
    # mk_dir_fn(ftzs_dir)

    print("mosaic_dir: ", mosaic_dir)
    sub_list = next(os.walk(mosaic_dir))[1]

    # define month list as per seasonal zonal stats
    month_list = ["none", "jan", "feb", "mar", "april", "may", "june", "july", "aug", "sep", "oct", "nov", "dec"]
    month_d_list = np.arange(0.0, 13.0).tolist()

    sub_dir_list = []

    cleaned_df_list = []
    cleaned_str_list = []

    # create lists for merged output data
    dka_0112_list = []

    list_merge = ["nearest"]  # , "forward", "backward"]

    fire_list = []
    for sub_dir in sub_list:

        file_list = []
        if "zonal_stats" in sub_dir:
            sub_dir_list.append(sub_dir)
            for file_ in glob(os.path.join(mosaic_dir, sub_dir, "*.csv")):

                # create df from csv
                df = pd.read_csv(file_)
                # append df to file list
                file_list.append(df)

        if len(file_list) > 1:
            df1 = pd.concat(file_list)
            if "date" in df1.columns and "im_date" not in df1.columns:
                df1.rename(columns={"date": "im_date"}, inplace=True)

            # correct df1 site name
            site_list = []
            site_name = df1["site"].tolist()
            for i in site_name:
                n = i.replace("_1ha", "")
                site_list.append(n)

            df1["site_clean"] = site_list

            if sub_dir == "dka_zonal_stats":

                dka = df1.copy(deep=True)
                if "date" in dka.columns and "im_date" not in dka.columns:
                    dka.rename(columns={"date": "im_date"}, inplace=True)
                dka = im_date_annual(dka)

                var_ = "fire"
                dka_dict = {"count": "{0}_count".format(var_),
                            "min": "{0}_min".format(var_),
                            "max": "{0}_max".format(var_),
                            "mean": "{0}_mean".format(var_),
                            "sum": "{0}_sum".format(var_),
                            "std": "{0}_std".format(var_),
                            "median": "{0}_med".format(var_),
                            "majority": "{0}_major".format(var_),
                            "minority": "{0}_minor".format(var_),
                            "one": "{0}_one".format(var_),
                            "two": "{0}_two".format(var_),
                            "three": "{0}_three".format(var_),
                            "four": "{0}_four".format(var_),
                            "five": "{0}_five".format(var_),
                            "six": "{0}_six".format(var_),
                            "seven": "{0}_seven".format(var_),
                            "eight": "{0}_eight".format(var_),
                            "nine": "{0}_nine".format(var_),
                            "ten": "{0}_ten".format(var_)}

                dka.rename(columns=dka_dict, inplace=True)

                dka_s_ = convert_to_datetime(dka, "s_date", "image_s_dt")
                dka_s = convert_to_datetime(dka_s_, "e_date", "image_e_dt")
                dka_s.sort_values(by="s_date", inplace=True)
                dka_s.dropna(subset=[f'{var_}_min'], inplace=True)

                fire_list.append(dka_s)

            elif sub_dir == "dkn_zonal_stats":
                dkn = df1.copy(deep=True)
                if "date" in dkn.columns and "im_date" not in dkn.columns:
                    #                 print(dkn.columns)

                    dkn.rename(columns={"date": "im_date"}, inplace=True)
                dkn = im_date_annual(dkn)

                var_ = "fire"
                dkn_dict = {"count": "{0}_count".format(var_),
                            "min": "{0}_min".format(var_),
                            "max": "{0}_max".format(var_),
                            "mean": "{0}_mean".format(var_),
                            "sum": "{0}_sum".format(var_),
                            "std": "{0}_std".format(var_),
                            "median": "{0}_med".format(var_),
                            "majority": "{0}_major".format(var_),
                            "minority": "{0}_minor".format(var_),
                            "one": "{0}_one".format(var_),
                            "two": "{0}_two".format(var_),
                            "three": "{0}_three".format(var_),
                            "four": "{0}_four".format(var_),
                            "five": "{0}_five".format(var_),
                            "six": "{0}_six".format(var_),
                            "seven": "{0}_seven".format(var_),
                            "eight": "{0}_eight".format(var_),
                            "nine": "{0}_nine".format(var_),
                            "ten": "{0}_ten".format(var_),
                            # "im_date": "{0}_im_date".format("dkn")
                            }

                dkn.rename(columns=dkn_dict, inplace=True)

                dkn_s_ = convert_to_datetime(dkn, "s_date", "image_s_dt")
                dkn_s = convert_to_datetime(dkn_s_, "e_date", "image_e_dt")
                dkn_s.sort_values(by="s_date", inplace=True)
                dkn_s.dropna(subset=[f'{var_}_min'], inplace=True)

                fire_list.append(dkn_s)

            elif sub_dir == "dkh_zonal_stats":
                dkh = df1.copy(deep=True)
                if "date" in dkh.columns and "im_date" not in dkh.columns:
                    dkh.rename(columns={"date": "im_date"}, inplace=True)
                dkh = im_date_annual(dkh)

                var_ = "fire"
                dkh_dict = {"count": "{0}_count".format(var_),
                            "min": "{0}_min".format(var_),
                            "max": "{0}_max".format(var_),
                            "mean": "{0}_mean".format(var_),
                            "sum": "{0}_sum".format(var_),
                            "std": "{0}_std".format(var_),
                            "median": "{0}_med".format(var_),
                            "majority": "{0}_major".format(var_),
                            "minority": "{0}_minor".format(var_),
                            "one": "{0}_one".format(var_),
                            "two": "{0}_two".format(var_),
                            "three": "{0}_three".format(var_),
                            "four": "{0}_four".format(var_),
                            "five": "{0}_five".format(var_),
                            "six": "{0}_six".format(var_),
                            "seven": "{0}_seven".format(var_),
                            "eight": "{0}_eight".format(var_),
                            "nine": "{0}_nine".format(var_),
                            "ten": "{0}_ten".format(var_),
                            # "im_date": "{0}_im_date".format("dkh"),
                            }

                dkh.rename(columns=dkh_dict, inplace=True)

                dkh_s_ = convert_to_datetime(dkh, "s_date", "image_s_dt")
                dkh_s = convert_to_datetime(dkh_s_, "e_date", "image_e_dt")
                dkh_s.sort_values(by="s_date", inplace=True)
                dkh_s.dropna(subset=[f'{var_}_min'], inplace=True)

                fire_list.append(dkh_s)


    df = pd.concat(fire_list)
    df_all = df[['site_clean', 'im_date', 'band', 'fire_count', 'fire_min',
                 'fire_max', 'fire_mean', 'fire_sum', 'fire_std', 'fire_med',
                 'fire_major', 'fire_minor', 'jan', 'feb', 'mar', 'april', 'may', 'june',
                 'july', 'aug', 'sep', 'oct', 'nov', 'dec',  's_date',
                 'e_date', 'image_s_dt', 'image_e_dt',]]

    df_all.sort_values(by="s_date", inplace=True)
    df_all.dropna(subset=['fire_min'], inplace=True)

    # call fire frequency etc functions
    df_all = fire_percent_fn(df_all, "fire")
    df_all = fire_yn_fn(df_all, "fire")
    df_all = fire_intensity_fn(df_all, "fire")
    df_all = prop_fire_freq_fn(df_all)
    df_all = fire_previous_year(df_all)
    df_all = fire_gap_fn(df_all)
    df_all.to_csv(r"U:\biomass\collated_zonal_stats\issue\df_all_before_possion.csv", index=False)
    df_all = poisson_fn(df_all, 1, 2)
    df_all = poisson_fn(df_all, 1, 5)
    df_all = poisson_fn(df_all, 1, 10)
    df_all = fire_major_fn(df_all, month_list, month_d_list)

    print(list(df_all.columns))
    df_all.to_csv(r"U:\biomass\collated_zonal_stats\issue\df_all.csv", index=False)

    df_all.sort_values(by="image_s_dt", inplace=True)
    df_all.dropna(subset=['fire_min'], inplace=True)

    df_all.drop_duplicates(inplace=True)

    export_csv_file_fn(df_all, fire_scar_dir, "all_fire_zonal_stats.csv")
    cleaned_df_list.append(df_all)

    # n_df = pd.merge_asof(biomass_df, df_all, left_on="basal_dt", right_on="image_s_dt", by="site_clean",
    # direction="forward") out = os.path.join(r"U:\biomass\collated_zonal_stats\fire", "asof_df_fire_forward.csv") n_df.to_csv(
    # out, index=False)

    fire_df = pd.merge_asof(
        biomass_df, df_all, left_on="basal_dt", right_on="image_s_dt", by="site_clean", direction="backward")
    out = os.path.join(r"U:\biomass\collated_zonal_stats\fire", "asof_df_fire_backward.csv")
    fire_df.to_csv(out, index=False)

    print("fire_df: ", list(fire_df.columns))

    export_csv_file_fn(fire_df, fire_scar_dir, "asof_back_fire_zonal_stats.csv")

    out = os.path.join(r"U:\biomass\collated_zonal_stats\fire", "fire_df_clean.csv")
    fire_df.to_csv(out, index=False)

    fire_df_clean = fire_df[['uid',  'site_clean', 'date', 'lon_gda94', 'lat_gda94', 'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
     'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha', 'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
     'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha', 'c_r_kg1ha', 'c_agb_kg1ha', 'geometry', 'basal_dt',
      'im_date', 'band', 'fire_count', 'fire_min', 'fire_max', 'fire_mean', 'fire_sum', 'fire_std',
     'fire_med', 'fire_major', 'fire_minor', 'jan', 'feb', 'mar', 'april', 'may', 'june', 'july', 'aug', 'sep', 'oct',
     'nov', 'dec', 's_date', 'e_date', 'image_s_dt', 'image_e_dt', 'area_ha', 'jan_per', 'feb_per', 'mar_per',
     'april_per', 'may_per', 'june_per', 'july_per', 'aug_per', 'sep_per', 'oct_per', 'nov_per', 'dec_per', 'burnt_enco',
     'burnt_cat', 'fire_sn_ord', 'fire_sn_cat', 'fire_f', 'fire_tot', 'since_fire', 'fire_gap', 'fire_pois1_2', 'fire_pois1_5', 'fire_pois1_10']]

    # n_df = pd.merge_asof(biomass_df, df_all, left_on="basal_dt", right_on="image_s_dt", by="site_clean",
    # direction="nearest")
    #
    out = os.path.join(r"U:\biomass\collated_zonal_stats\fire", "fire_df_clean.csv")
    fire_df_clean.to_csv(out, index=False)
    
    # ==================================================================================================================

    # ---------------------------------------------------- fire_df_clean --------------------------------------------------------

    # --------------------------------------------- annual merge -------------------------------------------------------
    dp0_dbg_si_single_annual_density_near_met_si_fire = pd.merge(right=dp0_dbg_si_single_annual_density_near_met_si, left=fire_df_clean, how="outer",
                                                         on=['uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94',
                                                             'geometry',
                                                             'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                                                             'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha',
                                                             'bio_r_kg1ha',
                                                             'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
                                                             'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha',
                                                             'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'])

    dp0_dbg_si_single_annual_density_near_met_si_fire.to_csv(
        r"U:\biomass\collated_zonal_stats\single\dp0_dbg_si_single_annual_density_near_met_si_fire.csv",
        index=False)

    dp0_dbg_si_mask_single_annual_density_near_met_si_fire = pd.merge(right=dp0_dbg_si_mask_single_annual_density_near_met_si, left=fire_df_clean,
                                                              how="outer",
                                                              on=['uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94',
                                                                  'geometry',
                                                                  'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                                                                  'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha',
                                                                  'bio_r_kg1ha',
                                                                  'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
                                                                  'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha',
                                                                  'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'])

    dp0_dbg_si_mask_single_annual_density_near_met_si_fire.to_csv(
        r"U:\biomass\collated_zonal_stats\single_mask\dp0_dbg_si_mask_single_annual_density_near_met_si_fire.csv",
        index=False)

    # ------------------------------------------------------ dry merge -------------------------------------------------

    dp0_dbg_si_single_dry_density_near_met_si_fire = pd.merge(right=dp0_dbg_si_single_dry_density_near_met_si, left=fire_df_clean, how="outer",
                                                      on=['uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94',
                                                          'geometry',
                                                          'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                                                          'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha',
                                                          'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
                                                          'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha',
                                                          'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'])

    dp0_dbg_si_single_dry_density_near_met_si_fire.to_csv(
        r"U:\biomass\collated_zonal_stats\single\dp0_dbg_si_single_dry_density_near_met_si_fire.csv",
        index=False)

    dp0_dbg_si_mask_single_dry_density_near_met_si_fire = pd.merge(right=dp0_dbg_si_mask_single_dry_density_near_met_si, left=fire_df_clean,
                                                           how="outer",
                                                           on=['uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94',
                                                               'geometry',
                                                               'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                                                               'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha',
                                                               'bio_r_kg1ha',
                                                               'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
                                                               'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha',
                                                               'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'])

    dp0_dbg_si_mask_single_dry_density_near_met_si_fire.to_csv(
        r"U:\biomass\collated_zonal_stats\single_mask\dp0_dbg_si_mask_single_dry_density_near_met_si_fire.csv",
        index=False)

    # ------------------------------------------------------ annual merge dp1 bbi --------------------------------------

    dp1_dbi_si_annual_density_near_met_si_fire = pd.merge(right=dp1_dbi_si_annual_density_near_met_si, left=fire_df_clean, how="outer",
                                                  on=['uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94', 'geometry',
                                                      'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                                                      'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha',
                                                      'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
                                                      'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha',
                                                      'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'])

    dp1_dbi_si_annual_density_near_met_si_fire.to_csv(
        r"U:\biomass\collated_zonal_stats\annual\dp1_dbi_si_annual_density_near_met_si_fire.csv",
        index=False)

    dp1_dbi_si_annual_mask_density_near_met_si_fire = pd.merge(right=dp1_dbi_si_annual_mask_density_near_met_si, left=fire_df_clean, how="outer",
                                                       on=['uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94',
                                                           'geometry',
                                                           'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                                                           'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha',
                                                           'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
                                                           'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha',
                                                           'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'])

    dp1_dbi_si_annual_mask_density_near_met_si_fire.to_csv(
        r"U:\biomass\collated_zonal_stats\annual_mask\dp1_dbi_si_annual_mask_density_near_met_si_fire.csv",
        index=False)

    # ---------------------------------------------- dry merge dp1 bbi -------------------------------------------------

    dp1_dbi_si_dry_density_near_met_si_fire = pd.merge(right=dp1_dbi_si_dry_density_near_met_si, left=fire_df_clean, how="outer",
                                               on=['uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94', 'geometry',
                                                   'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                                                   'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha',
                                                   'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
                                                   'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha',
                                                   'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'])

    dp1_dbi_si_dry_density_near_met_si.to_csv(
        r"U:\biomass\collated_zonal_stats\dry\dp1_dbi_si_dry_density_near_df_near_met_si.csv",
        index=False)

    dp1_dbi_si_dry_mask_density_near_met_si_fire = pd.merge(right=dp1_dbi_si_dry_mask_density_near_met_si, left=fire_df_clean, how="outer",
                                                    on=['uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94',
                                                        'geometry',
                                                        'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
                                                        'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha',
                                                        'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
                                                        'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha',
                                                        'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt'])

    dp1_dbi_si_dry_mask_density_near_met_si_fire.to_csv(
        r"U:\biomass\collated_zonal_stats\dry_mask\dp1_dbi_si_dry_mask_density_near_met_si_fire.csv",
        index=False)

    return fire_df_clean, dp0_dbg_si_single_annual_density_near_met_si_fire, \
        # dp0_dbg_si_mask_single_annual_density_near_met_si_fire, \
        # dp0_dbg_si_single_dry_density_near_met_si_fire, \
        # dp0_dbg_si_mask_single_dry_density_near_met_si_fire, \
        # dp1_dbi_si_annual_density_near_met_si_fire, \
        # dp1_dbi_si_annual_mask_density_near_met_si_fire, \
        # dp1_dbi_si_dry_density_near_met_si_fire, \
        # dp1_dbi_si_dry_mask_density_near_met_si_fire


if __name__ == '__main__':
    main_routine()
