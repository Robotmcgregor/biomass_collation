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
#import seaborn as sns
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


def fire_percent_fn(df):
    """ Calculate the percent cover burnt by fire. """
    df.fillna(0, inplace=True)
    df["area_ha"] = (df.dka_count * (30 * 30) * 0.0001)
    df["jan_per"] = (df.jan / df.dka_count * 100)  # (30 *30)/ 1000)
    df["feb_per"] = (df.feb / df.dka_count * 100)
    df["mar_per"] = (df.mar / df.dka_count * 100)
    df["april_per"] = (df.april / df.dka_count * 100)
    df["may_per"] = (df.may / df.dka_count * 100)
    df["june_per"] = (df.june / df.dka_count * 100)
    df["july_per"] = (df.july / df.dka_count * 100)
    df["aug_per"] = (df.aug / df.dka_count * 100)
    df["sep_per"] = (df.sep / df.dka_count * 100)
    df["oct_per"] = (df.oct / df.dka_count * 100)
    df["nov_per"] = (df.nov / df.dka_count * 100)
    df["dec_per"] = (df.dec / df.dka_count * 100)

    return df


def fire_yn_fn(df):
    """ Score if fire occured during the year 0 = No, 1 = yes. """
    fire_1_0 = []

    for index, row in df.iterrows():

        if row.dka_major == 0:
            fire_1_0.append(0)
        else:
            fire_1_0.append(1)

    df['burnt'] = fire_1_0

    return df


def fire_intensity_fn(df):
    """ Score fire intensity by majority burnt 0 = no fire, 1 = Jan - June, 2 July - December """

    list_ = []

    for index, row in df.iterrows():

        if row.dka_major == 0:
            list_.append(0)


        elif row.dka_major > 1 and row.dka_major < 7:
            list_.append(1)
        else:
            list_.append(2)

    df['intens'] = list_

    return df


def ratio_fire_year_fn(x, y, p, n):
    # data number of fires per data lenght of time = x/y
    # revised time frame i.e. number of fires per time restriction = p/n

    final = (x * n) / (p * y)

    return final


def prop_fire_freq_fn(df):
    list_ = []
    for i in df.site.unique():
        df1 = df[df["site"] == i]

        burnt_sum = df1.burnt.sum()
        # calculate average time between fires over years of data capture
        if burnt_sum > 0:

            freq = ratio_fire_year_fn(1, 10, burnt_sum, (len(df1.index)))

            frequency = round(freq, 5)  # round((len(df1.index)) / burnt_sum, 5)
        else:
            frequency = round(0 / 10, 5)  # round(len(df1.index), 5)

        df1["fire_f"] = frequency
        df1["fire_tot"] = burnt_sum = df1.burnt.sum()

        list_.append(df1)

    df2 = pd.concat(list_, axis=0)
    return df2


def fire_previous_year(df):
    list_ = []
    df.dropna(inplace=True)
    for i in df.site.unique():
        years_since_list = []
        print(i)
        df1 = df[df["site"] == i]

        df1.sort_values(by="s_date", inplace=True, ascending=True)
        #         print(df1)

        no_fire_list = []

        loop_x = 1
        for index, row in df1.iterrows():
            x = row["burnt"]
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

    for i in df.site.unique():
        years_since_list = []
        print(i)
        df1 = df[df["site"] == i]

        x = df1.since_fire.mean()
        print("x: ", x)

        df1["fire_gap"] = x

        list_.append(df1)

    df2 = pd.concat(list_, axis=0)

    return df2


def ratio_fire_year_fn(x, y, p, n):
    # data number of fires per data lenght of time = x/y
    # revised time frame i.e. number of fires per time restriction = p/n

    final = (x * n) / (p * y)

    return final


def poisson_fn(df, p, n):
    list_ = []
    df.dropna(inplace=True)

    for i in df.site.unique():
        years_since_list = []
        print(i)
        df1 = df[df["site"] == i]

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
    """ Calculate the percent cover burnt by fire. """
    df.fillna(0, inplace=True)
    site_list = []
    burnt_start_list = []
    burnt_end_list = []
    burnt_year_list = []

    for index, row in df.iterrows():
        month__ = []
        site_list.append(row.site)
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

    return site_list, burnt_start_list, burnt_end_list, burnt_year_list


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

    for i in df.site.unique():

        df_site = df[df["site"] == i]

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
                # st != 0 and end != 0

                # convert to string
                st_ = str(st)
                end_ = str(end)

                # seperate month
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

    fire_ym_mask["site"] = site_list
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
    for i in df.site.unique():

        df_site = df[df["site"] == i]
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
                'site': site_list,
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
    for i in df.site.unique():

        df_site = df[df["site"] == i]
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
                'site': site_list,
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
    for i in df.site.unique():

        df_site = df[df["site"] == i]
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
                'site': site_list,
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

    #     ['uid', 'site', 'image', 's_day', 's_month', 's_year', 's_date', 'e_day', 'e_month', 'e_year',
    #      'e_date', 'b1_dbi_count', 'b1_dbi_min', 'b1_dbi_max', 'b1_dbi_mean', 'b1_dbi_med', 'b1_dbi_std',
    #      'b1_dbi_p25', 'b1_dbi_p50', 'b1_dbi_p75', 'b1_dbi_p95', 'b1_dbi_p99', 'b1_dbi_range', 'b2_dbi_count',
    #      'b2_dbi_min', 'b2_dbi_max', 'b2_dbi_mean', 'b2_dbi_med', 'b2_dbi_std', 'b2_dbi_p25', 'b2_dbi_p50', 'b2_dbi_p75',
    #      'b2_dbi_p95', 'b2_dbi_p99', 'b2_dbi_range', 'b3_dbi_count', 'b3_dbi_min', 'b3_dbi_max', 'b3_dbi_mean', 'b3_dbi_med',
    #      'b3_dbi_std', 'b3_dbi_p25', 'b3_dbi_p50', 'b3_dbi_p75', 'b3_dbi_p95', 'b3_dbi_p99', 'b3_dbi_range', 'b4_dbi_count',
    #      'b4_dbi_min', 'b4_dbi_max', 'b4_dbi_mean', 'b4_dbi_med', 'b4_dbi_std', 'b4_dbi_p25', 'b4_dbi_p50', 'b4_dbi_p75',
    #      'b4_dbi_p95', 'b4_dbi_p99', 'b4_dbi_range', 'b5_dbi_count', 'b5_dbi_min', 'b5_dbi_max', 'b5_dbi_mean', 'b5_dbi_med',
    #      'b5_dbi_std', 'b5_dbi_p25', 'b5_dbi_p50', 'b5_dbi_p75', 'b5_dbi_p95', 'b5_dbi_p99', 'b5_dbi_range', 'b6_dbi_count',
    #      'b6_dbi_min', 'b6_dbi_max', 'b6_dbi_mean', 'b6_dbi_med', 'b6_dbi_std', 'b6_dbi_p25', 'b6_dbi_p50', 'b6_dbi_p75',
    #      'b6_dbi_p95', 'b6_dbi_p99',
    #      'b6_dbi_range', 'image_s_dt', 'image_e_dt', 'dt_year', 'dt_ym']

    print(df.columns)
    print("init seasonal_fpca2_" * 20)
    for i in df.site.unique():

        df_site = df[df["site"] == i]
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
                'site': site_list,
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
    for i in df.site.unique():

        df_site = df[df["site"] == i]
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
                'site': site_list,
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
    for i in df.site.unique():

        df_site = df[df["site"] == i]
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
                'site': site_list,
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


def clean_y_fn(var_df, fire_occ):
    # filter variable df by same site
    var_filt = var_df[var_df["site"] == i]
    # test = convert_to_dt_year(test, "s_year", "bn_y")
    var_filt['dt_year'] = var_filt['s_year'].astype(int)
    var_filt.sort_values(by="dt_year", inplace=True)

    # merge fire scar and variable data on the nearest unburnt date
    y_merge = pd.merge(fire_occ, var_filt, on=["site", "dt_year"], how="inner")

    return y_merge, var_filt


def append_y_merge(merge_df, merge_list, column_dict):
    merge_fire_mask = merge_df[merge_list]
    merge_fire_mask.rename(columns=column_dict, inplace=True)

    return merge_fire_mask


def clean_ym_fn(df):
    print(df)

    df.dropna(subset=["image_s_dt"], inplace=True)
    df.sort_values(by="image_s_dt", inplace=True)

    ym_list, y_list = year_month_fn(df, "image_s_dt")

    df.loc[:, "dt_ym"] = ym_list
    df.loc[:, "dt_ym"] = var_filt["dt_ym"].astype(int)
    df.loc[:, "dt_year"] = y_list
    df.loc[:, "dt_year"] = var_filt["dt_year"].astype(int)

    df_ym = df.copy(deep=True)
    print("var_filt_ym: ", df_ym.shape)

    return (df_ym)


def export_var_fire_scar_zonal_fn(df, dir_, var_, site):
    exp_dir = os.path.join(dir_, var_)
    mk_dir_fn(exp_dir)
    output_path = os.path.join(exp_dir, f"{site}_agb_nt_mosaic_{var_}_fire_scar_zonal.csv")
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


def basal_merge_fire_year(basal_df, var_df, var_col, var_, i):
    basal_filt = basal_df[basal_df["site"] == i]
    # merge previous output with basal
    basal_nfy = pd.merge_asof(basal_filt, var_df, left_on="basal_dt", right_on=var_col, by="site", direction="nearest")
    column_dict = {'uid_x': 'uid', 'uid_y': f'uid_{var_}'}
    basal_nfy.rename(columns=column_dict, inplace=True)
    print(basal_nfy.columns)

    return basal_nfy


def basal_dbi_merge_fire_year(basal_df, var_df, var_col, var_, i):
    basal_filt = basal_df[basal_df["site"] == i]
    # merge previous output with basal
    basal_nfy = pd.merge_asof(basal_filt, var_df, left_on="basal_dt", right_on=var_col, by="site", direction="nearest")
    column_dict = {'uid_x': 'uid', 'uid_y': f'uid_{var_}'}
    basal_nfy.rename(columns=column_dict, inplace=True)
    print(basal_nfy.columns)

    return basal_nfy


def concat_export_csv_fn(list_, dir_, file_name):
    if len(list_) > 0:

        df_final = pd.concat(list_, axis=0)
        print("df_final: ", df_final)
        output_path = os.path.join(dir_, file_name)
        df_final.to_csv(os.path.join(output_path), index=False)
        print("File output to: ", output_path)

    else:
        df_final = None

    return df_final


def main_routine(): #mosaic_export, cleaned_df_list, cleaned_str_list, dka_s, month_list):
    # dka_s, month_list from step3

    cleaned_df_list = []
    cleaned_str_list = []

    # define month list as per seasonal zonal stats
    month_list = ["jan", "feb", "mar", "april", "may", "june", "july", "aug", "sep", "oct", "nov", "dec"]
    month_d_list = np.arange(1, 13).tolist()


    biomass_csv = (r"U:\biomass\agb\20230729\concat_slats_tern_blue_biolib_biomass.csv")

    biomass_df = pd.read_csv(biomass_csv)

    # Call the convert to datetime function
    biomass_df = convert_to_datetime(biomass_df, "date", "basal_dt")
    biomass_df.sort_values(by='basal_dt', inplace=True)

    dp1 = pd.read_csv(
        r"U:\scratch\rob\pipelines\outputs\rmcgr_collation_tile_data_20230826_1230\tile_concat\dp1_zonal_concat.csv")
    mosaic_export = r"U:\scratch\rob\pipelines\outputs\rmcgr_collation_tile_data_20230826_1230\mosaic_concat"

    dka_s = pd.read_csv(
        r"U:\scratch\rob\pipelines\outputs\rmcgr_collation_tile_data_20230826_1230\mosaic_concat\file_type_zonal_stats\dka_all_seasons_zonal_stats.csv")
    print('+'*50)
    print("mosaic_export: ", mosaic_export)

    # no_fire_scar_dir = os.path.join(mosaic_export, "no_fire_scar_zonal")
    # fire_scar_dir = os.path.join(mosaic_export, "fire_scar_zonal_stats")
    fire_mask_dir = os.path.join(mosaic_export, "fire_mask")
    # fms_dir = os.path.join(fire_mask_dir, "site")
    # no_fire_scar_basal_dir = os.path.join(mosaic_export, "fire_mask_applied_basal")
    w_no_fire_scar_basal_dir = os.path.join(mosaic_export, "fire_mask_NOT_applied_basal")
    revised_fire_scar_dir = os.path.join(mosaic_export, "initial_asof_merge_fmna")
    # szs_dir = os.path.join(mosaic_export, "seasonal_zonal_stats")
    ftzs_dir = os.path.join(mosaic_export, "file_type_zonal_stats")

    # ------------------------------------------------------------------------------------------------------------------

    print("dp1 shape: ", dp1.shape)
    print("-"*50)

    dp1_0112_list = []
    dp1_0509_list = []

    dp1_annual = dp1[dp1["s_month"] == 1]
    dp1_dry = dp1[dp1["s_month"] == 5]

    print("annual: ", dp1_annual.shape)
    print("dry: ", dp1_dry.shape)

    for df, var_ in zip([dp1_annual, dp1_dry], ["dp1_0112", "dp1_0509"]):

        print(df.columns)
        dp1 = df.copy(deep=True)

        dp1_s = convert_to_datetime(dp1, "s_date", "image_s_dt")
        dp1_s.sort_values(by="s_date", inplace=True)
        dp1_s.dropna(subset=['b1_dp1_min'], inplace=True)

        biomass_df.to_csv(r'U:\scratch\rob\pipelines\outputs\biomass_df', index=False)
        print("biomass")
        for b in biomass_df.basal_dt:
            print(b, type(b))

        # print("dp1")
        # for b in dp1_s.image_s_dt:
        #     print(b, type(b))


        export_csv_file_fn(dp1_s, ftzs_dir, f"{var_}_all_seasons_zonal_stats.csv")

        print(type(biomass_df.basal_dt))
        # merge data with basal datset based on the nearest date to the field data colection
        dp1_s_single = pd.merge_asof(biomass_df, dp1_s, left_on="basal_dt", right_on="image_s_dt", by="site",
                                     direction="forward")

        dp1_s_single.rename(columns={"uid_x": "uid", "uid_y": f"uid_{var_}"}, inplace=True)
        export_csv_file_fn(dp1_s_single, revised_fire_scar_dir, f"agb_nt_mosaic_{var_}_fmna.csv")

        if var_ == "dp1_0112":
            dp1_0112_list.append(dp1_s_single)

            # append data to list
            cleaned_df_list.append(dp1_s)
            cleaned_str_list.append("dp1_0112")

        elif var_ == "dp1_0509":
            dp1_0509_list.append(dp1_s_single)

            # append data to list
            cleaned_df_list.append(dp1_s)
            cleaned_str_list.append("dp1_0509")


        else:
            import sys
            sys.exit()

    # print("end here....")
    # import sys
    # sys.exit()
    #
    # concatenate data here
    dp1_0112_basal_nfs = export_csv_fn(dp1_0112_list, w_no_fire_scar_basal_dir,
                                       "dp1_0112_basal_with_y_fire_mask_not_applied.csv")
    dp1_0509_basal_nfs = export_csv_fn(dp1_0509_list, w_no_fire_scar_basal_dir,
                                       "dp1_0509_basal_with_y_fire_mask_not_applied.csv")

    # Run Scar Year Month Function
    site_list, burnt_start_list, burnt_end_list, burnt_year_list = fire_scar_year_month_fn(
        dka_s, month_list, month_d_list)

    # dka_s["site_check"] = site_list
    dka_s.loc[:, "bnt_st_ym"] = burnt_start_list
    dka_s.loc[:, "bn_end_ym"] = burnt_end_list
    dka_s.loc[:, "bn_end_ym"] = burnt_year_list

    # print out readable fire scar info per site
    for s, d in zip(site_list, burnt_start_list):
        print(f"{s} was burnt on {d}")

    dka_s_nfire = dka_s[dka_s["burnt"] == 0]

    fire_mask_path = os.path.join(fire_mask_dir, "dkk_with_fire_scars_years_removed.csv")
    dka_s_nfire.to_csv(fire_mask_path, index=False)
    print(fire_mask_path)

    # Fire mask

    fire_mask = pd.DataFrame()
    fire_mask["site"] = site_list
    fire_mask["bnt_st_ym"] = burnt_start_list
    fire_mask["bn_end_ym"] = burnt_end_list
    fire_mask["bn_year"] = burnt_year_list

    dbi_s_merge_list = []

    fire_ym_mask = fire_month_mask(fire_mask)

    # ----------------------------------------------------------------------------
    # ----------------------------------------------------------------------------

    #def list_sort():
    stc_s_merge_list = ['uid', 'site', 'dt_year', 'stc_image', 'image_s_dt', 'band', 'stc_count', 'stc_min', 'stc_max',
                        'stc_mean',
                        'stc_sum', 'stc_std', 'stc_med', 'stc_major', 'stc_minor', 'stc_one', 'stc_two', 'stc_three',
                        'stc_four', 'stc_five', 'stc_six', 'stc_seven', 'stc_eight', 'stc_nine', 'stc_ten', 'stc_elev',
                        'stc_twelv', 'stc_thirt', 'stc_fourt', 'stc_fift', 'stc_sixt', 'stc_sevent']

    fpca2_y_merge_list = ['uid', 'site', 'dt_year', 'image', 'image_s_dt', 'b1_fpca2_count', 'b1_fpca2_min',
                          'b1_fpca2_max', 'b1_fpca2_mean', 'b1_fpca2_med', 'b1_fpca2_std', 'b1_fpca2_p25',
                          'b1_fpca2_p50', 'b1_fpca2_p75', 'b1_fpca2_p95', 'b1_fpca2_p99', 'b1_fpca2_range']

    fpca2_ym_merge_list = ['uid', 'site', 'dt_year', 'dt_ym', 'image', 'image_s_dt', 'fpca2_count',
                           'fpca2_min', 'fpca2_max', 'fpca2_mean', 'fpca2_med', 'fpca2_std', 'fpca2_p25',
                           'fpca2_p50', 'fpca2_p75', 'fpca2_p95', 'fpca2_p99', 'fpca2_range']

    h99a_merge_list = ['uid', 'site', 'dt_year', 'image', 'image_s_dt', 'b1_h99a2_count', 'b1_h99a2_min',
                       'b1_h99a2_max', 'b1_h99a2_mean', 'b1_h99a2_med', 'b1_h99a2_std', 'b1_h99a2_p25',
                       'b1_h99a2_p50', 'b1_h99a2_p75', 'b1_h99a2_p95', 'b1_h99a2_p99', 'b1_h99a2_range']

    dbi_y_merge_list = ['uid', 'site', 'dt_year', 'image', 'image_s_dt', 'b1_dbi_count', 'b1_dbi_min',
                        'b1_dbi_max', 'b1_dbi_mean', 'b1_dbi_med', 'b1_dbi_std', 'b1_dbi_p25', 'b1_dbi_p50',
                        'b1_dbi_p75',
                        'b1_dbi_p95', 'b1_dbi_p99', 'b1_dbi_range', 'b2_dbi_count', 'b2_dbi_min', 'b2_dbi_max',
                        'b2_dbi_mean',
                        'b2_dbi_med', 'b2_dbi_std', 'b2_dbi_p25', 'b2_dbi_p50', 'b2_dbi_p75', 'b2_dbi_p95',
                        'b2_dbi_p99',
                        'b2_dbi_range', 'b3_dbi_count', 'b3_dbi_min', 'b3_dbi_max', 'b3_dbi_mean', 'b3_dbi_med',
                        'b3_dbi_std',
                        'b3_dbi_p25', 'b3_dbi_p50', 'b3_dbi_p75', 'b3_dbi_p95', 'b3_dbi_p99', 'b3_dbi_range',
                        'b4_dbi_count',
                        'b4_dbi_min', 'b4_dbi_max', 'b4_dbi_mean', 'b4_dbi_med', 'b4_dbi_std', 'b4_dbi_p25',
                        'b4_dbi_p50',
                        'b4_dbi_p75', 'b4_dbi_p95', 'b4_dbi_p99', 'b4_dbi_range', 'b5_dbi_count', 'b5_dbi_min',
                        'b5_dbi_max',
                        'b5_dbi_mean', 'b5_dbi_med', 'b5_dbi_std', 'b5_dbi_p25', 'b5_dbi_p50', 'b5_dbi_p75',
                        'b5_dbi_p95',
                        'b5_dbi_p99', 'b5_dbi_range', 'b6_dbi_count', 'b6_dbi_min', 'b6_dbi_max', 'b6_dbi_mean',
                        'b6_dbi_med',
                        'b6_dbi_std', 'b6_dbi_p25', 'b6_dbi_p50', 'b6_dbi_p75', 'b6_dbi_p95', 'b6_dbi_p99',
                        'b6_dbi_range',
                        ]

    dbi_ym_merge_list = ['uid', 'site', 'dt_year', 'dt_ym', 'image', 'image_s_dt', 'b1_dbi_count', 'b1_dbi_min',
                         'b1_dbi_max', 'b1_dbi_mean', 'b1_dbi_med', 'b1_dbi_std', 'b1_dbi_p25', 'b1_dbi_p50',
                         'b1_dbi_p75',
                         'b1_dbi_p95', 'b1_dbi_p99', 'b1_dbi_range', 'b2_dbi_count', 'b2_dbi_min', 'b2_dbi_max',
                         'b2_dbi_mean',
                         'b2_dbi_med', 'b2_dbi_std', 'b2_dbi_p25', 'b2_dbi_p50', 'b2_dbi_p75', 'b2_dbi_p95',
                         'b2_dbi_p99', 'b2_dbi_range',
                         'b3_dbi_count', 'b3_dbi_min', 'b3_dbi_max', 'b3_dbi_mean', 'b3_dbi_med', 'b3_dbi_std',
                         'b3_dbi_p25', 'b3_dbi_p50',
                         'b3_dbi_p75', 'b3_dbi_p95', 'b3_dbi_p99', 'b3_dbi_range', 'b4_dbi_count', 'b4_dbi_min',
                         'b4_dbi_max', 'b4_dbi_mean',
                         'b4_dbi_med', 'b4_dbi_std', 'b4_dbi_p25', 'b4_dbi_p50', 'b4_dbi_p75', 'b4_dbi_p95',
                         'b4_dbi_p99', 'b4_dbi_range',
                         'b5_dbi_count', 'b5_dbi_min', 'b5_dbi_max', 'b5_dbi_mean', 'b5_dbi_med', 'b5_dbi_std',
                         'b5_dbi_p25', 'b5_dbi_p50',
                         'b5_dbi_p75', 'b5_dbi_p95', 'b5_dbi_p99', 'b5_dbi_range', 'b6_dbi_count', 'b6_dbi_min',
                         'b6_dbi_max', 'b6_dbi_mean',
                         'b6_dbi_med', 'b6_dbi_std', 'b6_dbi_p25', 'b6_dbi_p50', 'b6_dbi_p75', 'b6_dbi_p95',
                         'b6_dbi_p99', 'b6_dbi_range'
                         ]

    # dja_y_merge_list = ['uid', 'site', 'dt_year', 'image', 'image_s_dt', 'b1_dja_count', 'b1_dja_min', 'b1_dja_max',
    #                     'b1_dja_mean', 'b1_dja_med',
    #                     'b1_dja_std', 'b1_dja_p25', 'b1_dja_p50', 'b1_dja_p75', 'b1_dja_p95', 'b1_dja_p99',
    #                     'b1_dja_range', 'image_s_dt']

    dja_y_merge_list = ['uid', 'site', 'dt_year', 'image', 'b1_dja_count', 'b1_dja_min', 'b1_dja_max', 'b1_dja_mean',
                        'b1_dja_med', 'b1_dja_std', 'b1_dja_p25', 'b1_dja_p50', 'b1_dja_p75', 'b1_dja_p95',
                        'b1_dja_p99',
                        'b1_dja_range', 'image_s_dt', 'image_e_dt']

    # dja_ym_merge_list = ['uid', 'site', 'dt_ym', 'image', 'image_s_dt',
    #                      'dja_count', 'dja_min', 'dja_max', 'dja_mean', 'dja_med', 'dja_std', 'dja_p25', 'dja_p50',
    #                      'dja_p75',
    #                      'dja_p95', 'dja_p99', 'dja_range']

    dis_y_merge_list = ['uid', 'site', 'dt_year', 'dis_image', 'image_s_dt', 'dis_count', 'dis_min', 'dis_max',
                        'dis_mean', 'dis_sum', 'dis_std', 'dis_med', 'dis_major',
                        'dis_minor', 'dis_one', 'dis_two', 'dis_three', 'dis_four', 'dis_five', 'dis_six', 'dis_seven',
                        'dis_eight', 'dis_nine', 'dis_ten']

    dis_ym_merge_list = ['uid', 'site', 'dt_year', 'dt_ym', 'image', 'image_s_dt',
                         'dis_count', 'dis_min', 'dis_max', 'dis_mean', 'dis_med', 'dis_std', 'dis_major', 'dis_minor',
                         'dis_one', 'dis_two', 'dis_three', 'dis_four', 'dis_five', 'dis_six', 'dis_seven', 'dis_eight',
                         'dis_nine', 'dis_ten']

    dja_ym_merge_list = ['uid', 'site', 'dt_ym', 'image', 'image_s_dt', 'dja_count', 'dja_min', 'dja_max', 'dja_mean',
                         'dja_med',
                         'dja_std', 'dja_p25', 'dja_p50', 'dja_p75', 'dja_p95', 'dja_p99', 'dja_range']

    dim_y_merge_list = ['uid', 'site', 'dt_year', 'image', 'image_s_dt',
                        'b1_dim_count', 'b1_dim_min', 'b1_dim_max', 'b1_dim_mean',
                        'b1_dim_med', 'b1_dim_std', 'b1_dim_p25', 'b1_dim_p50', 'b1_dim_p75',
                        'b1_dim_p95', 'b1_dim_p99', 'b1_dim_range', 'b2_dim_count',
                        'b2_dim_min', 'b2_dim_max', 'b2_dim_mean', 'b2_dim_med', 'b2_dim_std',
                        'b2_dim_p25', 'b2_dim_p50', 'b2_dim_p75', 'b2_dim_p95', 'b2_dim_p99',
                        'b2_dim_range', 'b3_dim_count', 'b3_dim_min', 'b3_dim_max',
                        'b3_dim_mean', 'b3_dim_med', 'b3_dim_std', 'b3_dim_p25', 'b3_dim_p50',
                        'b3_dim_p75', 'b3_dim_p95', 'b3_dim_p99', 'b3_dim_range']

    dim_ym_merge_list = ['uid', 'site', 'dt_year', 'dt_ym', 'image', 'image_s_dt', 'b1_dim_count', 'b1_dim_min',
                         'b1_dim_max', 'b1_dim_mean', 'b1_dim_med', 'b1_dim_std', 'b1_dim_p25', 'b1_dim_p50',
                         'b1_dim_p75',
                         'b1_dim_p95', 'b1_dim_p99', 'b1_dim_range', 'b2_dim_count', 'b2_dim_min', 'b2_dim_max',
                         'b2_dim_mean',
                         'b2_dim_med', 'b2_dim_std', 'b2_dim_p25', 'b2_dim_p50', 'b2_dim_p75', 'b2_dim_p95',
                         'b2_dim_p99', 'b2_dim_range',
                         'b3_dim_count', 'b3_dim_min', 'b3_dim_max', 'b3_dim_mean', 'b3_dim_med', 'b3_dim_std',
                         'b3_dim_p25', 'b3_dim_p50',
                         'b3_dim_p75', 'b3_dim_p95', 'b3_dim_p99', 'b3_dim_range']

    dp1_y_merge_list = ['uid', 'site', 'dt_year', 'image', 'image_s_dt',
                        'b1_dp1_min', 'b1_dp1_max', 'b1_dp1_mean', 'b1_dp1_count', 'b1_dp1_std', 'b1_dp1_med',
                        'b1_dp1_p25', 'b1_dp1_p50', 'b1_dp1_p75', 'b1_dp1_p95', 'b1_dp1_p99', 'b1_dp1_range',
                        'b2_dp1_min', 'b2_dp1_max', 'b2_dp1_mean', 'b2_dp1_count', 'b2_dp1_std', 'b2_dp1_med',
                        'b2_dp1_p25', 'b2_dp1_p50', 'b2_dp1_p75', 'b2_dp1_p95', 'b2_dp1_p99', 'b2_dp1_range',
                        'b3_dp1_min', 'b3_dp1_max', 'b3_dp1_mean', 'b3_dp1_count', 'b3_dp1_med', 'b3_dp1_p25',
                        'b3_dp1_p50', 'b3_dp1_p75', 'b3_dp1_p95', 'b3_dp1_p99', 'b3_dp1_range', 'b3_dp1_std']

    dp1_ym_merge_list = ['uid', 'site', 'dt_year', 'dt_ym', 'image', 'image_s_dt', 'b1_dp1_count', 'b1_dp1_min',
                         'b1_dp1_max', 'b1_dp1_mean', 'b1_dp1_med', 'b1_dp1_std', 'b1_dp1_p25', 'b1_dp1_p50',
                         'b1_dp1_p75',
                         'b1_dp1_p95', 'b1_dp1_p99', 'b1_dp1_range', 'b2_dp1_count', 'b2_dp1_min', 'b2_dp1_max',
                         'b2_dp1_mean',
                         'b2_dp1_med', 'b2_dp1_std', 'b2_dp1_p25', 'b2_dp1_p50', 'b2_dp1_p75', 'b2_dp1_p95',
                         'b2_dp1_p99', 'b2_dp1_range',
                         'b3_dp1_count', 'b3_dp1_min', 'b3_dp1_max', 'b3_dp1_mean', 'b3_dp1_med', 'b3_dp1_std',
                         'b3_dp1_p25', 'b3_dp1_p50',
                         'b3_dp1_p75', 'b3_dp1_p95', 'b3_dp1_p99', 'b3_dp1_range']

    # return dja_ym_merge_list, dis_y_merge_list, dis_ym_merge_list, dja_ym_merge_list, dim_y_merge_list, \
    #     dim_y_merge_list, dim_ym_merge_list, dp1_y_merge_list, dp1_ym_merge_list

    # ------------------------------------------------------------------------
    # --------------------------------------------------------------------------

    # define list orders for export dataframe's
    df_list_ = [dka_0112_list,
                dim_0305_list,
                dim_0608_list,
                dim_0911_list,
                dim_1202_list,
                dis_0305_list,
                dis_0608_list,
                dis_0911_list,
                dis_1202_list,
                dbi_0608_list,
                dbi_0911_list,
                dbi_1202_list,
                dja_0305_list,
                dja_0608_list,
                dja_0911_list,
                dja_1202_list,
                h99a2_0112_list,
                fpca2_0509_list,
                stc_0112_list,
                dp1_0112_list,
                dp1_0509_list]

    # df_list_ = [dka_0112_list[0],
    # dim_0305_list[0],
    # dim_0608_list[0],
    # dim_0911_list[0],
    # dim_1202_list[0],
    # dis_0305_list[0],
    # dis_0608_list[0],
    # dis_0911_list[0],
    # dis_1202_list[0],
    # dja_0305_list[0],
    # dja_0608_list[0],
    # dja_0911_list[0],
    # dja_1202_list[0],
    # dbi_0608_list[0],
    # dbi_0911_list[0],
    # dbi_1202_list[0],
    # h99a2_0112_list[0],
    # fpca2_0509_list[0],
    # stc_0112_list[0],
    # dp1_0112_list[0],
    # dp1_0509_list[0]]

    df_str_list_ = ["dka_0112_list",
                    "dim_0305_list",
                    "dim_0608_list",
                    "dim_0911_list",
                    "dim_1202_list",
                    "dis_0305_list",
                    "dis_0608_list",
                    "dis_0911_list",
                    "dis_1202_list",
                    "dbi_0608_list",
                    "dbi_0911_list",
                    "dbi_1202_list",
                    "dja_0305_list",
                    "dja_0608_list",
                    "dja_0911_list",
                    "dja_1202_list",
                    "h99a2_0112_list",
                    "fpca2_0509_list",
                    "stc_0112_list",
                    "dp1_0112_list",
                    "dp1_0509_list"]

    # --------------------------------------------------------
    # --------------------------------------------------------

    # create lists for output df's

    fire_list = []

    dka_0112_list2 = []

    dim_0305_list2 = []
    dim_0608_list2 = []
    dim_0911_list2 = []
    dim_1202_list2 = []

    dis_0305_list2 = []
    dis_0608_list2 = []
    dis_0911_list2 = []
    dis_1202_list2 = []

    # dbi_0305_list = []
    dbi_0608_list2 = []
    dbi_0911_list2 = []
    dbi_1202_list2 = []

    dja_0305_list2 = []
    dja_0608_list2 = []
    dja_0911_list2 = []
    dja_1202_list2 = []

    h99a2_0112_list2 = []
    fpca2_0509_list2 = []

    stc_0112_list2 = []
    dp1_0112_list2 = []
    dp1_0509_list2 = []

    # basal output lists
    dka_0112_basal_list = []

    dim_0305_basal_list = []
    dim_0608_basal_list = []
    dim_0911_basal_list = []
    dim_1202_basal_list = []

    dis_0305_basal_list = []
    dis_0608_basal_list = []
    dis_0911_basal_list = []
    dis_1202_basal_list = []

    # dbi_0305_list = []
    dbi_0608_basal_list = []
    dbi_0911_basal_list = []
    dbi_1202_basal_list = []

    dja_0305_basal_list = []
    dja_0608_basal_list = []
    dja_0911_basal_list = []
    dja_1202_basal_list = []

    h99a2_0112_basal_list = []
    fpca2_0509_basal_list = []

    stc_0112_basal_list = []
    dp1_0112_basal_list = []
    dp1_0509_basal_list = []

    # ensure that bnt_st_ym is an int
    fire_mask.loc[:, "bnt_st_ym"] = fire_mask.loc[:, "bnt_st_ym"].astype(int)

    # for var_, var_df in zip(cleaned_str_list, cleaned_df_list):
    #     print("var: ", var_)
    #     for i in fire_mask.site.unique():
    #
    #         # filter fire mask by site
    #         fire_occ = fire_mask[(fire_mask["site"] == i) & (fire_mask["bnt_st_ym"] == 0)]
    #
    #         # fire_occ = convert_to_dt_year(fire_occ, "bn_year", "bn_y")
    #         fire_occ.loc[:, 'dt_year'] = fire_occ.loc[:, 'bn_year'].astype(int)
    #         fire_occ.sort_values(by="dt_year", inplace=True)
    #         # print(fire_occ)
    #         fire_list.append(fire_occ)
    #
    #         fire_occ_ym = fire_ym_mask[fire_ym_mask["site"] == i]
    #         fire_occ_ym.loc[:, 'dt_ym'] = fire_occ_ym.loc[:, "ym_bfr_fs"].astype(int)
    #         fire_occ_ym.sort_values(by='dt_ym', inplace=True)
    #
    #         # export csv per site to dir
    #         export_csv_file_fn(fire_occ_ym, fms_dir, f"fire_mask_ym_per_{i}.csv")
    #
    #         #         if var_ == "dka_0112":
    #
    #         #             print("LOCATED_"*100)
    #
    #         #             var_filt = var_df[var_df["site"]== i]
    #         #             print("dka len: ", var_filt.shape)
    #         #             #test = convert_to_dt_year(test, "s_year", "bn_y")
    #         #             var_filt.dropna(subset=["s_date"], inplace=True)
    #         #             print("dka len dropna: ", var_filt.shape)
    #         #             var_filt['dt_date'] = var_filt['s_date'].astype(int)
    #         #             var_filt.drop_duplicates(inplace=True)
    #         #             var_filt.sort_values(by="s_date", inplace=True)
    #         #             print("-"*50)
    #         #             print(i)
    #         #             print(var_filt.shape)
    #         #             print(var_filt)
    #         #             print(basal_df)
    #
    #         #                 # merge with basal not fire as is fire
    #         #             dka_merge = pd.merge_asof(basal_df, var_filt, left_on="basal_dt", right_on= "dt_date", by="site", direction="forward")
    #         #             dka_single = dka_merge[dka_s_merge_list]
    #         #             dka_single.rename(columns={'dt_year': 'dt_no_a_fs', 'image_s_dt': 'dka_s_dt'}, inplace=True)
    #
    #         #             dka_0112_basal_list.append(dka_single)
    #
    #         #             import sys
    #         #             sys.exit()
    #         # export_csv_file_fn(dka_s_single, ftzs_dir, "dka_all_seasons_zonal_stats.csv")
    #
    #         # dka_0112_list.append(dka_s)
    #
    #         if var_ == "stc_0112":
    #
    #             # WORKING
    #
    #             # filter variable df by same site
    #             var_filt = var_df[var_df["site"] == i]
    #             print("stc len: ", var_filt.shape)
    #             # test = convert_to_dt_year(test, "s_year", "bn_y")
    #             var_filt.dropna(subset=["s_year"], inplace=True)
    #             print("stc len dropna: ", var_filt.shape)
    #             var_filt['dt_year'] = var_filt['s_year'].astype(int)
    #             var_filt.sort_values(by="dt_year", inplace=True)
    #             print("-" * 50)
    #             print(i)
    #             print(var_filt.shape)
    #
    #             # merge fire scar and variable data on the nearest unburnt date
    #             stc_merge = pd.merge(fire_occ, var_filt, on=["site", "dt_year"], how="inner")
    #
    #             stc_s_fire_mask = stc_merge[stc_s_merge_list]
    #             stc_s_fire_mask.rename(columns={'dt_year': 'dt_no_a_fs', 'image_s_dt': 'stc_s_dt'}, inplace=True)
    #             # append df to list
    #
    #             stc_0112_list2.append(stc_s_fire_mask)
    #
    #             # export site df to csv
    #             export_var_fire_scar_zonal_fn(stc_s_fire_mask, fire_scar_dir, var_, i)
    #
    #             # call the basal merge function to merge no fire year zonal with site basal
    #             stc_basal_nfy = basal_merge_fire_year(basal_df, stc_s_fire_mask, "stc_s_dt", var_, i)
    #             stc_0112_basal_list.append(stc_basal_nfy)
    #             # stc_basal_nfy.to_csv(r"D:\cdu\data\zonal_stats\output\20230128\scratch\{0}_{1}_y_basal_merge_fire_mask.csv".format(i, var_))
    #
    #
    #         elif var_ == "fpca2_0509":
    #
    #             # WORKING
    #
    #             # filter variable df by same site
    #             var_filt = var_df[var_df["site"] == i]
    #
    #             var_filt.dropna(subset=["image_s_dt"], inplace=True)
    #             var_filt.sort_values(by="image_s_dt", inplace=True)
    #
    #             ym_list, y_list = year_month_fn(var_filt, "image_s_dt")
    #
    #             var_filt.loc[:, "dt_ym"] = ym_list
    #             var_filt.loc[:, "dt_ym"] = var_filt["dt_ym"].astype(int)
    #             var_filt.loc[:, "dt_year"] = y_list
    #             var_filt.loc[:, "dt_year"] = var_filt["dt_year"].astype(int)
    #
    #             var_filt_ym = var_filt.copy(deep=True)
    #             print("var_filt_ym: ", var_filt_ym.shape)
    #
    #             # merge fire scar and variable data on the nearest unburnt date
    #             fpca2_0509_merge = pd.merge(fire_occ, var_filt, on=["site", "dt_year"], how="inner")
    #             fpca2_0509_fire_mask = fpca2_0509_merge[fpca2_y_merge_list]
    #             fpca2_0509_fire_mask.rename(columns={'dt_year': 'dt_no_a_fs', 'dt_ym': 'dt_be_ym_fs',
    #                                                  'image_s_dt': 'fpca2_s_dt'}, inplace=True)
    #
    #             fpca2_0509_list2.append(fpca2_0509_fire_mask)
    #
    #             # export file
    #             export_var_fire_scar_zonal_fn(fpca2_0509_fire_mask, fire_scar_dir, var_, i)
    #
    #             # call the basal merge function to merge no fire year zonal with site basal
    #             fpca2_0509_basal_nfy = basal_merge_fire_year(basal_df, fpca2_0509_fire_mask, "fpca2_s_dt", var_, i)
    #             fpca2_0509_basal_list.append(fpca2_0509_basal_nfy)
    #             # fpca2_0509_basal_nfy.to_csv(r"D:\cdu\data\zonal_stats\output\20230128\scratch\{0}_{1}_y_basal_merge_fire_mask.csv".format(i, var_))
    #
    #
    #         elif var_ == "h99a2_0112":
    #
    #             # WORKING
    #
    #             # filter variable df by same site
    #             var_filt = var_df[var_df["site"] == i]
    #             # test = convert_to_dt_year(test, "s_year", "bn_y")
    #             var_filt['dt_year'] = var_filt['s_year'].astype(int)
    #             var_filt.sort_values(by="dt_year", inplace=True)
    #
    #             # merge fire scar and variable data on the nearest unburnt date
    #             h99a_0112_merge = pd.merge(fire_occ, var_filt, on=["site", "dt_year"], how="inner")
    #
    #             h99a_0112_fire_mask = h99a_0112_merge[h99a_merge_list]
    #             h99a_0112_fire_mask.rename(columns={'dt_year': 'dt_no_a_fs', 'image_s_dt': 'h99a_s_dt',
    #                                                 'image_e_dt': 'h99a_e_dt'}, inplace=True)
    #
    #             # h99a_0112_fire_mask.to_csv(r"D:\cdu\data\zonal_stats\output\20230128\scratch\{0}_{1}_y_merge_fire_mask.csv".format(i, var_))
    #
    #             h99a2_0112_list2.append(h99a_0112_fire_mask)
    #             # export file
    #             export_var_fire_scar_zonal_fn(h99a_0112_fire_mask, fire_scar_dir, var_, i)
    #
    #             # call the basal merge function to merge no fire year zonal with site basal
    #             h99a2_basal_nfy = basal_merge_fire_year(basal_df, h99a_0112_fire_mask, "h99a_s_dt", var_, i)
    #             h99a2_0112_basal_list.append(h99a2_basal_nfy)
    #             # h99a2_basal_nfy.to_csv(r"D:\cdu\data\zonal_stats\output\20230128\scratch\{0}_{1}_y_basal_merge_fire_mask.csv".format(i, var_))
    #
    #
    #
    #
    #         elif var_ == "dbi_0608" or var_ == "dbi_0911" or var_ == "dbi_1202":
    #
    #             # NOT WORKING -SEASONAL
    #
    #             # filter variable df by same site
    #             var_filt = var_df[var_df["site"] == i]
    #             # test = convert_to_dt_year(test, "s_year", "bn_y")
    #             var_filt['dt_year'] = var_filt['s_year'].astype(int)
    #             var_filt.sort_values(by="dt_year", inplace=True)
    #             var_filt.to_csv(r"D:\cdu\data\zonal_stats\output\20230204\scratch\{0}_{1}.csv".format(i, var_))
    #             fire_occ.to_csv(r"D:\cdu\data\zonal_stats\output\20230204\scratch\{0}_{1}_fire_occ.csv".format(i, var_))
    #             # merge fire scar and variable data on the nearest unburnt date
    #             dbi_merge = pd.merge(fire_occ, var_filt, on=["site", "dt_year"], how="inner")
    #
    #             # todo - remove this once I have additonal processed dbi landsat mosaics
    #             if dbi_merge.empty:
    #                 print(dbi_merge)
    #
    #                 dbi_merge = var_filt
    #
    #             dbi_s_fire_mask = dbi_merge[dbi_y_merge_list]
    #             dbi_s_fire_mask.rename(columns={'dt_year': 'dt_no_a_fs', 'image_s_dt': 'dbi_s_dt'}, inplace=True)
    #             dbi_s_fire_mask.to_csv(
    #                 r"D:\cdu\data\zonal_stats\output\20230204\scratch\{0}_{1}_mask.csv".format(i, var_))
    #
    #             # call the basal merge function to merge no fire year zonal with site basal
    #             dbi_basal_nfy = basal_merge_fire_year(basal_df, dbi_s_fire_mask, "dbi_s_dt", var_, i)
    #
    #             if var_ == "dbi_0608":
    #                 dbi_0608_list2.append(dbi_s_fire_mask)
    #                 dbi_0608_basal_list.append(dbi_basal_nfy)
    #                 dbi_basal_nfy.to_csv(
    #                     r"D:\cdu\data\zonal_stats\output\20230204\scratch\{0}_{1}_y_basal_merge_fire_mask.csv".format(i,
    #                                                                                                                   var_))
    #
    #             elif var_ == "dbi_0911":
    #                 dbi_0911_list2.append(dbi_s_fire_mask)
    #                 dbi_0911_basal_list.append(dbi_basal_nfy)
    #                 dbi_basal_nfy.to_csv(
    #                     r"D:\cdu\data\zonal_stats\output\20230204\scratch\{0}_{1}_y_basal_merge_fire_mask.csv".format(i,
    #                                                                                                                   var_))
    #
    #
    #             elif var_ == "dbi_1202":
    #                 dbi_1202_list2.append(dbi_s_fire_mask)
    #                 dbi_1202_basal_list.append(dbi_basal_nfy)
    #                 dbi_basal_nfy.to_csv(
    #                     r"D:\cdu\data\zonal_stats\output\20230204\scratch\{0}_{1}_y_basal_merge_fire_mask.csv".format(i,
    #                                                                                                                   var_))
    #
    #             else:
    #                 print("ERROR")
    #
    #             var_split_list = var_.split("_")
    #             var_str = var_split_list[0] + "_" + var_split_list[1]
    #             # export file
    #             #             export_var_fire_scar_zonal_fn(dbi_s_fire_mask, fire_scar_dir, var_, i)
    #             export_var_fire_scar_zonal_fn(dbi_s_fire_mask, fire_scar_dir, var_str, i)
    #
    #         #             # NOTE insufficent data to mask
    #
    #         elif var_ == "dja_0305" or var_ == "dja_0608" or var_ == "dja_0911" or var_ == "dja_1202":
    #
    #             # filter variable df by same site
    #             var_filt = var_df[var_df["site"] == i]
    #             # test = convert_to_dt_year(test, "s_year", "bn_y")
    #             var_filt['dt_year'] = var_filt['s_year'].astype(int)
    #             var_filt.sort_values(by="dt_year", inplace=True)
    #             # var_filt.to_csv(r"D:\cdu\data\zonal_stats\output\20230128\scratch\{0}_{1}.csv".format(i, var_))
    #             # fire_occ.to_csv(r"D:\cdu\data\zonal_stats\output\20230128\scratch\{0}_{1}_fire_occ.csv".format(i, var_))
    #             # merge fire scar and variable data on the nearest unburnt date
    #             dja_merge = pd.merge(fire_occ, var_filt, on=["site", "dt_year"], how="inner")
    #             dja_s_fire_mask = dja_merge[dja_y_merge_list]
    #             dja_s_fire_mask.rename(columns={'dt_year': 'dt_no_a_fs', 'image_s_dt': 'dja_s_dt'}, inplace=True)
    #             # dja_s_fire_mask.to_csv(r"D:\cdu\data\zonal_stats\output\20230128\scratch\{0}_{1}_mask.csv".format(i, var_))
    #
    #             # WORKING
    #             dja_y_merge, var_filt = clean_y_fn(var_df, fire_occ)
    #
    #             column_dict = {'dt_year': 'dt_no_a_fs', 'image_s_dt': 'dja_s_dt'}
    #             dja_y_merge_fire_mask = append_y_merge(dja_y_merge, dja_y_merge_list, column_dict)
    #
    #             # call the basal merge function to merge no fire year zonal with site basal
    #             dja_basal_nfy = basal_merge_fire_year(basal_df, dja_s_fire_mask, "dja_s_dt", var_, i)
    #
    #             if var_ == "dja_0305":
    #                 dja_0305_list2.append(dja_y_merge_fire_mask)
    #
    #                 dja_0305_basal_list.append(dja_basal_nfy)
    #             # dja_basal_nfy.to_csv(r"D:\cdu\data\zonal_stats\output\20230128\scratch\{0}_{1}_y_basal_merge_fire_mask.csv".format(i, var_))
    #
    #             elif var_ == "dja_0608":
    #                 dja_0608_list2.append(dja_y_merge_fire_mask)
    #
    #                 dja_0608_basal_list.append(dja_basal_nfy)
    #                 # dja_basal_nfy.to_csv(r"D:\cdu\data\zonal_stats\output\20230128\scratch\{0}_{1}_y_basal_merge_fire_mask.csv".format(i, var_))
    #
    #             elif var_ == "dja_0911":
    #                 dja_0911_list2.append(dja_y_merge_fire_mask)
    #
    #                 dja_0911_basal_list.append(dja_basal_nfy)
    #                 # dja_basal_nfy.to_csv(r"D:\cdu\data\zonal_stats\output\20230128\scratch\{0}_{1}_y_basal_merge_fire_mask.csv".format(i, var_))
    #
    #             elif var_ == "dja_1202":
    #                 dja_1202_list2.append(dja_y_merge_fire_mask)
    #
    #                 dja_1202_basal_list.append(dja_basal_nfy)
    #                 # dja_basal_nfy.to_csv(r"D:\cdu\data\zonal_stats\output\20230128\scratch\{0}_{1}_y_basal_merge_fire_mask.csv".format(i, var_))
    #
    #             else:
    #                 print("ERROR")
    #
    #
    #         elif var_ == "dis_0305" or var_ == "dis_0608" or var_ == "dis_0911" or var_ == "dis_1202":
    #
    #             # filter variable df by same site
    #             var_filt = var_df[var_df["site"] == i]
    #             # test = convert_to_dt_year(test, "s_year", "bn_y")
    #             var_filt['dt_year'] = var_filt['s_year'].astype(int)
    #             var_filt.sort_values(by="dt_year", inplace=True)
    #             var_filt.to_csv(r"D:\cdu\data\zonal_stats\output\20230128\scratch\{0}_{1}.csv".format(i, var_))
    #             fire_occ.to_csv(r"D:\cdu\data\zonal_stats\output\20230128\scratch\{0}_{1}_fire_occ.csv".format(i, var_))
    #             # merge fire scar and variable data on the nearest unburnt date
    #             dis_merge = pd.merge(fire_occ, var_filt, on=["site", "dt_year"], how="inner")
    #             dis_s_fire_mask = dis_merge[dis_y_merge_list]
    #             dis_s_fire_mask.rename(columns={'dt_year': 'dt_no_a_fs', 'image_s_dt': 'dis_s_dt'}, inplace=True)
    #             # dis_s_fire_mask.to_csv(r"D:\cdu\data\zonal_stats\output\20230128\scratch\{0}_{1}_mask.csv".format(i, var_))
    #
    #             # WORKING
    #             dis_y_merge, var_filt = clean_y_fn(var_df, fire_occ)
    #
    #             column_dict = {'dt_year': 'dt_no_a_fs', 'image_s_dt': 'dis_s_dt'}
    #             dis_y_merge_fire_mask = append_y_merge(dis_y_merge, dis_y_merge_list, column_dict)
    #
    #             # call the basal merge function to merge no fire year zonal with site basal
    #             dis_basal_nfy = basal_merge_fire_year(basal_df, dis_y_merge_fire_mask, "dis_s_dt", var_, i)
    #
    #             if var_ == "dis_0305":
    #                 dis_0305_list2.append(dis_y_merge_fire_mask)
    #                 dis_0305_basal_list.append(dis_basal_nfy)
    #                 # dis_basal_nfy.to_csv(r"D:\cdu\data\zonal_stats\output\20230128\scratch\{0}_{1}_y_basal_merge_fire_mask.csv".format(i, var_))
    #
    #             elif var_ == "dis_0608":
    #                 dis_0608_list2.append(dis_y_merge_fire_mask)
    #                 dis_0608_basal_list.append(dis_basal_nfy)
    #                 # dis_basal_nfy.to_csv(r"D:\cdu\data\zonal_stats\output\20230128\scratch\{0}_{1}_y_basal_merge_fire_mask.csv".format(i, var_))
    #
    #             elif var_ == "dis_0911":
    #                 dis_0911_list2.append(dis_y_merge_fire_mask)
    #                 dis_0911_basal_list.append(dis_basal_nfy)
    #                 # dis_basal_nfy.to_csv(r"D:\cdu\data\zonal_stats\output\20230128\scratch\{0}_{1}_y_basal_merge_fire_mask.csv".format(i, var_))
    #
    #             elif var_ == "dis_1202":
    #                 dis_1202_list2.append(dis_y_merge_fire_mask)
    #                 dis_1202_basal_list.append(dis_basal_nfy)
    #                 # dis_basal_nfy.to_csv(r"D:\cdu\data\zonal_stats\output\20230128\scratch\{0}_{1}_y_basal_merge_fire_mask.csv".format(i, var_))
    #
    #             else:
    #                 print("ERROR")
    #
    #
    #         elif var_ == "dim_0305" or var_ == "dim_0608" or var_ == "dim_0911" or var_ == "dim_1202":
    #
    #             # filter variable df by same site
    #             var_filt = var_df[var_df["site"] == i]
    #             # test = convert_to_dt_year(test, "s_year", "bn_y")
    #             var_filt['dt_year'] = var_filt['s_year'].astype(int)
    #             var_filt.sort_values(by="dt_year", inplace=True)
    #             # var_filt.to_csv(r"D:\cdu\data\zonal_stats\output\20230128\scratch\{0}_{1}.csv".format(i, var_))
    #             # fire_occ.to_csv(r"D:\cdu\data\zonal_stats\output\20230128\scratch\{0}_{1}_fire_occ.csv".format(i, var_))
    #             # merge fire scar and variable data on the nearest unburnt date
    #             dim_merge = pd.merge(fire_occ, var_filt, on=["site", "dt_year"], how="inner")
    #
    #             dim_s_fire_mask = dim_merge[dim_y_merge_list]
    #             dim_s_fire_mask.rename(columns={'dt_year': 'dt_no_a_fs', 'image_s_dt': 'dim_s_dt'}, inplace=True)
    #             # dim_s_fire_mask.to_csv(r"D:\cdu\data\zonal_stats\output\20230128\scratch\{0}_{1}_mask.csv".format(i, var_))
    #
    #             # WORKING
    #             dim_y_merge, var_filt = clean_y_fn(var_df, fire_occ)
    #
    #             column_dict = {'dt_year': 'dt_no_a_fs', 'image_s_dt': 'dim_s_dt'}
    #             dim_y_merge_fire_mask = append_y_merge(dim_y_merge, dim_y_merge_list, column_dict)
    #
    #             # call the basal merge function to merge no fire year zonal with site basal
    #             dim_basal_nfy = basal_merge_fire_year(basal_df, dim_y_merge_fire_mask, "dim_s_dt", var_, i)
    #
    #             if var_ == "dim_0305":
    #                 dim_0305_list2.append(dim_y_merge_fire_mask)
    #                 dim_0305_basal_list.append(dim_basal_nfy)
    #                 # dim_basal_nfy.to_csv(r"D:\cdu\data\zonal_stats\output\20230128\scratch\{0}_{1}_y_basal_merge_fire_mask.csv".format(i, var_))
    #
    #
    #
    #             elif var_ == "dim_0608":
    #                 dim_0608_list2.append(dim_y_merge_fire_mask)
    #                 dim_0608_basal_list.append(dim_basal_nfy)
    #                 # dim_basal_nfy.to_csv(r"D:\cdu\data\zonal_stats\output\20230128\scratch\{0}_{1}_y_basal_merge_fire_mask.csv".format(i, var_))
    #
    #             elif var_ == "dim_0911":
    #                 dim_0911_list2.append(dim_y_merge_fire_mask)
    #
    #                 dim_0911_basal_list.append(dim_basal_nfy)
    #                 # dim_basal_nfy.to_csv(r"D:\cdu\data\zonal_stats\output\20230128\scratch\{0}_{1}_y_basal_merge_fire_mask.csv".format(i, var_))
    #
    #
    #             elif var_ == "dim_1202":
    #                 dim_1202_list2.append(dim_y_merge_fire_mask)
    #                 dim_1202_basal_list.append(dim_basal_nfy)
    #                 # dim_basal_nfy.to_csv(r"D:\cdu\data\zonal_stats\output\20230128\scratch\{0}_{1}_y_basal_merge_fire_mask.csv".format(i, var_))
    #
    #             else:
    #                 print("ERROR")
    #
    #
    #         elif var_ == "dp1_0112" or var_ == "dp1_0509":  # or var_ == "dp1_0911" or var_ == "dp1_1202":
    #
    #             # filter variable df by same site
    #             var_df.dropna(subset=["b1_dp1_mean"], inplace=True)
    #             var_filt = var_df[var_df["site"] == i]
    #             # test = convert_to_dt_year(test, "s_year", "bn_y")
    #             var_filt['dt_year'] = var_filt['s_year'].astype(int)
    #             var_filt.sort_values(by="dt_year", inplace=True)
    #             # var_filt.to_csv(r"D:\cdu\data\zonal_stats\output\20230128\scratch\{0}_{1}.csv".format(i, var_))
    #             # fire_occ.to_csv(r"D:\cdu\data\zonal_stats\output\20230128\scratch\{0}_{1}_fire_occ.csv".format(i, var_))
    #             # merge fire scar and variable data on the nearest unburnt date
    #             dp1_merge = pd.merge(fire_occ, var_filt, on=["site", "dt_year"], how="inner")
    #             dp1_s_fire_mask = dp1_merge[dp1_y_merge_list]
    #             dp1_s_fire_mask.rename(columns={'dt_year': 'dt_no_a_fs', 'image_s_dt': 'dp1_s_dt',
    #                                             'uid_x': 'uid'}, inplace=True)
    #
    #             # call the basal merge function to merge no fire year zonal with site basal
    #             dp1_basal_nfy = basal_merge_fire_year(basal_df, dp1_s_fire_mask, "dp1_s_dt", var_, i)
    #
    #             # append and export data
    #             if var_ == "dp1_0112":
    #                 dp1_0112_list2.append(dp1_s_fire_mask)
    #                 # dp1_s_fire_mask.to_csv(r"D:\cdu\data\zonal_stats\output\20230128\scratch\{0}_{1}_y_merge_fire_mask.csv".format(i, var_))
    #                 dp1_0112_basal_list.append(dp1_basal_nfy)
    #                 # dp1_basal_nfy.to_csv(r"D:\cdu\data\zonal_stats\output\20230128\scratch\{0}_{1}_y_basal_merge_fire_mask.csv".format(i, var_))
    #
    #             elif var_ == "dp1_0509":
    #                 dp1_0509_list2.append(dp1_s_fire_mask)
    #                 # dp1_s_fire_mask.to_csv(r"D:\cdu\data\zonal_stats\output\20230128\scratch\{0}_{1}_fire_mask.csv".format(i, var_))
    #                 dp1_0509_basal_list.append(dp1_basal_nfy)
    #                 # dp1_basal_nfy.to_csv(r"D:\cdu\data\zonal_stats\output\20230128\scratch\{0}_{1}_y_basal_merge_fire_mask.csv".format(i, var_))
    #
    #             else:
    #                 print("ERROR")
    #
    # # complete outputs
    # stc_0112_y = export_csv_fn(stc_0112_list2, fire_scar_dir, "stc_0112_fire_mask.csv")
    # fpca_0509_y = export_csv_fn(fpca2_0509_list2, fire_scar_dir, "fpca2_0509_fire_mask.csv")
    # h99a_0112_y = export_csv_fn(h99a2_0112_list2, fire_scar_dir, "h99a_0112_fire_mask.csv")
    #
    # dbi_0608_y = export_csv_fn(dbi_0608_list2, fire_scar_dir, "dbi_0608_fire_mask.csv")
    # dbi_0911_y = export_csv_fn(dbi_0911_list2, fire_scar_dir, "dbi_0911_fire_mask.csv")
    # dbi_1202_y = export_csv_fn(dbi_1202_list2, fire_scar_dir, "dbi_1202_fire_mask.csv")
    #
    # dja_0305_y = export_csv_fn(dja_0305_list2, fire_scar_dir, "dja_0305_fire_mask.csv")
    # dja_0608_y = export_csv_fn(dja_0608_list2, fire_scar_dir, "dja_0608_fire_mask.csv")
    # dja_0911_y = export_csv_fn(dja_0911_list2, fire_scar_dir, "dja_0911_fire_mask.csv")
    # dja_1202_y = export_csv_fn(dja_1202_list2, fire_scar_dir, "dja_1202_fire_mask.csv")
    #
    # dis_0305_y = export_csv_fn(dis_0305_list2, fire_scar_dir, "dis_0305_fire_mask.csv")
    # dis_0608_y = export_csv_fn(dis_0608_list2, fire_scar_dir, "dis_0608_fire_mask.csv")
    # dis_0911_y = export_csv_fn(dis_0911_list2, fire_scar_dir, "dis_0911_fire_mask.csv")
    # dis_1202_y = export_csv_fn(dis_1202_list2, fire_scar_dir, "dis_1202_fire_mask.csv")
    #
    # dim_0305_y = export_csv_fn(dim_0305_list2, fire_scar_dir, "dim_0305_fire_mask.csv")
    # dim_0608_y = export_csv_fn(dim_0608_list2, fire_scar_dir, "dim_0608_fire_mask.csv")
    # dim_0911_y = export_csv_fn(dim_0911_list2, fire_scar_dir, "dim_0911_fire_mask.csv")
    # dim_1202_y = export_csv_fn(dim_1202_list2, fire_scar_dir, "dim_1202_fire_mask.csv")
    #
    # dp1_0112_y = export_csv_fn(dp1_0112_list2, fire_scar_dir, "dp1_0112_fire_mask.csv")
    # dp1_0509_y = export_csv_fn(dp1_0509_list2, fire_scar_dir, "dp1_0509_fire_mask.csv")
    #
    # # # ------------------------------- Basal ----------------------------------------
    #
    # stc_0112_basal_y = export_csv_fn(stc_0112_basal_list, no_fire_scar_basal_dir,
    #                                  "stc_0112_basal_with_y_fire_mask_applied.csv")
    # fpca_0509_basal_y = export_csv_fn(fpca2_0509_basal_list, no_fire_scar_basal_dir,
    #                                   "fpca2_0509_basal_with_y_fire_mask_applied.csv")
    # h99a_0112_basal_y = export_csv_fn(h99a2_0112_basal_list, no_fire_scar_basal_dir,
    #                                   "h99a_0112_basal_with_y_fire_mask_applied.csv")
    #
    # dbi_0608_basal_y = export_csv_fn(dbi_0608_basal_list, no_fire_scar_basal_dir,
    #                                  "dbi_0608_basal_with_y_fire_mask_applied.csv")
    # dbi_0911_basal_y = export_csv_fn(dbi_0911_basal_list, no_fire_scar_basal_dir,
    #                                  "dbi_0911_basal_with_y_fire_mask_applied.csv")
    # dbi_1202_basal_y = export_csv_fn(dbi_1202_basal_list, no_fire_scar_basal_dir,
    #                                  "dbi_1202_basal_with_y_fire_mask_applied.csv")
    #
    # dja_0305_basal_y = export_csv_fn(dja_0305_basal_list, no_fire_scar_basal_dir,
    #                                  "dja_0305_basal_with_y_fire_mask_applied.csv")
    # dja_0608_basal_y = export_csv_fn(dja_0608_basal_list, no_fire_scar_basal_dir,
    #                                  "dja_0608_basal_with_y_fire_mask_applied.csv")
    # dja_0911_basal_y = export_csv_fn(dja_0911_basal_list, no_fire_scar_basal_dir,
    #                                  "dja_0911_basal_with_y_fire_mask_applied.csv")
    # dja_1202_basal_y = export_csv_fn(dja_1202_basal_list, no_fire_scar_basal_dir,
    #                                  "dja_1202_basal_with_y_fire_mask_applied.csv")
    #
    # dis_0305_basal_y = export_csv_fn(dis_0305_basal_list, no_fire_scar_basal_dir,
    #                                  "dis_0305_basal_with_y_fire_mask_applied.csv")
    # dis_0608_basal_y = export_csv_fn(dis_0608_basal_list, no_fire_scar_basal_dir,
    #                                  "dis_0608_basal_with_y_fire_mask_applied.csv")
    # dis_0911_basal_y = export_csv_fn(dis_0911_basal_list, no_fire_scar_basal_dir,
    #                                  "dis_0911_basal_with_y_fire_mask_applied.csv")
    # dis_1202_basal_y = export_csv_fn(dis_1202_basal_list, no_fire_scar_basal_dir,
    #                                  "dis_1202_basal_with_y_fire_mask_applied.csv")
    #
    # dim_0305_basal_y = export_csv_fn(dim_0305_basal_list, no_fire_scar_basal_dir,
    #                                  "dim_0305_basal_with_y_fire_mask_applied.csv")
    # dim_0608_basal_y = export_csv_fn(dim_0608_basal_list, no_fire_scar_basal_dir,
    #                                  "dim_0608_basal_with_y_fire_mask_applied.csv")
    # dim_0911_basal_y = export_csv_fn(dim_0911_basal_list, no_fire_scar_basal_dir,
    #                                  "dim_0911_basal_with_y_fire_mask_applied.csv")
    # dim_1202_basal_y = export_csv_fn(dim_1202_basal_list, no_fire_scar_basal_dir,
    #                                  "dim_1202_basal_with_y_fire_mask_applied.csv")
    #
    # dp1_0112_basal_y = export_csv_fn(dp1_0112_basal_list, no_fire_scar_basal_dir,
    #                                  "dp1_0112_basal_with_y_fire_mask_applied.csv")
    # dp1_0509_basal_y = export_csv_fn(dp1_0509_basal_list, no_fire_scar_basal_dir,
    #                                  "dp1_0509_basal_with_y_fire_mask_applied.csv")
    #
    # # Fire mask output csv
    # export_csv_fn(fire_list, fire_mask_dir, "fire_mask.csv")
    #
    # df = dim_0305_basal_y
    # col_names = df.columns.tolist()
    # band = 1
    # res = [i for i in col_names if f"b{band}" in i]
    # res.append('bio_agb_kg1ha')
    # # print(res)
    # df1 = df[res]
    # # print(df1)
    # corr_matrix = df1.corr()
    # print(corr_matrix)

    # --------------------------------------------------------------------------------
    # Fire scars taken into consideration


if __name__ == '__main__':
    main_routine()
