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
from __future__ import division
# import modules
import pandas as pd
from glob import glob
import os
from calendar import monthrange
from datetime import datetime
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import geopandas as gpd

pd.set_option('chained_assignment', None)


def convert_int_to_float(df):
    # converts integer stage code dbg surface reflectance data for landsat 5 & 7
    # to floating point for analysis of the vegetation index, blue band not used

    blue = ((df['psB1a'].astype('int16') * 0.0001) + 0.0)
    green = ((df['psB2a'].astype('int16') * 0.0001) + 0.0)
    red = ((df['psB3a'].astype('int16') * 0.0001) + 0.0)
    nir = ((df['psB4a'].astype('int16') * 0.0001) + 0.0)
    swir1 = ((df['psB5a'].astype('int16') * 0.0001) + 0.0)
    swir2 = ((df['psB6a'].astype('int16') * 0.0001) + 0.0)

    print("blue: ", blue)
    print("green: ", green)
    print("red: ", red)
    print("nir: ", nir)
    print("swir1: ", swir1)
    print("swir2: ", swir2)

    # import sys
    # sys.exit()
    return blue, green, red, nir, swir1, swir2


def calculate_band_ratio(df):
    # calculate the band ratios

    df['ratio32fa'] = (df['psB3a'] / df['psB2a'])
    df['ratio42fa'] = (df['psB4a'] / df['psB2a'])
    df['ratio43fa'] = (df['psB4a'] / df['psB3a'])
    df['ratio52fa'] = (df['psB5a'] / df['psB2a'])
    df['ratio53fa'] = (df['psB5a'] / df['psB3a'])
    df['ratio54fa'] = (df['psB5a'] / df['psB4a'])
    df['ratio62fa'] = (df['psB6a'] / df['psB2a'])
    df['ratio63fa'] = (df['psB6a'] / df['psB3a'])
    df['ratio64fa'] = (df['psB6a'] / df['psB4a'])
    df['ratio65fa'] = (df['psB6a'] / df['psB5a'])

    # calculate the band ratios and convert them to int32 bit at 7 decimal places

    df['ratio32a'] = np.int32(np.around(df['ratio32fa'] * 10 ** 7))

    df['ratio42a'] = np.int32(np.around(df['ratio42fa'] * 10 ** 7))

    df['ratio43a'] = np.int32(np.around(df['ratio43fa'] * 10 ** 7))

    df['ratio52a'] = np.int32(np.around(df['ratio52fa'] * 10 ** 7))

    df['ratio53a'] = np.int32(np.around(df['ratio53fa'] * 10 ** 7))

    df['ratio54a'] = np.int32(np.around(df['ratio54fa'] * 10 ** 7))

    df['ratio62a'] = np.int32(np.around(df['ratio62fa'] * 10 ** 7))

    df['ratio63a'] = np.int32(np.around(df['ratio63fa'] * 10 ** 7))

    df['ratio64a'] = np.int32(np.around(df['ratio64fa'] * 10 ** 7))

    df['ratio65a'] = np.int32(np.around(df['ratio65fa'] * 10 ** 7))

    return df


def calculate_veg_indices(df, blue, green, red, nir, swir1, swir2):

    # calculate the vegetation indices
    df['GSAVIfa'] = ((nir - green) / (nir + green + 0.5)) * (1.5)
    df['GSAVIa'] = np.int32(np.around(df['GSAVIfa'] * 10 ** 7))

    df['GNDVIfa'] = (nir - green) / (nir + green)
    df['GNDVIa'] = np.int32(np.around(df['GNDVIfa'] * 10 ** 7))

    df['CVIfa'] = (nir / green) * (red / green)
    df['CVIa'] = np.int32(np.around(df['CVIfa'] * 10 ** 7))

    df['NDGIfa'] = (green - red) / (green + red)
    df['NDGIa'] = np.int32(np.around(df['NDGIfa'] * 10 ** 7))

    df['RIfa'] = (red - green) / (red + green)
    df['RIa'] = np.int32(np.around(df['RIfa'] * 10 ** 7))

    df['NBRfa'] = (nir - swir2) / (nir + swir2)
    df['NBRa'] = np.int32(np.around(df['NBRfa'] * 10 ** 7))

    df['NDIIfa'] = (nir - swir1) / (nir + swir1)
    df['NDIIa'] = np.int32(np.around(df['NDIIfa'] * 10 ** 7))

    df['GDVIfa'] = (nir - green)
    df['GDVIa'] = np.int32(np.around(df['GDVIfa'] * 10 ** 7))

    df['MSAVIfa'] = (2 * nir + 1 - np.sqrt((np.power(((2 * nir) + 1), 2)) - (8 * (nir - red)))) / 2
    df['MSAVIa'] = np.int32(np.around(df['MSAVIfa'] * 10 ** 7))

    df['DVIfa'] = (nir - red)
    df['DVIa'] = np.int32(np.around(df['DVIfa'] * 10 ** 7))

    df['SAVIfa'] = ((nir - red) / (nir + red + 0.5)) * (1 + 0.5)
    df['SAVIa'] = np.int32(np.around(df['SAVIfa'] * 10 ** 7))

    df['NDVIfa'] = (nir - red) / (nir + red)
    df['NDVIa'] = np.int32(np.around(df['NDVIfa'] * 10 ** 7))

    df['MSRfa'] = (((nir / red) - 1) / ((np.sqrt(nir / red)) + 1))
    df['MSRa'] = np.int32(np.around(df['MSRfa'] * 10 ** 7))

    return df


def main_routine(df_all):
    # dbi annual fire mask 6 band mean values
    dbifm_annual_df = df_all[[
        'uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94', 'geometry',
        'b1_dbifm_mean', 'b2_dbifm_mean', 'b3_dbifm_mean', 'b4_dbifm_mean', 'b5_dbifm_mean', 'b6_dbifm_mean',
    ]]

    # dbi dry season fire mask 6 band mean values
    dbifm_dry_df = df_all[[
        'uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94', 'geometry',
        'b1_dbifm_dry_mean', 'b2_dbifm_dry_mean', 'b3_dbifm_dry_mean', 'b4_dbifm_dry_mean',
        'b5_dbifm_dry_mean', 'b6_dbifm_dry_mean',
    ]]

    # dbi annual 6 band mean values
    dbi_annual_df = df_all[[
        'uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94', 'geometry',
        'b1_dbi_mean', 'b2_dbi_mean', 'b3_dbi_mean', 'b4_dbi_mean', 'b5_dbi_mean', 'b6_dbi_mean',
    ]]

    # dbi dry season 6 band mean values
    dbi_dry_df = df_all[[
        'uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94', 'geometry',
        'b1_dbi_dry_mean', 'b2_dbi_dry_mean', 'b3_dbi_dry_mean', 'b4_dbi_dry_mean', 'b5_dbi_dry_mean',
        'b6_dbi_dry_mean',
    ]]

    # dbg 6 fire mask band mean values
    dbg_fire_df = df_all[[
        'uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94', 'geometry',
        'b1_dbgfm_mean', 'b2_dbgfm_mean', 'b3_dbgfm_mean', 'b4_dbgfm_mean', 'b5_dbgfm_mean', 'b6_dbgfm_mean',
    ]]

    # dbg 6 band mean values
    dbg_df = df_all[[
        'uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94', 'geometry',
        'b1_dbg_mean', 'b2_dbg_mean', 'b3_dbg_mean', 'b4_dbg_mean', 'b5_dbg_mean', 'b6_dbg_mean',

    ]]

    list_df = [dbifm_annual_df, dbifm_dry_df, dbi_annual_df, dbi_dry_df, dbg_fire_df, dbg_df]
    list_str_df = ["dbifman_df", "dbifmdry_df", "dbian_df", "dbidry_df", "dbgfm_df", "dbg_df"]

    out_df_list = []

    for df, str_df in zip(list_df, list_str_df):
        print(type(df))

        df.dropna(inplace=True)
        print(str_df, df.shape)

        print(df.columns)
        print(len(df.columns))
        #print("df: ", df)
        #df.to_csv(r"C:\Users\robot\projects\biomass\scratch\check_df.csv", index=False)
        # import sys
        # sys.exit()
        column = ['uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94', 'geometry',
                  "psB1a", "psB2a", "psB3a", "psB4a", "psB5a", "psB6a", ]
        print(len(column))

        df.set_axis(column, axis=1, inplace=True)

        # import sys
        # sys.exit()

        blue, green, red, nir, swir1, swir2 = convert_int_to_float(df)
        df = calculate_band_ratio(df)
        #df.to_csv(r"C:\Users\robot\projects\biomass\scratch\ratio.csv", index=False)

        df = calculate_veg_indices(df, blue, green, red, nir, swir1, swir2)
        #df.to_csv(r"C:\Users\robot\projects\biomass\scratch\si_check.csv", index=False)

        # import sys
        # sys.exit()
        # remove fa values
        out_df = df[
            ['uid', 'site_clean', "date", 'lon_gda94', 'lat_gda94', 'geometry', 'psB1a', 'psB2a', 'psB3a', 'psB4a',
             'psB5a', 'psB6a',
             'ratio32a', 'ratio42a', 'ratio43a', 'ratio52a', 'ratio53a', 'ratio54a', 'ratio62a', 'ratio63a',
             'ratio64a', 'ratio65a', 'GSAVIa', 'GNDVIa', 'CVIa', 'NDGIa', 'RIa',
             'NBRa', 'NDIIa', 'GDVIa', 'MSAVIa', 'DVIa', 'SAVIa', 'NDVIa', 'MSRa']]


        out_df.to_csv(r"C:\Users\robot\projects\biomass\scratch\out_df_si_check.csv", index=False)
        print(str_df)
        list_str = str_df.split("_")
        print(list_str)
        print("-" * 50)

        # import sys
        # sys.exit()
        column = [
            'uid', 'site_clean', "date", 'lon_gda94', 'lat_gda94', 'geometry',
            f'{list_str[0]}_psB1a', f'{list_str[0]}_psB2a', f'{list_str[0]}_psB3a', f'{list_str[0]}_psB4a',
            f'{list_str[0]}_psB5a', f'{list_str[0]}_psB6a',
            f'{list_str[0]}_r32', f'{list_str[0]}_r42', f'{list_str[0]}_r43', f'{list_str[0]}_r52',
            f'{list_str[0]}_r53', f'{list_str[0]}_r54', f'{list_str[0]}_r62', f'{list_str[0]}_r63',
            f'{list_str[0]}_r64', f'{list_str[0]}_r65', f'{list_str[0]}_GSAVI', f'{list_str[0]}_GNDVI',
            f'{list_str[0]}_CVI', f'{list_str[0]}_NDGI', f'{list_str[0]}_RI', f'{list_str[0]}_NBR',
            f'{list_str[0]}_NDII', f'{list_str[0]}_GDVI', f'{list_str[0]}_MSAVI', f'{list_str[0]}_DVI',
            f'{list_str[0]}_SAVI', f'{list_str[0]}_NDVI', f'{list_str[0]}_MSR']
        print(len(column))

        out_df.set_axis(column, axis=1, inplace=True)

        out_df_list.append(out_df)

        for df, str_df in zip(out_df_list, list_str_df):
            output = os.path.join(r"C:\Users\robot\projects\biomass\collated_zonal_stats\veg_ind", f"{str_df}_veg_indices.csv")
            df.to_csv(output, index=False)

    # --------------------------------------- Merge vegetation indices  ----------------------------------------

    df01 = pd.merge(right=out_df_list[0], left=out_df_list[1], how="outer",
                    on=['uid', 'site_clean', "date", 'lon_gda94', 'lat_gda94', 'geometry', ])

    df01.to_csv(r"C:\Users\robot\projects\biomass\collated_zonal_stats\sr_fc\df01.csv",
                index=False)

    df012 = pd.merge(right=df01, left=out_df_list[2], how="outer",
                     on=['uid', 'site_clean', "date", 'lon_gda94', 'lat_gda94', 'geometry', ])

    df012.to_csv(r"C:\Users\robot\projects\biomass\collated_zonal_stats\sr_fc\df012.csv",
                 index=False)

    df0123 = pd.merge(right=df012, left=out_df_list[3], how="outer",
                      on=['uid', 'site_clean', "date", 'lon_gda94', 'lat_gda94', 'geometry', ])

    df0123.to_csv(r"C:\Users\robot\projects\biomass\collated_zonal_stats\sr_fc\df0123.csv",
                  index=False)

    df01234 = pd.merge(right=df0123, left=out_df_list[4], how="outer",
                       on=['uid', 'site_clean', "date", 'lon_gda94', 'lat_gda94', 'geometry', ])

    df01234.to_csv(r"C:\Users\robot\projects\biomass\collated_zonal_stats\sr_fc\df01234.csv",
                   index=False)

    df012345 = pd.merge(right=df01234, left=out_df_list[5], how="outer",
                        on=['uid', 'site_clean', "date", 'lon_gda94', 'lat_gda94', 'geometry', ])

    df012345.to_csv(r"C:\Users\robot\projects\biomass\collated_zonal_stats\sr_fc\df01234.csv",
                    index=False)

    df_all_filter = df_all[['uid', 'site_clean', "date", 'lon_gda94', 'lat_gda94', 'geometry', ]]
    df_all_012345 = pd.merge(right=df012345, left=df_all, how="outer",
                             on=['uid', 'site_clean', "date", 'lon_gda94', 'lat_gda94', 'geometry', ])

    df_all_012345.to_csv(r"C:\Users\robot\projects\biomass\collated_zonal_stats\sr_fc\df_all_01234.csv",
                         index=False)

    print(list(df_all_012345.columns))
    
    # import sys
    # sys.exit()

    column = ['uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94', 'geometry', 'bio_l_kg1ha', 'bio_t_kg1ha',
              'bio_b_kg1ha',
              'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha', 'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
              'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha', 'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt',
              'b1_dp1fm_min',
              'b1_dp1fm_max', 'b1_dp1fm_mean', 'b1_dp1fm_std', 'b1_dp1fm_med', 'b1_dp1fm_p25', 'b1_dp1fm_p50',
              'b1_dp1fm_p75',
              'b1_dp1fm_p95', 'b1_dp1fm_p99', 'b2_dp1fm_min', 'b2_dp1fm_max', 'b2_dp1fm_mean', 'b2_dp1fm_std',
              'b2_dp1fm_med',
              'b2_dp1fm_p25', 'b2_dp1fm_p50', 'b2_dp1fm_p75', 'b2_dp1fm_p95', 'b2_dp1fm_p99', 'b3_dp1fm_min',
              'b3_dp1fm_max',
              'b3_dp1fm_mean', 'b3_dp1fm_std', 'b3_dp1fm_med', 'b3_dp1fm_p25', 'b3_dp1fm_p50', 'b3_dp1fm_p75',
              'b3_dp1fm_p95',
              'b3_dp1fm_p99', 'b1_dp1fm_dry_min', 'b1_dp1fm_dry_max', 'b1_dp1fm_dry_mean', 'b1_dp1fm_dry_std',
              'b1_dp1fm_dry_med',
              'b1_dp1fm_dry_p25', 'b1_dp1fm_dry_p50', 'b1_dp1fm_dry_p75', 'b1_dp1fm_dry_p95', 'b1_dp1fm_dry_p99',
              'b2_dp1fm_dry_min', 'b2_dp1fm_dry_max', 'b2_dp1fm_dry_mean', 'b2_dp1fm_dry_std', 'b2_dp1fm_dry_med',
              'b2_dp1fm_dry_p25', 'b2_dp1fm_dry_p50', 'b2_dp1fm_dry_p75', 'b2_dp1fm_dry_p95', 'b2_dp1fm_dry_p99',
              'b3_dp1fm_dry_min', 'b3_dp1fm_dry_max', 'b3_dp1fm_dry_mean', 'b3_dp1fm_dry_std', 'b3_dp1fm_dry_med',
              'b3_dp1fm_dry_p25', 'b3_dp1fm_dry_p50', 'b3_dp1fm_dry_p75', 'b3_dp1fm_dry_p95', 'b3_dp1fm_dry_p99',
              'b1_dp1_min', 'b1_dp1_max', 'b1_dp1_mean', 'b1_dp1_std', 'b1_dp1_med', 'b1_dp1_p25', 'b1_dp1_p50',
              'b1_dp1_p75', 'b1_dp1_p95', 'b1_dp1_p99', 'b2_dp1_min', 'b2_dp1_max', 'b2_dp1_mean', 'b2_dp1_std',
              'b2_dp1_med', 'b2_dp1_p25', 'b2_dp1_p50', 'b2_dp1_p75', 'b2_dp1_p95', 'b2_dp1_p99', 'b3_dp1_min',
              'b3_dp1_max', 'b3_dp1_mean', 'b3_dp1_std', 'b3_dp1_med', 'b3_dp1_p25', 'b3_dp1_p50', 'b3_dp1_p75',
              'b3_dp1_p95', 'b3_dp1_p99', 'b1_dp1_dry_min', 'b1_dp1_dry_max', 'b1_dp1_dry_mean', 'b1_dp1_dry_std',
              'b1_dp1_dry_med', 'b1_dp1_dry_p25', 'b1_dp1_dry_p50', 'b1_dp1_dry_p75', 'b1_dp1_dry_p95',
              'b1_dp1_dry_p99', 'b2_dp1_dry_min', 'b2_dp1_dry_max', 'b2_dp1_dry_mean', 'b2_dp1_dry_std',
              'b2_dp1_dry_med', 'b2_dp1_dry_p25', 'b2_dp1_dry_p50', 'b2_dp1_dry_p75', 'b2_dp1_dry_p95',
              'b2_dp1_dry_p99', 'b3_dp1_dry_min', 'b3_dp1_dry_max', 'b3_dp1_dry_mean', 'b3_dp1_dry_std',
              'b3_dp1_dry_med', 'b3_dp1_dry_p25', 'b3_dp1_dry_p50', 'b3_dp1_dry_p75', 'b3_dp1_dry_p95',
              'b3_dp1_dry_p99', 'b1_dp0fm_min', 'b1_dp0fm_max', 'b1_dp0fm_mean', 'b1_dp0fm_std', 'b1_dp0fm_med',
              'b1_dp0fm_p25', 'b1_dp0fm_p50', 'b1_dp0fm_p75', 'b1_dp0fm_p95', 'b1_dp0fm_p99', 'b2_dp0fm_min',
              'b2_dp0fm_max', 'b2_dp0fm_mean', 'b2_dp0fm_std', 'b2_dp0fm_med', 'b2_dp0fm_p25', 'b2_dp0fm_p50',
              'b2_dp0fm_p75', 'b2_dp0fm_p95', 'b2_dp0fm_p99', 'b3_dp0fm_min', 'b3_dp0fm_max', 'b3_dp0fm_mean',
              'b3_dp0fm_std', 'b3_dp0fm_med', 'b3_dp0fm_p25', 'b3_dp0fm_p50', 'b3_dp0fm_p75', 'b3_dp0fm_p95',
              'b3_dp0fm_p99', 'b1_dp0_min', 'b1_dp0_max', 'b1_dp0_mean', 'b1_dp0_std', 'b1_dp0_med', 'b1_dp0_p25',
              'b1_dp0_p50', 'b1_dp0_p75', 'b1_dp0_p95', 'b1_dp0_p99', 'b2_dp0_min', 'b2_dp0_max', 'b2_dp0_mean',
              'b2_dp0_std', 'b2_dp0_med', 'b2_dp0_p25', 'b2_dp0_p50', 'b2_dp0_p75', 'b2_dp0_p95', 'b2_dp0_p99',
              'b3_dp0_min', 'b3_dp0_max', 'b3_dp0_mean', 'b3_dp0_std', 'b3_dp0_med', 'b3_dp0_p25', 'b3_dp0_p50',
              'b3_dp0_p75', 'b3_dp0_p95', 'b3_dp0_p99', 'b1_dbifm_min', 'b1_dbifm_max', 'b1_dbifm_mean', 'b1_dbifm_std',
              'b1_dbifm_med', 'b1_dbifm_p25', 'b1_dbifm_p50', 'b1_dbifm_p75', 'b1_dbifm_p95', 'b1_dbifm_p99',
              'b2_dbifm_min', 'b2_dbifm_max', 'b2_dbifm_mean', 'b2_dbifm_std', 'b2_dbifm_med', 'b2_dbifm_p25',
              'b2_dbifm_p50', 'b2_dbifm_p75', 'b2_dbifm_p95', 'b2_dbifm_p99', 'b3_dbifm_min', 'b3_dbifm_max',
              'b3_dbifm_mean', 'b3_dbifm_std', 'b3_dbifm_med', 'b3_dbifm_p25', 'b3_dbifm_p50', 'b3_dbifm_p75',
              'b3_dbifm_p95', 'b3_dbifm_p99', 'b4_dbifm_min', 'b4_dbifm_max', 'b4_dbifm_mean', 'b4_dbifm_std',
              'b4_dbifm_med', 'b4_dbifm_p25', 'b4_dbifm_p50', 'b4_dbifm_p75', 'b4_dbifm_p95', 'b4_dbifm_p99',
              'b5_dbifm_min', 'b5_dbifm_max', 'b5_dbifm_mean', 'b5_dbifm_std', 'b5_dbifm_med', 'b5_dbifm_p25',
              'b5_dbifm_p50', 'b5_dbifm_p75', 'b5_dbifm_p95', 'b5_dbifm_p99', 'b6_dbifm_min', 'b6_dbifm_max',
              'b6_dbifm_mean', 'b6_dbifm_std', 'b6_dbifm_med', 'b6_dbifm_p25', 'b6_dbifm_p50', 'b6_dbifm_p75',
              'b6_dbifm_p95', 'b6_dbifm_p99', 'b1_dbifm_dry_min', 'b1_dbifm_dry_max', 'b1_dbifm_dry_mean',
              'b1_dbifm_dry_std', 'b1_dbifm_dry_med', 'b1_dbifm_dry_p25', 'b1_dbifm_dry_p50', 'b1_dbifm_dry_p75',
              'b1_dbifm_dry_p95', 'b1_dbifm_dry_p99', 'b2_dbifm_dry_min', 'b2_dbifm_dry_max', 'b2_dbifm_dry_mean',
              'b2_dbifm_dry_std', 'b2_dbifm_dry_med', 'b2_dbifm_dry_p25', 'b2_dbifm_dry_p50', 'b2_dbifm_dry_p75',
              'b2_dbifm_dry_p95', 'b2_dbifm_dry_p99', 'b3_dbifm_dry_min', 'b3_dbifm_dry_max', 'b3_dbifm_dry_mean',
              'b3_dbifm_dry_std', 'b3_dbifm_dry_med', 'b3_dbifm_dry_p25', 'b3_dbifm_dry_p50', 'b3_dbifm_dry_p75',
              'b3_dbifm_dry_p95', 'b3_dbifm_dry_p99', 'b4_dbifm_dry_min', 'b4_dbifm_dry_max', 'b4_dbifm_dry_mean',
              'b4_dbifm_dry_std', 'b4_dbifm_dry_med', 'b4_dbifm_dry_p25', 'b4_dbifm_dry_p50', 'b4_dbifm_dry_p75',
              'b4_dbifm_dry_p95', 'b4_dbifm_dry_p99', 'b5_dbifm_dry_min', 'b5_dbifm_dry_max', 'b5_dbifm_dry_mean',
              'b5_dbifm_dry_std', 'b5_dbifm_dry_med', 'b5_dbifm_dry_p25', 'b5_dbifm_dry_p50', 'b5_dbifm_dry_p75',
              'b5_dbifm_dry_p95', 'b5_dbifm_dry_p99', 'b6_dbifm_dry_min', 'b6_dbifm_dry_max', 'b6_dbifm_dry_mean',
              'b6_dbifm_dry_std', 'b6_dbifm_dry_med', 'b6_dbifm_dry_p25', 'b6_dbifm_dry_p50', 'b6_dbifm_dry_p75',
              'b6_dbifm_dry_p95', 'b6_dbifm_dry_p99', 'b1_dbi_min', 'b1_dbi_max', 'b1_dbi_mean', 'b1_dbi_std',
              'b1_dbi_med', 'b1_dbi_p25', 'b1_dbi_p50', 'b1_dbi_p75', 'b1_dbi_p95', 'b1_dbi_p99', 'b2_dbi_min',
              'b2_dbi_max', 'b2_dbi_mean', 'b2_dbi_std', 'b2_dbi_med', 'b2_dbi_p25', 'b2_dbi_p50', 'b2_dbi_p75',
              'b2_dbi_p95', 'b2_dbi_p99', 'b3_dbi_min', 'b3_dbi_max', 'b3_dbi_mean', 'b3_dbi_std', 'b3_dbi_med',
              'b3_dbi_p25', 'b3_dbi_p50', 'b3_dbi_p75', 'b3_dbi_p95', 'b3_dbi_p99', 'b4_dbi_min', 'b4_dbi_max',
              'b4_dbi_mean', 'b4_dbi_std', 'b4_dbi_med', 'b4_dbi_p25', 'b4_dbi_p50', 'b4_dbi_p75', 'b4_dbi_p95',
              'b4_dbi_p99', 'b5_dbi_min', 'b5_dbi_max', 'b5_dbi_mean', 'b5_dbi_std', 'b5_dbi_med', 'b5_dbi_p25',
              'b5_dbi_p50', 'b5_dbi_p75', 'b5_dbi_p95', 'b5_dbi_p99', 'b6_dbi_min', 'b6_dbi_max', 'b6_dbi_mean',
              'b6_dbi_std', 'b6_dbi_med', 'b6_dbi_p25', 'b6_dbi_p50', 'b6_dbi_p75', 'b6_dbi_p95', 'b6_dbi_p99',
              'b1_dbi_dry_min', 'b1_dbi_dry_max', 'b1_dbi_dry_mean', 'b1_dbi_dry_std', 'b1_dbi_dry_med',
              'b1_dbi_dry_p25', 'b1_dbi_dry_p50', 'b1_dbi_dry_p75', 'b1_dbi_dry_p95', 'b1_dbi_dry_p99',
              'b2_dbi_dry_min', 'b2_dbi_dry_max', 'b2_dbi_dry_mean', 'b2_dbi_dry_std', 'b2_dbi_dry_med',
              'b2_dbi_dry_p25', 'b2_dbi_dry_p50', 'b2_dbi_dry_p75', 'b2_dbi_dry_p95', 'b2_dbi_dry_p99',
              'b3_dbi_dry_min', 'b3_dbi_dry_max', 'b3_dbi_dry_mean', 'b3_dbi_dry_std', 'b3_dbi_dry_med',
              'b3_dbi_dry_p25', 'b3_dbi_dry_p50', 'b3_dbi_dry_p75', 'b3_dbi_dry_p95', 'b3_dbi_dry_p99',
              'b4_dbi_dry_min', 'b4_dbi_dry_max', 'b4_dbi_dry_mean', 'b4_dbi_dry_std', 'b4_dbi_dry_med',
              'b4_dbi_dry_p25', 'b4_dbi_dry_p50', 'b4_dbi_dry_p75', 'b4_dbi_dry_p95', 'b4_dbi_dry_p99',
              'b5_dbi_dry_min', 'b5_dbi_dry_max', 'b5_dbi_dry_mean', 'b5_dbi_dry_std', 'b5_dbi_dry_med',
              'b5_dbi_dry_p25', 'b5_dbi_dry_p50', 'b5_dbi_dry_p75', 'b5_dbi_dry_p95', 'b5_dbi_dry_p99',
              'b6_dbi_dry_min', 'b6_dbi_dry_max', 'b6_dbi_dry_mean', 'b6_dbi_dry_std', 'b6_dbi_dry_med',
              'b6_dbi_dry_p25', 'b6_dbi_dry_p50', 'b6_dbi_dry_p75', 'b6_dbi_dry_p95', 'b6_dbi_dry_p99',
              'b1_dbgfm_min', 'b1_dbgfm_max', 'b1_dbgfm_mean', 'b1_dbgfm_std', 'b1_dbgfm_med', 'b1_dbgfm_p25',
              'b1_dbgfm_p50', 'b1_dbgfm_p75', 'b1_dbgfm_p95', 'b1_dbgfm_p99', 'b2_dbgfm_min', 'b2_dbgfm_max',
              'b2_dbgfm_mean', 'b2_dbgfm_std', 'b2_dbgfm_med', 'b2_dbgfm_p25', 'b2_dbgfm_p50', 'b2_dbgfm_p75',
              'b2_dbgfm_p95', 'b2_dbgfm_p99', 'b3_dbgfm_min', 'b3_dbgfm_max', 'b3_dbgfm_mean', 'b3_dbgfm_std',
              'b3_dbgfm_med', 'b3_dbgfm_p25', 'b3_dbgfm_p50', 'b3_dbgfm_p75', 'b3_dbgfm_p95', 'b3_dbgfm_p99',
              'b4_dbgfm_min', 'b4_dbgfm_max', 'b4_dbgfm_mean', 'b4_dbgfm_std', 'b4_dbgfm_med', 'b4_dbgfm_p25',
              'b4_dbgfm_p50', 'b4_dbgfm_p75', 'b4_dbgfm_p95', 'b4_dbgfm_p99', 'b5_dbgfm_min', 'b5_dbgfm_max',
              'b5_dbgfm_mean', 'b5_dbgfm_std', 'b5_dbgfm_med', 'b5_dbgfm_p25', 'b5_dbgfm_p50', 'b5_dbgfm_p75',
              'b5_dbgfm_p95', 'b5_dbgfm_p99', 'b6_dbgfm_min', 'b6_dbgfm_max', 'b6_dbgfm_mean', 'b6_dbgfm_std',
              'b6_dbgfm_med', 'b6_dbgfm_p25', 'b6_dbgfm_p50', 'b6_dbgfm_p75', 'b6_dbgfm_p95', 'b6_dbgfm_p99',
              'b1_dbg_min', 'b1_dbg_max', 'b1_dbg_mean', 'b1_dbg_std', 'b1_dbg_med', 'b1_dbg_p25', 'b1_dbg_p50',
              'b1_dbg_p75', 'b1_dbg_p95', 'b1_dbg_p99', 'b2_dbg_min', 'b2_dbg_max', 'b2_dbg_mean', 'b2_dbg_std',
              'b2_dbg_med', 'b2_dbg_p25', 'b2_dbg_p50', 'b2_dbg_p75', 'b2_dbg_p95', 'b2_dbg_p99', 'b3_dbg_min',
              'b3_dbg_max', 'b3_dbg_mean', 'b3_dbg_std', 'b3_dbg_med', 'b3_dbg_p25', 'b3_dbg_p50', 'b3_dbg_p75',
              'b3_dbg_p95', 'b3_dbg_p99', 'b4_dbg_min', 'b4_dbg_max', 'b4_dbg_mean', 'b4_dbg_std', 'b4_dbg_med',
              'b4_dbg_p25', 'b4_dbg_p50', 'b4_dbg_p75', 'b4_dbg_p95', 'b4_dbg_p99', 'b5_dbg_min', 'b5_dbg_max',
              'b5_dbg_mean', 'b5_dbg_std', 'b5_dbg_med', 'b5_dbg_p25', 'b5_dbg_p50', 'b5_dbg_p75', 'b5_dbg_p95',
              'b5_dbg_p99', 'b6_dbg_min', 'b6_dbg_max', 'b6_dbg_mean', 'b6_dbg_std', 'b6_dbg_med', 'b6_dbg_p25',
              'b6_dbg_p50', 'b6_dbg_p75', 'b6_dbg_p95', 'b6_dbg_p99', 'dbg_psB1a', 'dbg_psB2a', 'dbg_psB3a',
              'dbg_psB4a', 'dbg_psB5a', 'dbg_psB6a', 'dbg_r32', 'dbg_r42', 'dbg_r43', 'dbg_r52', 'dbg_r53',
              'dbg_r54', 'dbg_r62', 'dbg_r63', 'dbg_r64', 'dbg_r65', 'dbg_GSAVI', 'dbg_GNDVI', 'dbg_CVI',
              'dbg_NDGI', 'dbg_RI', 'dbg_NBR', 'dbg_NDII', 'dbg_GDVI', 'dbg_MSAVI', 'dbg_DVI', 'dbg_SAVI',
              'dbg_NDVI', 'dbg_MSR', 'dbgfm_psB1a', 'dbgfm_psB2a', 'dbgfm_psB3a', 'dbgfm_psB4a', 'dbgfm_psB5a',
              'dbgfm_psB6a', 'dbgfm_r32', 'dbgfm_r42', 'dbgfm_r43', 'dbgfm_r52', 'dbgfm_r53', 'dbgfm_r54',
              'dbgfm_r62', 'dbgfm_r63', 'dbgfm_r64', 'dbgfm_r65', 'dbgfm_GSAVI', 'dbgfm_GNDVI', 'dbgfm_CVI',
              'dbgfm_NDGI', 'dbgfm_RI', 'dbgfm_NBR', 'dbgfm_NDII', 'dbgfm_GDVI', 'dbgfm_MSAVI', 'dbgfm_DVI',
              'dbgfm_SAVI', 'dbgfm_NDVI', 'dbgfm_MSR', 'dbidry_psB1a', 'dbidry_psB2a', 'dbidry_psB3a', 'dbidry_psB4a',
              'dbidry_psB5a', 'dbidry_psB6a', 'dbidry_r32', 'dbidry_r42', 'dbidry_r43', 'dbidry_r52', 'dbidry_r53',
              'dbidry_r54', 'dbidry_r62', 'dbidry_r63', 'dbidry_r64', 'dbidry_r65', 'dbidry_GSAVI', 'dbidry_GNDVI',
              'dbidry_CVI', 'dbidry_NDGI', 'dbidry_RI', 'dbidry_NBR', 'dbidry_NDII', 'dbidry_GDVI', 'dbidry_MSAVI',
              'dbidry_DVI', 'dbidry_SAVI', 'dbidry_NDVI', 'dbidry_MSR', 'dbian_psB1a', 'dbian_psB2a', 'dbian_psB3a',
              'dbian_psB4a', 'dbian_psB5a', 'dbian_psB6a', 'dbian_r32', 'dbian_r42', 'dbian_r43', 'dbian_r52',
              'dbian_r53', 'dbian_r54', 'dbian_r62', 'dbian_r63', 'dbian_r64', 'dbian_r65', 'dbian_GSAVI',
              'dbian_GNDVI', 'dbian_CVI', 'dbian_NDGI', 'dbian_RI', 'dbian_NBR', 'dbian_NDII', 'dbian_GDVI',
              'dbian_MSAVI', 'dbian_DVI', 'dbian_SAVI', 'dbian_NDVI', 'dbian_MSR', 'dbifmdry_psB1a', 'dbifmdry_psB2a',
              'dbifmdry_psB3a', 'dbifmdry_psB4a', 'dbifmdry_psB5a', 'dbifmdry_psB6a', 'dbifmdry_r32', 'dbifmdry_r42',
              'dbifmdry_r43', 'dbifmdry_r52', 'dbifmdry_r53', 'dbifmdry_r54', 'dbifmdry_r62', 'dbifmdry_r63',
              'dbifmdry_r64', 'dbifmdry_r65', 'dbifmdry_GSAVI', 'dbifmdry_GNDVI', 'dbifmdry_CVI', 'dbifmdry_NDGI',
              'dbifmdry_RI', 'dbifmdry_NBR', 'dbifmdry_NDII', 'dbifmdry_GDVI', 'dbifmdry_MSAVI', 'dbifmdry_DVI',
              'dbifmdry_SAVI', 'dbifmdry_NDVI', 'dbifmdry_MSR', 'dbifman_psB1a', 'dbifman_psB2a', 'dbifman_psB3a',
              'dbifman_psB4a', 'dbifman_psB5a', 'dbifman_psB6a', 'dbifman_r32', 'dbifman_r42', 'dbifman_r43',
              'dbifman_r52', 'dbifman_r53', 'dbifman_r54', 'dbifman_r62', 'dbifman_r63', 'dbifman_r64', 'dbifman_r65',
              'dbifman_GSAVI', 'dbifman_GNDVI', 'dbifman_CVI', 'dbifman_NDGI', 'dbifman_RI', 'dbifman_NBR',
              'dbifman_NDII', 'dbifman_GDVI', 'dbifman_MSAVI', 'dbifman_DVI', 'dbifman_SAVI', 'dbifman_NDVI',
              'dbifman_MSR']

    df_all_012345_clean = df_all_012345.set_axis(column, axis=1)

    #rename_col = {}

    df_all_012345_clean.sort_values(by="uid", inplace=True)
    df_all_012345_clean.to_csv(r"C:\Users\robot\projects\biomass\collated_zonal_stats\veg_ind\df_all_012345_clean.csv", index=False)

    # ==================================================================================================================

    dp0_dbg_si = df_all_012345_clean[
        ['uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94', 'geometry', 'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
         'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha', 'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
         'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha', 'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt', 'b1_dp0_min',
         'b1_dp0_max', 'b1_dp0_mean', 'b1_dp0_std', 'b1_dp0_med', 'b1_dp0_p25',
         'b1_dp0_p50', 'b1_dp0_p75', 'b1_dp0_p95', 'b1_dp0_p99', 'b2_dp0_min', 'b2_dp0_max', 'b2_dp0_mean',
         'b2_dp0_std', 'b2_dp0_med', 'b2_dp0_p25', 'b2_dp0_p50', 'b2_dp0_p75', 'b2_dp0_p95', 'b2_dp0_p99',
         'b3_dp0_min', 'b3_dp0_max', 'b3_dp0_mean', 'b3_dp0_std', 'b3_dp0_med', 'b3_dp0_p25', 'b3_dp0_p50',
         'b3_dp0_p75', 'b3_dp0_p95', 'b3_dp0_p99',

         'b1_dbg_min', 'b1_dbg_max', 'b1_dbg_mean', 'b1_dbg_std', 'b1_dbg_med', 'b1_dbg_p25', 'b1_dbg_p50',
         'b1_dbg_p75', 'b1_dbg_p95', 'b1_dbg_p99', 'b2_dbg_min', 'b2_dbg_max', 'b2_dbg_mean', 'b2_dbg_std',
         'b2_dbg_med', 'b2_dbg_p25', 'b2_dbg_p50', 'b2_dbg_p75', 'b2_dbg_p95', 'b2_dbg_p99', 'b3_dbg_min',
         'b3_dbg_max', 'b3_dbg_mean', 'b3_dbg_std', 'b3_dbg_med', 'b3_dbg_p25', 'b3_dbg_p50', 'b3_dbg_p75',
         'b3_dbg_p95', 'b3_dbg_p99', 'b4_dbg_min', 'b4_dbg_max', 'b4_dbg_mean', 'b4_dbg_std', 'b4_dbg_med',
         'b4_dbg_p25', 'b4_dbg_p50', 'b4_dbg_p75', 'b4_dbg_p95', 'b4_dbg_p99', 'b5_dbg_min', 'b5_dbg_max',
         'b5_dbg_mean', 'b5_dbg_std', 'b5_dbg_med', 'b5_dbg_p25', 'b5_dbg_p50', 'b5_dbg_p75', 'b5_dbg_p95',
         'b5_dbg_p99', 'b6_dbg_min', 'b6_dbg_max', 'b6_dbg_mean', 'b6_dbg_std', 'b6_dbg_med', 'b6_dbg_p25',
         'b6_dbg_p50', 'b6_dbg_p75', 'b6_dbg_p95', 'b6_dbg_p99', 'dbg_psB1a', 'dbg_psB2a', 'dbg_psB3a',
         'dbg_psB4a', 'dbg_psB5a', 'dbg_psB6a', 'dbg_r32', 'dbg_r42', 'dbg_r43', 'dbg_r52', 'dbg_r53',
         'dbg_r54', 'dbg_r62', 'dbg_r63', 'dbg_r64', 'dbg_r65', 'dbg_GSAVI', 'dbg_GNDVI', 'dbg_CVI',
         'dbg_NDGI', 'dbg_RI', 'dbg_NBR', 'dbg_NDII', 'dbg_GDVI', 'dbg_MSAVI', 'dbg_DVI', 'dbg_SAVI',
         'dbg_NDVI', 'dbg_MSR']]

    dp0_dbg_si.sort_values(by="uid", inplace=True)
    dp0_dbg_si.to_csv(r"C:\Users\robot\projects\biomass\collated_zonal_stats\single\dp0_dpg_si_single.csv", index=False)

    dp0_dbg_si_mask = df_all_012345_clean[
        ['uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94', 'geometry', 'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
         'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha', 'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
         'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha', 'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt', 'b1_dp0fm_min',
         'b1_dp0fm_max', 'b1_dp0fm_mean', 'b1_dp0fm_std', 'b1_dp0fm_med',
         'b1_dp0fm_p25', 'b1_dp0fm_p50', 'b1_dp0fm_p75', 'b1_dp0fm_p95', 'b1_dp0fm_p99', 'b2_dp0fm_min',
         'b2_dp0fm_max', 'b2_dp0fm_mean', 'b2_dp0fm_std', 'b2_dp0fm_med', 'b2_dp0fm_p25', 'b2_dp0fm_p50',
         'b2_dp0fm_p75', 'b2_dp0fm_p95', 'b2_dp0fm_p99', 'b3_dp0fm_min', 'b3_dp0fm_max', 'b3_dp0fm_mean',
         'b3_dp0fm_std', 'b3_dp0fm_med', 'b3_dp0fm_p25', 'b3_dp0fm_p50', 'b3_dp0fm_p75', 'b3_dp0fm_p95',
         'b3_dp0fm_p99',

         'b1_dbgfm_min', 'b1_dbgfm_max', 'b1_dbgfm_mean', 'b1_dbgfm_std', 'b1_dbgfm_med', 'b1_dbgfm_p25',
         'b1_dbgfm_p50', 'b1_dbgfm_p75', 'b1_dbgfm_p95', 'b1_dbgfm_p99', 'b2_dbgfm_min', 'b2_dbgfm_max',
         'b2_dbgfm_mean', 'b2_dbgfm_std', 'b2_dbgfm_med', 'b2_dbgfm_p25', 'b2_dbgfm_p50', 'b2_dbgfm_p75',
         'b2_dbgfm_p95', 'b2_dbgfm_p99', 'b3_dbgfm_min', 'b3_dbgfm_max', 'b3_dbgfm_mean', 'b3_dbgfm_std',
         'b3_dbgfm_med', 'b3_dbgfm_p25', 'b3_dbgfm_p50', 'b3_dbgfm_p75', 'b3_dbgfm_p95', 'b3_dbgfm_p99',
         'b4_dbgfm_min', 'b4_dbgfm_max', 'b4_dbgfm_mean', 'b4_dbgfm_std', 'b4_dbgfm_med', 'b4_dbgfm_p25',
         'b4_dbgfm_p50', 'b4_dbgfm_p75', 'b4_dbgfm_p95', 'b4_dbgfm_p99', 'b5_dbgfm_min', 'b5_dbgfm_max',
         'b5_dbgfm_mean', 'b5_dbgfm_std', 'b5_dbgfm_med', 'b5_dbgfm_p25', 'b5_dbgfm_p50', 'b5_dbgfm_p75',
         'b5_dbgfm_p95', 'b5_dbgfm_p99', 'b6_dbgfm_min', 'b6_dbgfm_max', 'b6_dbgfm_mean', 'b6_dbgfm_std',
         'b6_dbgfm_med', 'b6_dbgfm_p25', 'b6_dbgfm_p50', 'b6_dbgfm_p75', 'b6_dbgfm_p95', 'b6_dbgfm_p99',
         'dbgfm_psB1a', 'dbgfm_psB2a', 'dbgfm_psB3a', 'dbgfm_psB4a', 'dbgfm_psB5a',
         'dbgfm_psB6a', 'dbgfm_r32', 'dbgfm_r42', 'dbgfm_r43', 'dbgfm_r52', 'dbgfm_r53', 'dbgfm_r54',
         'dbgfm_r62', 'dbgfm_r63', 'dbgfm_r64', 'dbgfm_r65', 'dbgfm_GSAVI', 'dbgfm_GNDVI', 'dbgfm_CVI',
         'dbgfm_NDGI', 'dbgfm_RI', 'dbgfm_NBR', 'dbgfm_NDII', 'dbgfm_GDVI', 'dbgfm_MSAVI', 'dbgfm_DVI',
         'dbgfm_SAVI', 'dbgfm_NDVI', 'dbgfm_MSR']]

    dp0_dbg_si_mask.sort_values(by="uid", inplace=True)
    dp0_dbg_si_mask_rename = dp0_dbg_si_mask.copy()
    # Rename columns by dropping "fm" from the names
    dp0_dbg_si_mask_rename.columns = dp0_dbg_si_mask_rename.columns.str.replace('fm', '', regex=False)

    dp0_dbg_si_mask_rename.to_csv(r"C:\Users\robot\projects\biomass\collated_zonal_stats\dp0_dpg_si_mask_single.csv", index=False)

    dp1_dbi_si_dry = df_all_012345_clean[
        ['uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94', 'geometry', 'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
         'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha', 'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
         'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha', 'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt',

         'b1_dp1_dry_min', 'b1_dp1_dry_max', 'b1_dp1_dry_mean', 'b1_dp1_dry_std',
         'b1_dp1_dry_med', 'b1_dp1_dry_p25', 'b1_dp1_dry_p50', 'b1_dp1_dry_p75', 'b1_dp1_dry_p95',
         'b1_dp1_dry_p99', 'b2_dp1_dry_min', 'b2_dp1_dry_max', 'b2_dp1_dry_mean', 'b2_dp1_dry_std',
         'b2_dp1_dry_med', 'b2_dp1_dry_p25', 'b2_dp1_dry_p50', 'b2_dp1_dry_p75', 'b2_dp1_dry_p95',
         'b2_dp1_dry_p99', 'b3_dp1_dry_min', 'b3_dp1_dry_max', 'b3_dp1_dry_mean', 'b3_dp1_dry_std',
         'b3_dp1_dry_med', 'b3_dp1_dry_p25', 'b3_dp1_dry_p50', 'b3_dp1_dry_p75', 'b3_dp1_dry_p95',
         'b3_dp1_dry_p99',

         'b1_dbi_dry_min', 'b1_dbi_dry_max', 'b1_dbi_dry_mean', 'b1_dbi_dry_std', 'b1_dbi_dry_med',
         'b1_dbi_dry_p25', 'b1_dbi_dry_p50', 'b1_dbi_dry_p75', 'b1_dbi_dry_p95', 'b1_dbi_dry_p99',
         'b2_dbi_dry_min', 'b2_dbi_dry_max', 'b2_dbi_dry_mean', 'b2_dbi_dry_std', 'b2_dbi_dry_med',
         'b2_dbi_dry_p25', 'b2_dbi_dry_p50', 'b2_dbi_dry_p75', 'b2_dbi_dry_p95', 'b2_dbi_dry_p99',
         'b3_dbi_dry_min', 'b3_dbi_dry_max', 'b3_dbi_dry_mean', 'b3_dbi_dry_std', 'b3_dbi_dry_med',
         'b3_dbi_dry_p25', 'b3_dbi_dry_p50', 'b3_dbi_dry_p75', 'b3_dbi_dry_p95', 'b3_dbi_dry_p99',
         'b4_dbi_dry_min', 'b4_dbi_dry_max', 'b4_dbi_dry_mean', 'b4_dbi_dry_std', 'b4_dbi_dry_med',
         'b4_dbi_dry_p25', 'b4_dbi_dry_p50', 'b4_dbi_dry_p75', 'b4_dbi_dry_p95', 'b4_dbi_dry_p99',
         'b5_dbi_dry_min', 'b5_dbi_dry_max', 'b5_dbi_dry_mean', 'b5_dbi_dry_std', 'b5_dbi_dry_med',
         'b5_dbi_dry_p25', 'b5_dbi_dry_p50', 'b5_dbi_dry_p75', 'b5_dbi_dry_p95', 'b5_dbi_dry_p99',
         'b6_dbi_dry_min', 'b6_dbi_dry_max', 'b6_dbi_dry_mean', 'b6_dbi_dry_std', 'b6_dbi_dry_med',
         'b6_dbi_dry_p25', 'b6_dbi_dry_p50', 'b6_dbi_dry_p75', 'b6_dbi_dry_p95', 'b6_dbi_dry_p99',
         'dbidry_psB1a', 'dbidry_psB2a', 'dbidry_psB3a', 'dbidry_psB4a',
         'dbidry_psB5a', 'dbidry_psB6a', 'dbidry_r32', 'dbidry_r42', 'dbidry_r43', 'dbidry_r52', 'dbidry_r53',
         'dbidry_r54', 'dbidry_r62', 'dbidry_r63', 'dbidry_r64', 'dbidry_r65', 'dbidry_GSAVI', 'dbidry_GNDVI',
         'dbidry_CVI', 'dbidry_NDGI', 'dbidry_RI', 'dbidry_NBR', 'dbidry_NDII', 'dbidry_GDVI', 'dbidry_MSAVI',
         'dbidry_DVI', 'dbidry_SAVI', 'dbidry_NDVI', 'dbidry_MSR']]

    dp1_dbi_si_dry.sort_values(by="uid", inplace=True)

    dp1_dbi_si_dry_rename = dp1_dbi_si_dry.copy()
    # Rename columns by dropping "fm" from the names
    dp1_dbi_si_dry_rename.columns = dp1_dbi_si_dry_rename.columns.str.replace('_dry', '', regex=False)
    dp1_dbi_si_dry_rename.to_csv(r"C:\Users\robot\projects\biomass\collated_zonal_stats\dry\dp1_dbi_si_dry.csv", index=False)


    dp1_dbi_si_annual = df_all_012345_clean[
        ['uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94', 'geometry', 'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
         'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha', 'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
         'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha', 'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt',
         'b1_dp1_min', 'b1_dp1_max', 'b1_dp1_mean', 'b1_dp1_std', 'b1_dp1_med', 'b1_dp1_p25', 'b1_dp1_p50',
         'b1_dp1_p75', 'b1_dp1_p95', 'b1_dp1_p99', 'b2_dp1_min', 'b2_dp1_max', 'b2_dp1_mean', 'b2_dp1_std',
         'b2_dp1_med', 'b2_dp1_p25', 'b2_dp1_p50', 'b2_dp1_p75', 'b2_dp1_p95', 'b2_dp1_p99', 'b3_dp1_min',
         'b3_dp1_max', 'b3_dp1_mean', 'b3_dp1_std', 'b3_dp1_med', 'b3_dp1_p25', 'b3_dp1_p50', 'b3_dp1_p75',
         'b3_dp1_p95', 'b3_dp1_p99',
         'b1_dbi_min', 'b1_dbi_max', 'b1_dbi_mean', 'b1_dbi_std',
         'b1_dbi_med', 'b1_dbi_p25', 'b1_dbi_p50', 'b1_dbi_p75', 'b1_dbi_p95', 'b1_dbi_p99', 'b2_dbi_min',
         'b2_dbi_max', 'b2_dbi_mean', 'b2_dbi_std', 'b2_dbi_med', 'b2_dbi_p25', 'b2_dbi_p50', 'b2_dbi_p75',
         'b2_dbi_p95', 'b2_dbi_p99', 'b3_dbi_min', 'b3_dbi_max', 'b3_dbi_mean', 'b3_dbi_std', 'b3_dbi_med',
         'b3_dbi_p25', 'b3_dbi_p50', 'b3_dbi_p75', 'b3_dbi_p95', 'b3_dbi_p99', 'b4_dbi_min', 'b4_dbi_max',
         'b4_dbi_mean', 'b4_dbi_std', 'b4_dbi_med', 'b4_dbi_p25', 'b4_dbi_p50', 'b4_dbi_p75', 'b4_dbi_p95',
         'b4_dbi_p99', 'b5_dbi_min', 'b5_dbi_max', 'b5_dbi_mean', 'b5_dbi_std', 'b5_dbi_med', 'b5_dbi_p25',
         'b5_dbi_p50', 'b5_dbi_p75', 'b5_dbi_p95', 'b5_dbi_p99', 'b6_dbi_min', 'b6_dbi_max', 'b6_dbi_mean',
         'b6_dbi_std', 'b6_dbi_med', 'b6_dbi_p25', 'b6_dbi_p50', 'b6_dbi_p75', 'b6_dbi_p95', 'b6_dbi_p99',
         'dbian_psB1a', 'dbian_psB2a', 'dbian_psB3a',
         'dbian_psB4a', 'dbian_psB5a', 'dbian_psB6a', 'dbian_r32', 'dbian_r42', 'dbian_r43', 'dbian_r52',
         'dbian_r53', 'dbian_r54', 'dbian_r62', 'dbian_r63', 'dbian_r64', 'dbian_r65', 'dbian_GSAVI',
         'dbian_GNDVI', 'dbian_CVI', 'dbian_NDGI', 'dbian_RI', 'dbian_NBR', 'dbian_NDII', 'dbian_GDVI',
         'dbian_MSAVI', 'dbian_DVI', 'dbian_SAVI', 'dbian_NDVI', 'dbian_MSR']]

    dp1_dbi_si_annual.sort_values(by="uid", inplace=True)
    dp1_dbi_si_annual.to_csv(r"C:\Users\robot\projects\biomass\collated_zonal_stats\annual\dp1_dbi_si_annual.csv", index=False)

    dp1_dbi_si_mask_dry = df_all_012345_clean[
        ['uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94', 'geometry', 'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
         'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha', 'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
         'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha', 'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt',
         # 'b1_dbifm_dry_min', 'b1_dbifm_dry_max', 'b1_dbifm_dry_mean', 'b1_dbifm_dry_std', 'b1_dbifm_dry_med',
         # 'b1_dbifm_dry_p25', 'b1_dbifm_dry_p50', 'b1_dbifm_dry_p75', 'b1_dbifm_dry_p95', 'b1_dbifm_dry_p99',

         'b1_dp1fm_dry_min', 'b1_dp1fm_dry_max', 'b1_dp1fm_dry_mean', 'b1_dp1fm_dry_std',
         'b1_dp1fm_dry_med',
         'b1_dp1fm_dry_p25', 'b1_dp1fm_dry_p50', 'b1_dp1fm_dry_p75', 'b1_dp1fm_dry_p95',
         'b1_dp1fm_dry_p99',
         'b2_dp1fm_dry_min', 'b2_dp1fm_dry_max', 'b2_dp1fm_dry_mean', 'b2_dp1fm_dry_std',
         'b2_dp1fm_dry_med',
         'b2_dp1fm_dry_p25', 'b2_dp1fm_dry_p50', 'b2_dp1fm_dry_p75', 'b2_dp1fm_dry_p95',
         'b2_dp1fm_dry_p99',
         'b3_dp1fm_dry_min', 'b3_dp1fm_dry_max', 'b3_dp1fm_dry_mean', 'b3_dp1fm_dry_std',
         'b3_dp1fm_dry_med',
         'b3_dp1fm_dry_p25', 'b3_dp1fm_dry_p50', 'b3_dp1fm_dry_p75', 'b3_dp1fm_dry_p95',
         'b3_dp1fm_dry_p99',
         'b1_dbifm_dry_std', 'b1_dbifm_dry_med', 'b1_dbifm_dry_p25', 'b1_dbifm_dry_p50', 'b1_dbifm_dry_p75',
         'b1_dbifm_dry_p95', 'b1_dbifm_dry_p99', 'b2_dbifm_dry_min', 'b2_dbifm_dry_max', 'b2_dbifm_dry_mean',
         'b2_dbifm_dry_std', 'b2_dbifm_dry_med', 'b2_dbifm_dry_p25', 'b2_dbifm_dry_p50', 'b2_dbifm_dry_p75',
         'b2_dbifm_dry_p95', 'b2_dbifm_dry_p99', 'b3_dbifm_dry_min', 'b3_dbifm_dry_max', 'b3_dbifm_dry_mean',
         'b3_dbifm_dry_std', 'b3_dbifm_dry_med', 'b3_dbifm_dry_p25', 'b3_dbifm_dry_p50', 'b3_dbifm_dry_p75',
         'b3_dbifm_dry_p95', 'b3_dbifm_dry_p99', 'b4_dbifm_dry_min', 'b4_dbifm_dry_max', 'b4_dbifm_dry_mean',
         'b4_dbifm_dry_std', 'b4_dbifm_dry_med', 'b4_dbifm_dry_p25', 'b4_dbifm_dry_p50', 'b4_dbifm_dry_p75',
         'b4_dbifm_dry_p95', 'b4_dbifm_dry_p99', 'b5_dbifm_dry_min', 'b5_dbifm_dry_max', 'b5_dbifm_dry_mean',
         'b5_dbifm_dry_std', 'b5_dbifm_dry_med', 'b5_dbifm_dry_p25', 'b5_dbifm_dry_p50', 'b5_dbifm_dry_p75',
         'b5_dbifm_dry_p95', 'b5_dbifm_dry_p99', 'b6_dbifm_dry_min', 'b6_dbifm_dry_max', 'b6_dbifm_dry_mean',
         'b6_dbifm_dry_std', 'b6_dbifm_dry_med', 'b6_dbifm_dry_p25', 'b6_dbifm_dry_p50', 'b6_dbifm_dry_p75',
         'b6_dbifm_dry_p95', 'b6_dbifm_dry_p99', 'dbifmdry_psB1a', 'dbifmdry_psB2a',
         'dbifmdry_psB3a', 'dbifmdry_psB4a', 'dbifmdry_psB5a', 'dbifmdry_psB6a', 'dbifmdry_r32', 'dbifmdry_r42',
         'dbifmdry_r43', 'dbifmdry_r52', 'dbifmdry_r53', 'dbifmdry_r54', 'dbifmdry_r62', 'dbifmdry_r63',
         'dbifmdry_r64', 'dbifmdry_r65', 'dbifmdry_GSAVI', 'dbifmdry_GNDVI', 'dbifmdry_CVI', 'dbifmdry_NDGI',
         'dbifmdry_RI', 'dbifmdry_NBR', 'dbifmdry_NDII', 'dbifmdry_GDVI', 'dbifmdry_MSAVI', 'dbifmdry_DVI',
         'dbifmdry_SAVI', 'dbifmdry_NDVI', 'dbifmdry_MSR']]

    dp1_dbi_si_mask_dry.sort_values(by="uid", inplace=True)
    dp1_dbi_si_mask_dry_rename = dp1_dbi_si_mask_dry.copy()
    # Rename columns by dropping "fm" from the names
    dp1_dbi_si_mask_dry_rename.columns = dp1_dbi_si_mask_dry_rename.columns.str.replace('fm_dry', '', regex=False)
    dp1_dbi_si_mask_dry_rename.columns = dp1_dbi_si_mask_dry_rename.columns.str.replace('fmdry', '', regex=False)
    dp1_dbi_si_mask_dry_rename.to_csv(r"C:\Users\robot\projects\biomass\collated_zonal_stats\dry_mask\dp1_dbi_si_mask_dry.csv", index=False)


    #todo up to here changing names
    dp1_dbi_si_mask_annual = df_all_012345_clean[
        ['uid', 'site_clean', 'date', 'lon_gda94', 'lat_gda94', 'geometry', 'bio_l_kg1ha', 'bio_t_kg1ha', 'bio_b_kg1ha',
         'bio_w_kg1ha', 'bio_br_kg1ha', 'bio_s_kg1ha', 'bio_r_kg1ha', 'bio_agb_kg1ha', 'c_l_kg1ha', 'c_t_kg1ha',
         'c_b_kg1ha', 'c_w_kg1ha', 'c_br_kg1ha', 'c_s_kg1ha', 'c_r_kg1ha', 'c_agb_kg1ha', 'basal_dt', 'b1_dp1fm_min',
         'b1_dp1fm_max', 'b1_dp1fm_mean', 'b1_dp1fm_std', 'b1_dp1fm_med', 'b1_dp1fm_p25', 'b1_dp1fm_p50',
         'b1_dp1fm_p75',
         'b1_dp1fm_p95', 'b1_dp1fm_p99', 'b2_dp1fm_min', 'b2_dp1fm_max', 'b2_dp1fm_mean', 'b2_dp1fm_std',
         'b2_dp1fm_med',
         'b2_dp1fm_p25', 'b2_dp1fm_p50', 'b2_dp1fm_p75', 'b2_dp1fm_p95', 'b2_dp1fm_p99', 'b3_dp1fm_min', 'b3_dp1fm_max',
         'b3_dp1fm_mean', 'b3_dp1fm_std', 'b3_dp1fm_med', 'b3_dp1fm_p25', 'b3_dp1fm_p50', 'b3_dp1fm_p75',
         'b3_dp1fm_p95',
         'b3_dp1fm_p99',
         'b1_dbifm_min', 'b1_dbifm_max', 'b1_dbifm_mean', 'b1_dbifm_std',
         'b1_dbifm_med', 'b1_dbifm_p25', 'b1_dbifm_p50', 'b1_dbifm_p75', 'b1_dbifm_p95', 'b1_dbifm_p99',
         'b2_dbifm_min', 'b2_dbifm_max', 'b2_dbifm_mean', 'b2_dbifm_std', 'b2_dbifm_med', 'b2_dbifm_p25',
         'b2_dbifm_p50', 'b2_dbifm_p75', 'b2_dbifm_p95', 'b2_dbifm_p99', 'b3_dbifm_min', 'b3_dbifm_max',
         'b3_dbifm_mean', 'b3_dbifm_std', 'b3_dbifm_med', 'b3_dbifm_p25', 'b3_dbifm_p50', 'b3_dbifm_p75',
         'b3_dbifm_p95', 'b3_dbifm_p99', 'b4_dbifm_min', 'b4_dbifm_max', 'b4_dbifm_mean', 'b4_dbifm_std',
         'b4_dbifm_med', 'b4_dbifm_p25', 'b4_dbifm_p50', 'b4_dbifm_p75', 'b4_dbifm_p95', 'b4_dbifm_p99',
         'b5_dbifm_min', 'b5_dbifm_max', 'b5_dbifm_mean', 'b5_dbifm_std', 'b5_dbifm_med', 'b5_dbifm_p25',
         'b5_dbifm_p50', 'b5_dbifm_p75', 'b5_dbifm_p95', 'b5_dbifm_p99', 'b6_dbifm_min', 'b6_dbifm_max',
         'b6_dbifm_mean', 'b6_dbifm_std', 'b6_dbifm_med', 'b6_dbifm_p25', 'b6_dbifm_p50', 'b6_dbifm_p75',
         'b6_dbifm_p95', 'b6_dbifm_p99', 'dbifman_psB1a', 'dbifman_psB2a', 'dbifman_psB3a',
         'dbifman_psB4a', 'dbifman_psB5a', 'dbifman_psB6a', 'dbifman_r32', 'dbifman_r42', 'dbifman_r43',
         'dbifman_r52', 'dbifman_r53', 'dbifman_r54', 'dbifman_r62', 'dbifman_r63', 'dbifman_r64', 'dbifman_r65',
         'dbifman_GSAVI', 'dbifman_GNDVI', 'dbifman_CVI', 'dbifman_NDGI', 'dbifman_RI', 'dbifman_NBR',
         'dbifman_NDII', 'dbifman_GDVI', 'dbifman_MSAVI', 'dbifman_DVI', 'dbifman_SAVI', 'dbifman_NDVI',
         'dbifman_MSR']]

    dp1_dbi_si_mask_annual.sort_values(by="uid", inplace=True)
    dp1_dbi_si_mask_annual_rename = dp1_dbi_si_mask_annual.copy()
    # Rename columns by dropping "fm" from the names
    dp1_dbi_si_mask_annual_rename.columns = dp1_dbi_si_mask_annual_rename.columns.str.replace('fm', '', regex=False)

    dp1_dbi_si_mask_annual_rename.to_csv(r"C:\Users\robot\projects\biomass\collated_zonal_stats\annual_mask\dp1_dbi_si_mask_annual.csv", index=False)


    return df_all_012345, df_all_012345_clean, dp0_dbg_si, dp0_dbg_si_mask, dp1_dbi_si_dry, \
        dp1_dbi_si_mask_dry, dp1_dbi_si_annual, dp1_dbi_si_mask_annual


    if __name__ == '__main__':
        main_routine()
