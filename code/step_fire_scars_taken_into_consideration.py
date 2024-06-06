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



def main_routine():
    # Dictionary identifies the data structure of the reference image

    for df1, title in zip([h99a_0112_basal_y],
                          ["h99a_0112_basal_y"]):
        df1.dropna(inplace=True)
        df = df1[df1['bio_agb_kg1ha'] != 0]
        value_y = 'bio_agb_kg1ha'
        value_x = 'b1_h99a2_mean'
        sns.regplot(x=value_x, y=value_y, data=df)
        slope, intercept, r_value, p_value, std_err = scipy.stats.linregress(df[value_x], df[value_y])

        print("Comparison relationship: ", title)
        print("*" * 50)
        print("slope: ", slope)
        print("intersept: ", intercept)
        print("r2: ", r_value)
        print("P_value: ", p_value)
        print("std error: ", std_err)
        plt.show()
        print('-' * 50)

        # correlation matrix
        band = 1
        col_names = df.columns.tolist()
        res = [i for i in col_names if f"b{band}" in i]
        res.append('bio_agb_kg1ha')
        # print(res)
        df1 = df[res]
        # print(df1)

        # correlation matrix
        col_names = df.columns.tolist()
        res = [i for i in col_names if f"b{band}" in i]
        res_ = res[:6]
        res_.append('bio_agb_kg1ha')
        df1 = df[res_]
        # plotting correlation heatmap
        dataplot = sns.heatmap(df1.corr(), cmap="YlGnBu", annot=True)
        plt.show()

        print('-' * 50)
        res_ = res[6:]
        res_.append('bio_agb_kg1ha')
        df1 = df[res_]
        # plotting correlation heatmap
        dataplot = sns.heatmap(df1.corr(), cmap="YlGnBu", annot=True)
        plt.show()
        print('-' * 50)

        # --------------------------------------------------------------------

        for df1, title in zip([fpca_0509_basal_y],
                              ["fpca_0509_basal_y"]):
            df1.dropna(inplace=True)
            df = df1[df1['bio_agb_kg1ha'] != 0]
            value_y = 'bio_agb_kg1ha'
            value_x = 'b1_fpca2_mean'
            sns.regplot(x=value_x, y=value_y, data=df)
            slope, intercept, r_value, p_value, std_err = scipy.stats.linregress(df[value_x], df[value_y])

            print("Comparison relationship: ", title)
            print("*" * 50)
            print("slope: ", slope)
            print("intersept: ", intercept)
            print("r2: ", r_value)
            print("P_value: ", p_value)
            print("std error: ", std_err)
            plt.show()
            print('-' * 50)

            # correlation matrix
            band = 1
            col_names = df.columns.tolist()
            res = [i for i in col_names if f"b{band}" in i]
            res.append('bio_agb_kg1ha')
            # print(res)
            df1 = df[res]
            # print(df1)

            # correlation matrix
            col_names = df.columns.tolist()
            res = [i for i in col_names if f"b{band}" in i]
            res_ = res[:6]
            res_.append('bio_agb_kg1ha')
            df1 = df[res_]
            # plotting correlation heatmap
            dataplot = sns.heatmap(df1.corr(), cmap="YlGnBu", annot=True)
            plt.show()

            print('-' * 50)
            res_ = res[6:]
            res_.append('bio_agb_kg1ha')
            df1 = df[res_]
            # plotting correlation heatmap
            dataplot = sns.heatmap(df1.corr(), cmap="YlGnBu", annot=True)
            plt.show()
            print('-' * 50)

    # --------------------------------------------------------------------

    for df1, title in zip([fpca_0509_basal_y],
                          ["fpca_0509_basal_y"]):
        df1.dropna(inplace=True)
        df = df1[df1['bio_agb_kg1ha'] != 0]
        value_y = 'bio_agb_kg1ha'
        value_x = 'b1_fpca2_mean'
        sns.regplot(x=value_x, y=value_y, data=df)
        slope, intercept, r_value, p_value, std_err = scipy.stats.linregress(df[value_x], df[value_y])

        print("Comparison relationship: ", title)
        print("*" * 50)
        print("slope: ", slope)
        print("intersept: ", intercept)
        print("r2: ", r_value)
        print("P_value: ", p_value)
        print("std error: ", std_err)
        plt.show()
        print('-' * 50)

        # correlation matrix
        band = 1
        col_names = df.columns.tolist()
        res = [i for i in col_names if f"b{band}" in i]
        res.append('bio_agb_kg1ha')
        # print(res)
        df1 = df[res]
        # print(df1)

        # correlation matrix
        col_names = df.columns.tolist()
        res = [i for i in col_names if f"b{band}" in i]
        res_ = res[:6]
        res_.append('bio_agb_kg1ha')
        df1 = df[res_]
        # plotting correlation heatmap
        dataplot = sns.heatmap(df1.corr(), cmap="YlGnBu", annot=True)
        plt.show()

        print('-' * 50)
        res_ = res[6:]
        res_.append('bio_agb_kg1ha')
        df1 = df[res_]
        # plotting correlation heatmap
        dataplot = sns.heatmap(df1.corr(), cmap="YlGnBu", annot=True)
        plt.show()
        print('-' * 50)


    # ------------------------------------------------------------

    for df1, title in zip([dja_0305_basal_y, dja_0608_basal_y, dja_0911_basal_y, dja_1202_basal_y],
                          ["dja_0305_basal_y", "dja_0608_basal_y", "dja_0911_basal_y", "dja_1202_basal_y"]):
        df = df1[df1['bio_agb_kg1ha'] != 0]

        value_y = 'bio_agb_kg1ha'
        value_x = 'b1_dja_mean'
        sns.regplot(x=value_x, y=value_y, data=df)
        slope, intercept, r_value, p_value, std_err = scipy.stats.linregress(df[value_x], df[value_y])

        print("Comparison relationship: ", title)
        print("*" * 50)
        print("slope: ", slope)
        print("intersept: ", intercept)
        print("r2: ", r_value)
        print("P_value: ", p_value)
        print("std error: ", std_err)
        plt.show()
        print('-' * 50)

        # correlation matrix
        band = 1
        col_names = df.columns.tolist()
        res = [i for i in col_names if f"b{band}" in i]
        res.append('bio_agb_kg1ha')
        # print(res)
        df1 = df[res]
        # print(df1)

        # correlation matrix
        col_names = df.columns.tolist()
        res = [i for i in col_names if f"b{band}" in i]
        res_ = res[:6]
        res_.append('bio_agb_kg1ha')
        df1 = df[res_]
        # plotting correlation heatmap
        dataplot = sns.heatmap(df1.corr(), cmap="YlGnBu", annot=True)
        plt.show()

        print('-' * 50)
        res_ = res[6:]
        res_.append('bio_agb_kg1ha')
        df1 = df[res_]
        # plotting correlation heatmap
        dataplot = sns.heatmap(df1.corr(), cmap="YlGnBu", annot=True)
        plt.show()
        print('-' * 50)

    # --------------------------------------------------------

    for band_ in range(0, 3):
        print("=" * 100)
        band = band_ + 1
        print(f"BAND: {band}")

        for df1, title in zip([dp1_0112_basal_y, dp1_0509_basal_y],
                              ["dp1_0112_basal_y", "dp1_0509_basal_y"]):
            df1.dropna(inplace=True)
            df = df1[df1['bio_agb_kg1ha'] != 0]
            value_y = 'bio_agb_kg1ha'
            value_x = f'b{band}_dp1_mean'
            sns.regplot(x=value_x, y=value_y, data=df)
            slope, intercept, r_value, p_value, std_err = scipy.stats.linregress(df[value_x], df[value_y])

            print(f"Comparison relationship: {title} title, band: {band}")
            print("*" * 50)
            print("slope: ", slope)
            print("intersept: ", intercept)
            print("r2: ", r_value)
            print("P_value: ", p_value)
            print("std error: ", std_err)
            plt.show()
            print('-' * 50)

            # correlation matrix
            col_names = df.columns.tolist()
            res = [i for i in col_names if f"b{band}" in i]
            res_ = res[:6]
            res_.append('bio_agb_kg1ha')
            df1 = df[res_]
            # plotting correlation heatmap
            dataplot = sns.heatmap(df1.corr(), cmap="YlGnBu", annot=True)
            plt.show()

            print('-' * 50)
            res_ = res[6:]
            res_.append('bio_agb_kg1ha')
            df1 = df[res_]
            # plotting correlation heatmap
            dataplot = sns.heatmap(df1.corr(), cmap="YlGnBu", annot=True)
            plt.show()
            print('-' * 50)

if __name__ == '__main__':
    main_routine()
