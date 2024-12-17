#!/usr/bin/env python

"""
Fractional cover zonal statistics pipeline
==========================================

Description:


step1_initiate_biomass_zonal_stats_collation_pipeline.py
===============================
Description: This script initiates the Fractional cover zonal statistics pipeline.
This script:

1. Imports and passes the command line arguments.

2. Creates two directories named: user_YYYYMMDD_HHMM. If either of the directories exist, they WILL BE DELETED.

3. Controls the workflow of the pipeline.

4. Deletes the temporary directory and its contents once the pipeline has completed.


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


--biomass_csv: str
string object containing the path to the biomass csv.

--tile_dir: str
string object containing the path to the biomass zonal statistics directory, one subdir per tile.
default="U:\\biomass\\raw_zonal_stats\\tile"

--mosaic_dir: str
string object containing the path to the seasonal mosaic zonal statistics directory.
default = "U:\\biomass\\raw_zonal_stats\\mosaic\\rmcgr_nt_mosaic_20230812_1807"

--output_dir: str
string object containing the path to the directory for the outputs.
default= "U:\\scratch\\rob\\pipelines\\outputs")


======================================================================================================

"""

# Import modules
from __future__ import print_function, division

import argparse
import shutil
import sys
import warnings
import os
from datetime import datetime
import pandas as pd

warnings.filterwarnings("ignore")

#
# def get_cmd_args_fn():
#     p = argparse.ArgumentParser(
#         description='''Input a single or multi-band raster to extracts the values from the input shapefile. ''')
#
#     p.add_argument('-b', '--biomass_csv', help='Biomass csv filepath.')
#
#     p.add_argument('-t', '--tile_dir',
#                    help="Path to the tile zonal stats directory.",
#                    default=r"C:\Users\robot\projects\biomass\zonal_stats_raw_biolib\dbi")
#
#     p.add_argument('-fd', '--fire_dir',
#                    help="Path to the mosaic fire scar zonal stats directory.",
#                    #default=r"C:\Users\robot\projects\outputs\fire_zonal\robot_met_ver_zonal_20240723_1008",
#                    default=r"C:\Users\robot\projects\biomass\zonal_stats_raw_biolib\fire")
#
#     p.add_argument('-m', '--met_dir',
#                    help="Path to the meteorological data zonal stats directory.",
#                    default=r"C:\Users\robot\projects\biomass\zonal_stats_raw_biolib\met_clean")
#
#     # p.add_argument('-ms', '--met_si_dir',
#     #                help="Path to the meteorological data zonal stats directory.",
#     #                default=r"C:\Users\robot\projects\biomass\zonal_stats_raw_biolib\met_clean")
#
#     p.add_argument('-d', '--density_dir',
#                    help="Path to the tile height and density zonal stats directory.",
#                    default=r"C:\Users\robot\projects\biomass\zonal_stats_raw_biolib\density")
#
#     p.add_argument('-o', '--output_dir',
#                    help="Pipeline export directory.", default=r"C:\Users\robot\projects\outputs\biomass_collation_biolib")
#     cmd_args = p.parse_args()
#
#     if cmd_args.biomass_csv is None:
#         p.print_help()
#
#         sys.exit()
#
#     return cmd_args

def get_cmd_args_fn():
    p = argparse.ArgumentParser(
        description='''Input a single or multi-band raster to extracts the values from the input shapefile. ''')

    p.add_argument('-b', '--biomass_csv', help='Biomass csv filepath.')

    p.add_argument('-t', '--tile_dir',
                   help="Path to the tile zonal stats directory.",
                   #default=r"C:\Users\robot\projects\biomass\zonal_stats_raw_biolib\dbi",
                   default=r"C:\Users\robot\projects\biomass\zonal_stats_raw\dbi")

    p.add_argument('-fd', '--fire_dir',
                   help="Path to the mosaic fire scar zonal stats directory.",
                   #default=r"C:\Users\robot\projects\outputs\fire_zonal\robot_met_ver_zonal_20240723_1008",
                   #default=r"C:\Users\robot\projects\outputs\fire_zonal\robot_met_ver_zonal_20240801_0927_nafi_only",
                   default=r"C:\Users\robot\projects\biomass\zonal_stats_raw\fire_nafi_2000_2024",
                   #default=r"C:\Users\robot\projects\outputs\fire_zonal",
                   )

    p.add_argument('-m', '--met_dir',
                   help="Path to the meteorological data zonal stats directory.",
                   #default=r"C:\Users\robot\projects\biomass\zonal_stats_raw_biolib\met_clean",
                   #default = r"C:\Users\robot\projects\biomass\zonal_stats_raw\met_clean_1988_2024",
                   #default = r"C:\Users\robot\projects\biomass\zonal_stats_raw\met_clean",
                   default = r"F:\silo\outputs")

    # p.add_argument('-ms', '--met_si_dir',
    #                help="Path to the meteorological data zonal stats directory.",
    #                default=r"C:\Users\robot\projects\biomass\zonal_stats_raw_biolib\met_clean")

    p.add_argument('-d', '--density_dir',
                   help="Path to the tile height and density zonal stats directory.",
                   default=r"C:\Users\robot\projects\biomass\zonal_stats_raw_biolib\density",
                   #default=r"C:\Users\robot\projects\biomass\zonal_stats_raw\density",
                   )

    p.add_argument('-o', '--output_dir',
                   help="Pipeline export directory.",
                   #default=r"C:\Users\robot\projects\outputs\biomass_collation_biolib",
                   default=r"C:\Users\robot\projects\outputs\biomass_collation")

    cmd_args = p.parse_args()

    if cmd_args.biomass_csv is None:
        p.print_help()

        sys.exit()

    return cmd_args

def temporary_dir_fn():
    """ Create a temporary directory 'user_YYYMMDD_HHMM'.

    @return temp_dir_path: string object containing the newly created directory path.
    @return final_user: string object containing the user id or the operator.
    """

    # extract user name
    home_dir = os.path.expanduser("~")
    _, user = home_dir.rsplit('\\', 1)
    final_user = user[3:]

    # create file name based on date and time.
    date_time_replace = str(datetime.now()).replace('-', '')
    date_time_list = date_time_replace.split(' ')
    date_time_list_split = date_time_list[1].split(':')
    temp_dir_path = '\\' + str(final_user) + '_' + str(date_time_list[0]) + '_' + str(
        date_time_list_split[0]) + str(date_time_list_split[1])

    try:
        shutil.rmtree(temp_dir_path)

    except:
        print('The following temporary directory will be created: ', temp_dir_path)
        pass
    # create folder a temporary folder titled (titled 'tempFolder'
    os.makedirs(temp_dir_path)

    return temp_dir_path, final_user


def export_file_path_fn(output_dir, final_user):
    """ Create an export directory 'user_YYYMMDD_HHMM' at the location specified in command argument output_dir.

    @param final_user: string object containing the user id or the operator.
    @param output_dir: string object containing the path to the export directory (command argument).
    @return output_dir_path: string object containing the newly created directory path for all retained exports.
    """

    # create string object from final_user and datetime.
    date_time_replace = str(datetime.now()).replace('-', '')
    date_time_list = date_time_replace.split(' ')
    date_time_list_split = date_time_list[1].split(':')
    output_dir_path = output_dir + '\\' + final_user + '_collation_tile_data_previous_fire' + str(
        date_time_list[0]) + '_' + str(
        date_time_list_split[0]) + str(
        date_time_list_split[1])

    # check if the folder already exists - if False = create directory, if True = return error message.
    try:
        shutil.rmtree(output_dir_path)

    except:
        print('The following export directory will be created: ', output_dir_path)
        pass

    # create folder.
    os.makedirs(output_dir_path)

    return output_dir_path


def export_folders(output_dir_path):
    sr_fc_zonal_stats = os.path.join(output_dir_path, "sr_fc_zonal_stats")
    os.mkdir(sr_fc_zonal_stats)

    ht_dn_zonal_stats = os.path.join(output_dir_path, "ht_dn_zonal_stats")
    os.mkdir(ht_dn_zonal_stats)

    met_zonal_stats = os.path.join(output_dir_path, "met_zonal_stats")
    os.mkdir(met_zonal_stats)

    met_si = os.path.join(output_dir_path, "met_si")
    os.mkdir(met_si)

    return sr_fc_zonal_stats, ht_dn_zonal_stats, met_zonal_stats, met_si


def main_routine():
    """ Description: This script determines which Landsat tile had the most non-null zonal statistics records per site
    and files those plots (bare ground, all bands and interactive) into final output folders. """

    # print('fcZonalStatsPipeline.py INITIATED.')
    # read in the command arguments
    cmd_args = get_cmd_args_fn()
    biomass_csv = cmd_args.biomass_csv
    tile_dir = cmd_args.tile_dir
    fire_dir = cmd_args.fire_dir
    density_dir = cmd_args.density_dir
    output_dir = cmd_args.output_dir
    met_dir = cmd_args.met_dir
    # met_si_dir = cmd_args.met_si_dir
    # mosaic_dir = cmd_args.mosaic_dir

    # call the temporaryDir function.
    temp_dir_path, final_user = temporary_dir_fn()

    output_dir_path = export_file_path_fn(output_dir, final_user)

    sr_fc_zonal_stats, ht_dn_zonal_stats, met_zonal_stats, met_si = export_folders(output_dir_path)


    # print("1 - line 236")
    # import sys
    # sys.exit()
    # todo working uncomment out
    import step2_merge_tile_zonal_stats
    biomass_df, dbg_dbi_dbi_mask_dp0_dp1_dp1_mask, dbg_dbi_dbi_mask_dp0_dp1_dp1_mask_clean, \
        dbi_dp1_dry, dbi_dp1_dry, dbi_dp1_mask_dry, dbi_dp1_annual, dbi_dp1_mask_annual, \
        dbg_dp0_single, dbg_dp0_mask_single = step2_merge_tile_zonal_stats.main_routine(
        biomass_csv, tile_dir, sr_fc_zonal_stats)

    print("dbg_dbi_dbi_mask_dp0_dp1_dp1_mask: ", dbg_dbi_dbi_mask_dp0_dp1_dp1_mask.shape)
    print("dbi_dp1_dry: ", dbi_dp1_dry.shape)
    print("=" * 100)
    print("=" * 100)
    print("step2_merge_tile_zonal_stats - COMPLETE")
    print("=" * 100)
    print("=" * 100)

    # print("1 - line 252")
    # import sys
    # sys.exit()

    #todo issue ouputting csv
    
    import step3_calculate_indices
    dbg_dbi_dbi_mask_dp0_dp1_dp1_mask_veg_ind, dbg_dbi_dbi_mask_dp0_dp1_dp1_mask_veg_ind_clean, \
        dp0_dbg_si, dp0_dbg_si_mask, dp1_dbi_si_dry, dp1_dbi_si_mask_dry, dp1_dbi_si_annual, \
        dp1_dbi_si_mask_annual = step3_calculate_indices.main_routine(
        dbg_dbi_dbi_mask_dp0_dp1_dp1_mask_clean)


    print("dbg_dbi_dbi_mask_dp0_dp1_dp1_mask_veg_ind: ", dbg_dbi_dbi_mask_dp0_dp1_dp1_mask_veg_ind.shape)
    print("dbg_dbi_dbi_mask_dp0_dp1_dp1_mask_veg_ind_clean: ", dbg_dbi_dbi_mask_dp0_dp1_dp1_mask_veg_ind_clean.shape)
    print("=" * 100)
    print("=" * 100)
    print("step3_calculate_indices - COMPLETE")
    print("=" * 100)
    print("=" * 100)

    # import sys
    # sys.exit("1 - line 268")

    print(biomass_csv)
    print(density_dir)
    print(ht_dn_zonal_stats)

    # # todo working produces a final output uncomment out
    # import step4_merge_tile_density_height_zonal_stats
    # biomass_df, ccw_fdc_h99_hcv_hmc_hsd_n17_wdc_wfp, ccw_fdc_h99_hcv_hmc_hsd_n17_wdc_wfp_clean, \
    #     dp0_dbg_si_single_annual_density, dp0_dbg_si_mask_single_annual_density, dp0_dbg_si_single_dry_density, \
    #     dp0_dbg_si_mask_single_dry_density, dp1_dbi_si_annual_density, dp1_dbi_si_annual_mask_density, \
    #     dp1_dbi_si_dry_density, \
    #     dp1_dbi_si_dry_mask_density = step4_merge_tile_density_height_zonal_stats.main_routine(biomass_csv,
    #                                                                                                 density_dir,
    #                                                                                                 ht_dn_zonal_stats,
    #

    # todo working produces a final output uncomment out
    import step4_merge_tile_density_height_zonal_stats_wh25
    biomass_df, ccw_fdc_h99_hcv_hmc_hsd_n17_wdc_wfp, ccw_fdc_h99_hcv_hmc_hsd_n17_wdc_wfp_clean, \
        dp0_dbg_si_single_annual_density, dp0_dbg_si_mask_single_annual_density, dp0_dbg_si_single_dry_density, \
        dp0_dbg_si_mask_single_dry_density, dp1_dbi_si_annual_density, dp1_dbi_si_annual_mask_density, \
        dp1_dbi_si_dry_density, \
        dp1_dbi_si_dry_mask_density = step4_merge_tile_density_height_zonal_stats_wh25.main_routine(biomass_csv,
                                                                                               density_dir,
                                                                                               ht_dn_zonal_stats,
                                                                                               dp0_dbg_si,
                                                                                               dp0_dbg_si_mask,
                                                                                               dp1_dbi_si_dry,
                                                                                               dp1_dbi_si_mask_dry,
                                                                                               dp1_dbi_si_annual,
                                                                                               dp1_dbi_si_mask_annual)




    # print("Finish 1 - line 279 - sr and density only")
    # import sys
    # sys.exit()
    print("=" * 100)
    print("=" * 100)
    print("step3_merge_tile_density_height_zonal_stats - COMPLETE")
    print("=" * 100)
    print("=" * 100)

    # #biomass_csv.to_csv(r"C:\Users\robot\projects\biomass\scratch_outputs\biomass_csv.csv", index=False)
    # import step5_merge_meteorological_data_agb_zonal_stats
    # near_met, dp0_dbg_si_single_annual_density_near_met, \
    #     dp0_dbg_si_mask_single_annual_density_near_met, \
    #     dp0_dbg_si_single_dry_density_near_met, \
    #     dp0_dbg_si_mask_single_dry_density_near_met, \
    #     dp1_dbi_si_annual_density_near_met, \
    #     dp1_dbi_si_annual_mask_density_near_met, \
    #     dp1_dbi_si_dry_density_near_met, \
    #     dp1_dbi_si_dry_mask_density_near_met = step5_merge_meteorological_data_agb_zonal_stats.main_routine(biomass_csv,
    #                                                                                                         met_dir,
    #                                                                                                         met_zonal_stats,
    #                                                                                                         dp0_dbg_si_single_annual_density,
    #                                                                                                         dp0_dbg_si_mask_single_annual_density,
    #                                                                                                         dp0_dbg_si_single_dry_density,
    #                                                                                                         dp0_dbg_si_mask_single_dry_density,
    #                                                                                                         dp1_dbi_si_annual_density,
    #                                                                                                         dp1_dbi_si_annual_mask_density,
    #                                                                                                         dp1_dbi_si_dry_density,
    #                                                                                                         dp1_dbi_si_dry_mask_density)

    #biomass_csv.to_csv(r"C:\Users\robot\projects\biomass\scratch_outputs\biomass_csv.csv", index=False)
    import step5_merge_meteorological_data_agb_zonal_stats_orig_data
    near_met, dp0_dbg_si_single_annual_density_near_met, \
        dp0_dbg_si_mask_single_annual_density_near_met, \
        dp0_dbg_si_single_dry_density_near_met, \
        dp0_dbg_si_mask_single_dry_density_near_met, \
        dp1_dbi_si_annual_density_near_met, \
        dp1_dbi_si_annual_mask_density_near_met, \
        dp1_dbi_si_dry_density_near_met, \
        dp1_dbi_si_dry_mask_density_near_met = step5_merge_meteorological_data_agb_zonal_stats_orig_data.main_routine(biomass_csv,
                                                                                                            met_dir,
                                                                                                            met_zonal_stats,
                                                                                                            dp0_dbg_si_single_annual_density,
                                                                                                            dp0_dbg_si_mask_single_annual_density,
                                                                                                            dp0_dbg_si_single_dry_density,
                                                                                                            dp0_dbg_si_mask_single_dry_density,
                                                                                                            dp1_dbi_si_annual_density,
                                                                                                            dp1_dbi_si_annual_mask_density,
                                                                                                            dp1_dbi_si_dry_density,
                                                                                                            dp1_dbi_si_dry_mask_density)

    # print("Finish 1 - line 332 - sr and density met only")
    # import sys
    # sys.exit()

    print("=" * 100)
    print("=" * 100)
    print("step5_merge_meteorological_data_agb_zonal_stats - COMPLETE")
    print("=" * 100)
    print("=" * 100)

    # import step5_merge_meteorological_si
    # seasonal_si_clean, dp0_dbg_si_single_annual_density_near_met_si, \
    #     dp0_dbg_si_mask_single_annual_density_near_met_si, \
    #     dp0_dbg_si_single_dry_density_near_met_si, \
    #     dp0_dbg_si_mask_single_dry_density_near_met_si, \
    #     dp1_dbi_si_annual_density_near_met_si, \
    #     dp1_dbi_si_annual_mask_density_near_met_si, \
    #     dp1_dbi_si_dry_density_near_met_si, \
    #     dp1_dbi_si_dry_mask_density_near_met_si = step5_merge_meteorological_si.main_routine(
    #     biomass_csv, met_si_dir, met_si,
    #     dp0_dbg_si_single_annual_density_near_met,
    #     dp0_dbg_si_mask_single_annual_density_near_met,
    #     dp0_dbg_si_single_dry_density_near_met,
    #     dp0_dbg_si_mask_single_dry_density_near_met,
    #     dp1_dbi_si_annual_density_near_met,
    #     dp1_dbi_si_annual_mask_density_near_met,
    #     dp1_dbi_si_dry_density_near_met,
    #     dp1_dbi_si_dry_mask_density_near_met
    # )

    print("=" * 100)
    print("=" * 100)
    print("step5_merge_meteorological_si - COMPLETE")
    print("=" * 100)
    print("=" * 100)

    # import sys
    # sys.exit("line - 445")

    import step6_merge_fire_data_agb_zonal_stats_from_met
    fire_df, dp0_dbg_si_single_annual_density_near_met_fire, \
        dp0_dbg_si_mask_single_annual_density_near_met_fire, \
        dp0_dbg_si_single_dry_density_near_met_fire, \
        dp0_dbg_si_mask_single_dry_density_near_met_fire, \
        dp1_dbi_si_annual_density_near_met_fire, \
        dp1_dbi_si_annual_mask_density_near_met_fire, \
        dp1_dbi_si_dry_density_near_met_fire, \
        dp1_dbi_si_dry_mask_density_near_met_fire = step6_merge_fire_data_agb_zonal_stats_from_met.main_routine(
        biomass_csv, fire_dir, output_dir_path,
        dp0_dbg_si_single_annual_density_near_met,
        dp0_dbg_si_mask_single_annual_density_near_met,
        dp0_dbg_si_single_dry_density_near_met,
        dp0_dbg_si_mask_single_dry_density_near_met,
        dp1_dbi_si_annual_density_near_met,
        dp1_dbi_si_annual_mask_density_near_met,
        dp1_dbi_si_dry_density_near_met,
        dp1_dbi_si_dry_mask_density_near_met)

    import sys
    sys.exit("step 1 386")

    # import step6_merge_tile_seasonal_fire_zonal_stats
    # fire_df, dp0_dbg_si_single_annual_density_near_met_si_fire = step6_merge_tile_seasonal_fire_zonal_stats.main_routine(
    #     biomass_df, mosaic_dir, output_dir_path, dp0_dbg_si_single_annual_density_near_met_si,
    #     dp0_dbg_si_mask_single_annual_density_near_met,
    #     dp0_dbg_si_single_dry_density_near_met,
    #     dp0_dbg_si_mask_single_dry_density_near_met,
    #     dp1_dbi_si_annual_density_near_met,
    #     dp1_dbi_si_annual_mask_density_near_met,
    #     dp1_dbi_si_dry_density_near_met,
    #     dp1_dbi_si_dry_mask_density_near_met)

    print("=" * 100)
    print("=" * 100)
    print("step6_merge_tile_seasonal_fire_zonal_stats - COMPLETE")
    print("=" * 100)
    print("=" * 100)

    # ======================================= MERGE ================================================

    # ----------------------------------- biomass and veg indices ----------------------------------------------------
    print(list(dbg_dbi_dbi_mask_dp0_dp1_dp1_mask_veg_ind_clean.columns))
    print(dbg_dbi_dbi_mask_dp0_dp1_dp1_mask_veg_ind_clean.shape)
    dbg_dbi_dbi_mask_dp0_dp1_dp1_mask_veg_ind_clean.to_csv(
        r"U:\biomass\collated_zonal_stats\dbg_dbi_dbi_mask_dp0_dp1_dp1_mask_veg_ind_clean.csv", index=False)

    biomass_df.rename(
        columns={'uid_x': 'uid', 'date_x': 'date'}, inplace=True)

    dbg_dbi_dbi_mask_dp0_dp1_dp1_mask_veg_ind_clean.rename(
        columns={'uid_x': 'uid', 'date_x': 'date'}, inplace=True)

    all_sites_dbg_dbi_dbi_mask_dp0_dp1_dp1_mask_veg_ind_clean = pd.merge(
        left=biomass_df, right=dbg_dbi_dbi_mask_dp0_dp1_dp1_mask_veg_ind_clean, how="outer", on=[
            'uid',
            'site_clean',
            'date', 'lon_gda94',
            'lat_gda94',
            'geometry',
            'bio_l_kg1ha',
            'bio_t_kg1ha',
            'bio_b_kg1ha',
            'bio_w_kg1ha',
            'bio_br_kg1ha',
            'bio_s_kg1ha',
            'bio_r_kg1ha',
            'bio_agb_kg1ha',
            'c_l_kg1ha',
            'c_t_kg1ha',
            'c_b_kg1ha',
            'c_w_kg1ha',
            'c_br_kg1ha',
            'c_s_kg1ha',
            'c_r_kg1ha',
            'c_agb_kg1ha',
            # 'basal_dt'
        ])

    all_sites_dbg_dbi_dbi_mask_dp0_dp1_dp1_mask_veg_ind_clean.to_csv(
        r"U:\biomass\collated_zonal_stats\final_out\all_sites_dbg_dbi_dbi_mask_dp0_dp1_dp1_mask_veg_ind_clean.csv", index=False)

    # ------------------------------------------ add density and height ------------------------------------------------
    all_sites_dbg_dbi_dbi_mask_dp0_dp1_dp1_mask_veg_ind_clean.rename(
        columns={'uid_x': 'uid', 'date_x': 'date'}, inplace=True)

    ccw_fdc_h99_hcv_hmc_hsd_n17_wdc_wfp_clean.rename(
        columns={'uid_x': 'uid', 'date_x': 'date'}, inplace=True)

    all_sites_dbg_dbi_dbi_mask_dp0_dp1_dp1_mask_veg_ind_ccw_fdc_h99_hcv_hmc_hsd_n17_wdc_wfp_clean = pd.merge(
        left=all_sites_dbg_dbi_dbi_mask_dp0_dp1_dp1_mask_veg_ind_clean,
        right=ccw_fdc_h99_hcv_hmc_hsd_n17_wdc_wfp_clean, how="outer", on=[
            'uid',
            'site_clean',
            'date', 'lon_gda94',
            'lat_gda94',
            'geometry',
            'bio_l_kg1ha',
            'bio_t_kg1ha',
            'bio_b_kg1ha',
            'bio_w_kg1ha',
            'bio_br_kg1ha',
            'bio_s_kg1ha',
            'bio_r_kg1ha',
            'bio_agb_kg1ha',
            'c_l_kg1ha',
            'c_t_kg1ha',
            'c_b_kg1ha',
            'c_w_kg1ha',
            'c_br_kg1ha',
            'c_s_kg1ha',
            'c_r_kg1ha',
            'c_agb_kg1ha',
            # 'basal_dt'
        ])

    all_sites_dbg_dbi_dbi_mask_dp0_dp1_dp1_mask_veg_ind_ccw_fdc_h99_hcv_hmc_hsd_n17_wdc_wfp_clean.to_csv(
        r"U:\biomass\collated_zonal_stats\final_out"
        r"\all_sites_dbg_dbi_dbi_mask_dp0_dp1_dp1_mask_veg_ind_ccw_fdc_h99_hcv_hmc_hsd_n17_wdc_wfp_clean.csv",
        index=False)

    # --------------------------------------------------- add met ------------------------------------------------------
    all_sites_dbg_dbi_dbi_mask_dp0_dp1_dp1_mask_veg_ind_ccw_fdc_h99_hcv_hmc_hsd_n17_wdc_wfp_met_clean = pd.merge(
        left=all_sites_dbg_dbi_dbi_mask_dp0_dp1_dp1_mask_veg_ind_ccw_fdc_h99_hcv_hmc_hsd_n17_wdc_wfp_clean,
        right=near_met, how="outer", on=['site_clean', 'bio_agb_kg1ha'])

    all_sites_dbg_dbi_dbi_mask_dp0_dp1_dp1_mask_veg_ind_ccw_fdc_h99_hcv_hmc_hsd_n17_wdc_wfp_met_clean.to_csv(
        r"U:\biomass\collated_zonal_stats\final_out"
        r"\all_sites_dbg_dbi_dbi_mask_dp0_dp1_dp1_mask_veg_ind_ccw_fdc_h99_hcv_hmc_hsd_n17_wdc_wfp_met_clean.csv",
        index=False)

    # ---------------------------------------------- Add met si -------------------------------------------------------

    print("all_sites_dbg_dbi_dbi_mask_dp0_dp1_dp1_mask_veg_ind_ccw_fdc_h99_hcv_hmc_hsd_n17_wdc_wfp_met_clean: ", list(
        all_sites_dbg_dbi_dbi_mask_dp0_dp1_dp1_mask_veg_ind_ccw_fdc_h99_hcv_hmc_hsd_n17_wdc_wfp_met_clean.columns))
    print("seasonal_si_clean: ", list(
        seasonal_si_clean.columns))



    # Renaming columns
    all_sites_dbg_dbi_dbi_mask_dp0_dp1_dp1_mask_veg_ind_ccw_fdc_h99_hcv_hmc_hsd_n17_wdc_wfp_met_clean.rename(
            columns={'uid_x': 'uid', 'date_x': 'date'}, inplace=True)

    seasonal_si_clean.rename(columns={'uid_x': 'uid', 'date_x': 'date'}, inplace=True)

    all_sites_dbg_dbi_dbi_mask_dp0_dp1_dp1_mask_veg_ind_ccw_fdc_h99_hcv_hmc_hsd_n17_wdc_wfp_met_seasonal_si_clean = pd.merge(
        left=all_sites_dbg_dbi_dbi_mask_dp0_dp1_dp1_mask_veg_ind_ccw_fdc_h99_hcv_hmc_hsd_n17_wdc_wfp_met_clean,
        right=seasonal_si_clean, how="outer", on=[
            'uid',
            'site_clean',
            'date', 'lon_gda94',
            'lat_gda94',
            'geometry',
            'bio_l_kg1ha',
            'bio_t_kg1ha',
            'bio_b_kg1ha',
            'bio_w_kg1ha',
            'bio_br_kg1ha',
            'bio_s_kg1ha',
            'bio_r_kg1ha',
            'bio_agb_kg1ha',
            'c_l_kg1ha',
            'c_t_kg1ha',
            'c_b_kg1ha',
            'c_w_kg1ha',
            'c_br_kg1ha',
            'c_s_kg1ha',
            'c_r_kg1ha',
            'c_agb_kg1ha',
            # 'basal_dt',
        ])

    all_sites_dbg_dbi_dbi_mask_dp0_dp1_dp1_mask_veg_ind_ccw_fdc_h99_hcv_hmc_hsd_n17_wdc_wfp_met_seasonal_si_clean.to_csv(
        r"U:\biomass\collated_zonal_stats\final_out\all_sites_dbg_dbi_dbi_mask_dp0_dp1_dp1_mask_veg_ind_ccw_fdc_h99_hcv_hmc_hsd_n17_wdc_wfp_met_seasonal_si_clean.csv",
        index=False)

    print("all_sites_dbg_dbi_dbi_mask_dp0_dp1_dp1_mask_veg_ind_ccw_fdc_h99_hcv_hmc_hsd_n17_wdc_wfp_met_seasonal_si_clean: ", list(
        all_sites_dbg_dbi_dbi_mask_dp0_dp1_dp1_mask_veg_ind_ccw_fdc_h99_hcv_hmc_hsd_n17_wdc_wfp_met_seasonal_si_clean.columns))

    print("fire_df: ", list(
        fire_df.columns))
    # ----------------------------------------------- add fire ---------------------------------------------------------


    # import sys
    # sys.exit()

    all_sites_dbg_dbi_dbi_mask_dp0_dp1_dp1_mask_veg_ind_ccw_fdc_h99_hcv_hmc_hsd_n17_wdc_wfp_met_seasonal_si_fire_clean = pd.merge(
        left=all_sites_dbg_dbi_dbi_mask_dp0_dp1_dp1_mask_veg_ind_ccw_fdc_h99_hcv_hmc_hsd_n17_wdc_wfp_met_seasonal_si_clean,
        right=fire_df, how="outer", on=[
            'site_clean',
            'date', 'lon_gda94',
            'lat_gda94',
            'geometry',
            'bio_l_kg1ha',
            'bio_t_kg1ha',
            'bio_b_kg1ha',
            'bio_w_kg1ha',
            'bio_br_kg1ha',
            'bio_s_kg1ha',
            'bio_r_kg1ha',
            'bio_agb_kg1ha',
            'c_l_kg1ha',
            'c_t_kg1ha',
            'c_b_kg1ha',
            'c_w_kg1ha',
            'c_br_kg1ha',
            'c_s_kg1ha',
            'c_r_kg1ha',
            'c_agb_kg1ha',
            # 'basal_dt'
        ])

    all_sites_dbg_dbi_dbi_mask_dp0_dp1_dp1_mask_veg_ind_ccw_fdc_h99_hcv_hmc_hsd_n17_wdc_wfp_met_seasonal_si_fire_clean.to_csv(
        r"U:\biomass\collated_zonal_stats\final_out\all_sites_dbg_dbi_dbi_mask_dp0_dp1_dp1_mask_veg_ind_ccw_fdc_h99_hcv_hmc_hsd_n17_wdc_wfp_met_seasonal_si_fire_clean.csv",
        index=False)

    # ---------------------------------------------------- Clean up ----------------------------------------------------

    shutil.rmtree(temp_dir_path)
    print('Temporary directory and its contents has been deleted from your working drive.')
    print(' - ', temp_dir_path)
    print('fractional cover zonal stats pipeline is complete.')
    print('goodbye.')


if __name__ == '__main__':
    main_routine()
