import os
import pandas as pd

root_dir = r"C:\Users\robot\projects\biomass\zonal_stats_raw\met_clean\daily_rain"
print("root_dir", root_dir)

# Iterate through all directories and subdirectories
for root, dirs, files in os.walk(root_dir):
    print("dirs", dirs)
    print("files", files)
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
        print(f"Concatenated dataframe for {root}:")
        print(combined_df)
