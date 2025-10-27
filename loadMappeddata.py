from functions import (get_state_filepath, get_state_mfc_filepath, deduplicate_timestamps,
                        replace_constant_numeric_columns, exclude_columns)
import pandas as pd
import json
from pathlib import Path
from typing import Iterable


if __name__ == "__main__":
    # Load configuration from inputs.json
    inputs_path = Path(__file__).parent / "inputs.json"
    with open(inputs_path, encoding="utf-8") as f:
        config = json.load(f)
    
    raw_data_dir = Path(config["Folder Path"])
    data_pack_name = config["Data Pack Name"]
    
    DATALOG_PATH = get_state_filepath()
    MFC_PATH = get_state_mfc_filepath()
    df = pd.read_parquet(str(DATALOG_PATH) + ".parquet")
    dfMfc = pd.read_parquet(str(MFC_PATH) + ".parquet")

    # Convert first column (Date/Time) to datetime and sort chronologically
    df.iloc[:, 0] = pd.to_datetime(df.iloc[:, 0], dayfirst=True, errors='coerce')
    df = df.sort_values(by=df.columns[0]).reset_index(drop=True)
    #add a time step column based on the new index
    if "Time Step" in df.columns:
        df = df.drop(columns="Time Step")
    df.insert(1, "Time Step", df.index)
    
    dfMfc.iloc[:, 0] = pd.to_datetime(dfMfc.iloc[:, 0], dayfirst=True, errors='coerce')
    dfMfc = dfMfc.sort_values(by=dfMfc.columns[0]).reset_index(drop=True)
    timestamp_col_mfc = dfMfc.columns[0]

    ##resample  mfc data to 1 second intervals
    dfMfc = (dfMfc
             .set_index(timestamp_col_mfc)
             .resample("1S")
             .mean(numeric_only=True)
             .reset_index())
    
    print(len(dfMfc.iloc[:, 0]))
    #add a time step column based on the new index
    if "Time Step" in dfMfc.columns:
        dfMfc = dfMfc.drop(columns="Time Step")
    dfMfc.insert(1, "Time Step", dfMfc.index)

    # Identify repeated timestamps within each dataframe
    datalog_duplicates = df[df.iloc[:, 0].duplicated(keep=False)]
    mfc_duplicates = dfMfc[dfMfc.iloc[:, 0].duplicated(keep=False)]
    
    # Define output paths using Data Pack Name
    datalog_precomparison = raw_data_dir / f"{data_pack_name}_precomparison.csv"
    mfc_precomparison = raw_data_dir / f"{data_pack_name}_MFC_precomparison.csv"
    
    dfMfc.to_csv(mfc_precomparison, index=False)
    df.to_csv(datalog_precomparison, index=False)
    
    # Check and print repeated timestamps
    if not datalog_duplicates.empty:
        print("\nRepeated timestamps found in datalog:")
        print(datalog_duplicates)
        df=deduplicate_timestamps(df)
    
    else:
        print("\nNo repeated timestamps found in datalog.")
    # Check and print repeated mfc timestamps
    if not mfc_duplicates.empty:
        print("\nRepeated timestamps found in MFC:")
        print(mfc_duplicates)
        dfMfc=deduplicate_timestamps(dfMfc)
    else:
        print("\nNo repeated timestamps found in MFC.")

    dfMfc = replace_constant_numeric_columns(dfMfc)
    df = replace_constant_numeric_columns(df)

    ##excluding specified columns 
    excluded_columns = config.get("Excluded Columns", [])
    if excluded_columns:
        print("excluding columns")
        df = exclude_columns(df, excluded_columns)
        dfMfc = exclude_columns(dfMfc, excluded_columns)

    # Define final output paths using Data Pack Name
    datalog_final = raw_data_dir / f"{data_pack_name}_DataPack_final.csv"
    mfc_final = raw_data_dir / f"{data_pack_name}_MFC_DataPack_final.csv"
    
    dfMfc.to_csv(mfc_final, index=False)
    df.to_csv(datalog_final, index=False)
    
    print(f"\nFinal outputs written:")
    print(f"  Datalog: {datalog_final}")
    print(f"  MFC: {mfc_final}")




   




    