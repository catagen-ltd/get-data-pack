from state import get_state_filepath, get_state_mfc_filepath, deduplicate_timestamps
import pandas as pd
from pathlib import Path

filepath_input = Path(r"C:\LocalOnly\Data packs raw data\SCA005_SCA006_AgeingTuning_240925 41 comparison").resolve()
mfc_filepath_input = Path(r"C:\LocalOnly\Data packs raw data\SCA005_SCA006_AgeingTuning_240925_MFC 0 comparison").resolve()
filepath_inputpre = Path(r"C:\LocalOnly\Data packs raw data\SCA005_SCA006_AgeingTuning_240925 41 precomparison").resolve()
mfc_filepath_inputpre = Path(r"C:\LocalOnly\Data packs raw data\SCA005_SCA006_AgeingTuning_240925_MFC 0 precomparison").resolve()
if __name__ == "__main__":
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

    print(len(dfMfc.iloc[:, 0]))
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
    dfMfc.to_csv(mfc_filepath_inputpre.with_suffix('.csv'), index=False)
    df.to_csv(filepath_inputpre.with_suffix('.csv'), index=False)
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

    dfMfc.to_csv(mfc_filepath_input.with_suffix('.csv'), index=False)
    df.to_csv(filepath_input.with_suffix('.csv'), index=False)




   




    