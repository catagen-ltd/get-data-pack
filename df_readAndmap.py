import numpy as np
import pandas as pd 
from pathlib import Path
from state import set_state, get_state_filepath, has_state


if __name__ == "__main__":
    filepath_input = Path(r"C:\LocalOnly\Data packs raw data\SCA005_SCA006_AgeingTuning_240925 41")
    mfc_filepath_input = Path(r"C:\LocalOnly\Data packs raw data\SCA005_SCA006_AgeingTuning_240925_MFC 0")
    FILEPATH = filepath_input.resolve()
    MFC_FILEPATH = mfc_filepath_input.resolve()
    print(FILEPATH) 
    print(MFC_FILEPATH)
    set_state(FILEPATH, MFC_FILEPATH)


    #make a list of columns input from gui an option also 
    #engineer_input_columns = []
    preset_columns = [
    0,   # A
    1,   # B
    3,   # D
    4,   # E
    5,   # F
    6,   # G
    39,  # AN
    40,  # AO
    41,  # AP
    42,  # AQ
    78,  # CA
    79,  # CB
    82,  # CE
    83,  # CF
    8,   # I
    9,   # J
    10,  # K
    11,  # L
    12,  # M
    13,  # N
    46,  # AU
    47,  # AV
    48,  # AW
    49,  # AX
    50,  # AY
    51,  # AZ
    52,  # BA
    53,  # BB
    54,  # BC
    55,  # BD
    56,  # BE
    57,  # BF
    58,  # BG
    59,  # BH
    106, # DB
    107, # DC
    108, # DD
    109, # DE
    110, # DF
    111, # DG
    112, # DH
    113, # DI
    114  # DJ
    ]


    # make a preset headers list for scania and one that can be customised for the other customer data sets 
    #engineer_input_headers = [] ##make this an input from gui 
    preset_colsMfc = [0, 2, 3, 11, 8, 10, 4, 13, 14, 22, 24, 21, 15]



    preset_headers = [
    "Date/Time",
    #"Time Step",
    "Flow (g/s)",
    "Cat 1 Inlet Analogue ETAS 1 (-)",
    "Cat 1 Outlet Analogue ETAS 2 (-)",
    "Cat 2 Inlet Analogue ETAS 3 (-)",
    "Cat 2 Outlet Analogue ETAS 4 (-)",
    "NB Sensor Cat 1 Mid Brick (V)",
    "NB Sensor Cat 1 Outlet (V)",
    "NB Sensor Cat 2 Mid Brick (V)",
    "NB Sensor Cat 2 Outlet (V)",
    "Cat 1 Inlet Digital ETAS 1 (-)",
    "Cat 1 Outlet Digital ETAS 2 (-)",
    "Cat 2 Inlet Digital ETAS 3 (-)",
    "Cat 2 Outlet Digital ETAS 4 (-)",
    "Cat 1 Inlet Gas Temperature (°C)",
    "Cat 1 Bed 1 Centre (°C)",
    "Cat 1 Bed 2 Centre (°C)",
    "Cat 1 Bed 2 Circumference Door Side (°C)",
    #"Cat 1 Bed 2 Circumference Machine Side (°C)",
    "Cat 1 Midpoint Gas Temperature (°C)",
    "Cat 1 Bed 3 Centre (°C)",
    "Cat 1 Bed 4 Centre (°C)",
    "Cat 1 Bed 5 Centre (°C)",
    "Cat 1 Bed 5 Circumference Door Side (°C)",
    #"Cat 1 Bed 5 Circumference Machine Side (°C)",
    "Cat 1 Outlet Gas Temperature (°C)",
    "Cat 2 Inlet Gas Temperature (°C)",
    "Cat 2 Bed 1 Centre (°C)",
    "Cat 2 Bed 2 Centre (°C)",
    "Cat 2 Bed 2 Circumference Door Side (°C)",
    #"Cat 2 Bed 2 Circumference Machine Side (°C)",
    "Cat 2 Midpoint Gas Temperature (°C)",
    "Cat 2 Bed 3 Centre (°C)",
    "Cat 2 Bed 4 Centre (°C)",
    "Cat 2 Bed 5 Centre (°C)",
    "Cat 2 Bed 5 Circumference Door Side (°C)",
    #"Cat 2 Bed 5 Circumference Machine Side (°C)",
    "Cat 2 Outlet Gas Temperature (°C)",
    "Water Concentration (%)",
    "CH4 Concentration (ppm)",
    "CO Concentration (%)",
    "NO Concentration (ppm)",
    "C3H6 Concentration (ppm)",
    "CO2 Concentration(%)",
    "NH3 Concentration (ppm)",
    "N2O Concentration (ppm)",
    "NO2 Concentration (ppm)"
    ]

    preset_headersMfc = [
    "Date/Time",
    "005 NG Injection (SLPM)",
    "005 Air Injection (SLPM)",
    "005 O2 Injection (SLPM)",
    "006 Air Injection (SLPM)",
    "006 NG Injection (SLPM)",
    "006 O2 Injection (SLPM)",
    "005 NG Injection Set Point (SLPM)",
    "005 Air Injection Set Point (SLPM)",
    "005 O2 Injection Set Point (SLPM)",
    "006 Air Injection Set Point (SLPM)",
    "006 NG Injection Set Point (SLPM)",
    "006 O2 Injection Set Point (SLPM)"
    ]

    #check if file exists
    if FILEPATH.exists():
        print("file exists")
    else:
        print(f"file doesn't exist at {FILEPATH}")

    #read in files
    df = pd.read_csv(FILEPATH, sep="\t", usecols=preset_columns, parse_dates=True, low_memory=False, header=None, names=preset_headers)
    dfMfc = pd.read_csv(MFC_FILEPATH, sep="\t", usecols=preset_colsMfc, parse_dates=True, low_memory=False, header=None, names=preset_headersMfc)

    

    df.to_parquet(FILEPATH.with_suffix('.parquet'), index=False)
    dfMfc.to_parquet(MFC_FILEPATH.with_suffix('.parquet'), index=False)
    dfMfc.to_csv(MFC_FILEPATH.with_suffix('.csv'), index=False)
    df.to_csv(FILEPATH.with_suffix('.csv'), index=False)

   



