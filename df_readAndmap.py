import numpy as np
import pandas as pd 
import json
import subprocess
import sys
from pathlib import Path
from functions import set_state, get_state_filepath, has_state, build_output_headers
from file_discovery import discover_files
from typing import Dict, List


if __name__ == "__main__":
    # Load configuration from inputs.json
    inputs_path = Path(__file__).parent / "inputs.json"
    with open(inputs_path, encoding="utf-8") as f:
        config = json.load(f)
    
    raw_data_dir = Path(config["Folder Path"])
    data_pack_name = Path(config["Data Pack Name"])
    
    # Discover and categorize files
    discovered = discover_files(raw_data_dir)
    
    print(f"Found {len(discovered.mfc_files)} MFC file(s):")
    for mfc in discovered.mfc_files:
        print(f"  - {mfc.name}")
    
    print(f"\nFound {len(discovered.datalog_files)} datalog file(s):")
    for datalog in discovered.datalog_files:
        print(f"  - {datalog.name}")


    #make a list of columns input from gui an option also 
    #engineer_input_columns = []
    preset_columns = config["Datalog columns"]
    
    additional_columns = config["Additional columns"]

    # make a preset headers list for scania and one that can be customised for the other customer data sets 
    #engineer_input_headers = [] ##make this an input from gui 
    preset_colsMfc = config["MFC columns"]

    preset_headers = config["Datalog names"]

    preset_headersMfc = config["MFC names"]


    assert len(preset_columns) == len(preset_headers), "Preset columns and headers length mismatch"
    assert len(preset_colsMfc) == len(preset_headersMfc), "Preset MFC columns and headers length mismatch"

    output_headers = build_output_headers(preset_headers, additional_columns)

    # Create sorted mappings (pandas reads usecols in sorted order)
    # Then we'll reindex to the desired output order
    headerMap = dict(sorted(zip(preset_columns, preset_headers)))
    headerMapMfc = dict(sorted(zip(preset_colsMfc, preset_headersMfc)))

    usecolumns = list(headerMap.keys())
    usecolumnsMfc = list(headerMapMfc.keys())
    useHeaders = list(headerMap.values())
    useHeadersMfc = list(headerMapMfc.values())
    
    # Read and concatenate all datalog files
    print("\nReading datalog files...")
    datalog_chunks = []
    for datalog_path in discovered.datalog_files:
        if not datalog_path.exists():
            print(f"  Warning: {datalog_path.name} not found, skipping.")
            continue
        print(f"  Reading {datalog_path.name}...")
        chunk = pd.read_csv(
            datalog_path,
            sep="\t",
            usecols=usecolumns,
            parse_dates=True,
            low_memory=False,
            header=None,
            names=useHeaders,
        )
        datalog_chunks.append(chunk)
    
    if not datalog_chunks:
        raise ValueError("No datalog files were successfully read.")
    
    df = pd.concat(datalog_chunks, ignore_index=True)
    print(f"Combined datalog: {len(df)} rows")

    # Read and concatenate all MFC files
    print("\nReading MFC files...")
    mfc_chunks = []
    for mfc_path in discovered.mfc_files:
        if not mfc_path.exists():
            print(f"  Warning: {mfc_path.name} not found, skipping.")
            continue
        print(f"  Reading {mfc_path.name}...")
        chunk = pd.read_csv(
            mfc_path,
            sep="\t",
            usecols=usecolumnsMfc,
            parse_dates=True,
            low_memory=False,
            header=None,
            names=useHeadersMfc,
        )
        mfc_chunks.append(chunk)
    
    if not mfc_chunks:
        raise ValueError("No MFC files were successfully read.")
    
    dfMfc = pd.concat(mfc_chunks, ignore_index=True)
    print(dfMfc.head())
    print(f"Combined MFC: {len(dfMfc)} rows")

    # Add additional columns
    for new_column in additional_columns:
        if new_column not in df.columns:
            df[new_column] = "-"

    df = df.reindex(columns=output_headers)
    dfMfc = dfMfc.reindex(columns=preset_headersMfc)

    print(dfMfc.head())

    # Determine output paths using Data Pack Name
    datalog_output = raw_data_dir / f"{data_pack_name}"
    mfc_output = raw_data_dir / f"{data_pack_name}_MFC"

    print(f"\nWriting combined datalog to {datalog_output.name}...")
    df.to_parquet(datalog_output.with_suffix('.parquet'), index=False)
    df.to_csv(datalog_output.with_suffix('.csv'), index=False)
    
    print(f"Writing combined MFC to {mfc_output.name}...")
    dfMfc.to_parquet(mfc_output.with_suffix('.parquet'), index=False)
    dfMfc.to_csv(mfc_output.with_suffix('.csv'), index=False)

    # Store paths in state for downstream processing
    set_state(datalog_output, mfc_output)
    print("\nStep 1 complete! Starting data cleaning and finalization...\n")
    
    # Automatically run loadMappeddata.py
    loadmappeddata_script = Path(__file__).parent / "loadMappeddata.py"
    result = subprocess.run(
        [sys.executable, str(loadmappeddata_script)],
        capture_output=False,
        text=True
    )
    
    if result.returncode == 0:
        print("\n✅ All processing complete! Final data packs are ready.")
    else:
        print(f"\n⚠️ Step 2 encountered an error. Check output above for details.")
        sys.exit(result.returncode)

   



