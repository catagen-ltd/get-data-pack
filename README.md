# get-data-pack

Automated tool for processing raw data files into standardized customer data packs.

---

## Overview

This tool combines multiple raw data files (datalog and MFC files) from a directory, processes them according to your configuration, and outputs cleaned, standardized CSV files ready for customer delivery.

### What it does

1. **Reads raw data files** from a specified folder
2. **Separates MFC files** (files with "MFC" in the name) from datalog files
3. **Combines multiple files** into single datalog and MFC datasets
4. **Sorts data chronologically** and removes duplicate timestamps
5. **Resamples MFC data** to 1-second intervals
6. **Replaces invalid data** (all zeros, all 1372s, or all negative values) with hyphens
7. **Outputs clean CSV files** with your specified column names

---

## Getting Started

### Step 1: Prepare Your Data

1. Place all your raw data files in a single folder
2. Ensure MFC files have "MFC" somewhere in their filename (case doesn't matter)
3. Make sure the folder doesn't contain any `.csv`, `.parquet`, or `.xlsx` files you want to process (these will be skipped)

### Step 2: Configure `inputs.json`

Open the `inputs.json` file in a text editor (like Notepad). You'll need to update the following fields:

#### Required Fields

**`"Folder Path"`**  
The full path to the folder containing your raw data files.

```json
"Folder Path": "C:\\LocalOnly\\Data packs raw data"
```

⚠️ **Important**: Use double backslashes (`\\`) in Windows paths, or use forward slashes (`/`).

**`"Data Pack Name"`**  
The name prefix for your output files. This should be descriptive and unique.

```json
"Data Pack Name": "SCA005_SCA006_AgeingTuning_240925"
```

**`"Datalog columns"`**  
A list of column numbers (starting from 0) to extract from datalog files.
Exclude the timestep column, this is created later.

```json
"Datalog columns": [0, 2, 3, 4, 5, 6, 39, 40]
```

💡 **Tip**: Column A in Excel = 0, Column B = 1, Column C = 2, etc.

**`"Datalog names"`**  
The headers you want for each datalog column. Must be in the same order as "Datalog columns".

```json
"Datalog names": [
    "Date/Time",
    "Flow (g/s)",
    "005 Inlet Analogue ETAS 1 (-)",
    "005 Outlet Analogue ETAS 2 (-)"
]
```

⚠️ **Important**: The number of names must match the number of columns exactly.

**`"MFC columns"`**  
A list of column numbers to extract from MFC files.

```json
"MFC columns": [0, 2, 3, 11, 8, 10, 4]
```

**`"MFC names"`**  
The headers for each MFC column.

```json
"MFC names": [
    "Date/Time",
    "005 NG Injection (SLPM)",
    "005 Air Injection (SLPM)"
]
```

**`"Additional columns"`**  
Extra columns to insert with placeholder values (hyphens). 
Input the new column name : the column you want to insert it after
The format is:

```json
"Additional columns": {
    "New Column Name": "Insert After This Column"
}
```

Example:
```json
"Additional columns": {
    "005 Bed 2 Circumference Machine Side (°C)": "005 Bed 2 Circumference Door Side (°C)"
}
```

This will insert a new column called "005 Bed 2 Circumference Machine Side (°C)" immediately after the column "005 Bed 2 Circumference Door Side (°C)", filled with hyphens.

---

## Running the Tool

### Step 1: Process Raw Data

Run the first script to read and combine your files:

```powershell
python df_readAndmap.py
```

**What happens:**
- Scans your folder for datalog and MFC files
- Reads and combines all matching files
- Creates initial output files in the same folder:
  - `{Data Pack Name}_DataPack.parquet`
  - `{Data Pack Name}_DataPack.csv`
  - `{Data Pack Name}_MFC.parquet`
  - `{Data Pack Name}_MFC.csv`

### Step 2: Clean and Finalize Data

Run the second script to sort, deduplicate, and clean:

```powershell
python loadMappeddata.py
```

**What happens:**
- Loads the files created in Step 1
- Sorts data chronologically
- Adds "Time Step" column (0, 1, 2, 3...)
- Detects and resolves duplicate timestamps
- Resamples MFC data to 1-second intervals
- Replaces invalid data columns with hyphens
- Creates final output files:
  - `{Data Pack Name}_DataPack_precomparison.csv` (before deduplication)
  - `{Data Pack Name}_MFC_precomparison.csv` (before deduplication)
  - `{Data Pack Name}_DataPack_final.csv` (cleaned, ready for delivery)
  - `{Data Pack Name}_MFC_final.csv` (cleaned, ready for delivery)

---

## Output Files Explained

| File | Description |
|------|-------------|
| `*_DataPack.csv` | Initial combined datalog data |
| `*_MFC.csv` | Initial combined MFC data |
| `*_DataPack_precomparison.csv` | Sorted datalog before duplicate removal |
| `*_MFC_precomparison.csv` | Sorted MFC before duplicate removal |
| `*_DataPack_final.csv` | **Final cleaned datalog** ✅ |
| `*_MFC_final.csv` | **Final cleaned MFC** ✅ |

💡 The `*_final.csv` files are the ones you deliver to customers.

---

## Common Issues

### "KeyError: Column not found"

**Cause**: A column number in `inputs.json` doesn't exist in your raw files.

**Solution**: Check your raw data files to verify column numbers. Remember, counting starts at 0.

### "Preset columns and headers length mismatch"

**Cause**: The number of items in "Datalog columns" doesn't match "Datalog names" (or same for MFC).

**Solution**: Count the items in both lists and make sure they're equal.

### "Path is not a directory"

**Cause**: The "Folder Path" in `inputs.json` is incorrect or doesn't exist.

**Solution**: 
1. Check the path is correct
2. Use double backslashes: `C:\\Folder\\Path`
3. Or use forward slashes: `C:/Folder/Path`

### No files found

**Cause**: The tool couldn't find any valid files to process.

**Solution**:
1. Make sure your raw data files don't have `.csv`, `.parquet`, or `.xlsx` extensions
2. For MFC files, ensure "MFC" appears somewhere in the filename
3. Check that the folder path is correct

---

## Tips for Non-Developers

1. **Make a backup** of `inputs.json` before editing
2. **Use a proper text editor** like Notepad++ or VS Code (not Microsoft Word)
3. **Validate your JSON** at [jsonlint.com](https://jsonlint.com) after editing
4. **Keep commas** between list items, but not after the last item
5. **Match your quotes**: Use double quotes (`"`) for all text values
6. **Test with a small dataset** first to verify your column numbers are correct

---

## Support

If you encounter issues:
1. Check the console output for error messages
2. Verify your `inputs.json` formatting
3. Ensure your raw data files are in the correct format (tab-separated)
4. Contact the development team with the full error message

---

## File Structure

```
get-data-pack/
├── inputs.json           # Configuration file (edit this!)
├── df_readAndmap.py      # Step 1: Read and combine files
├── loadMappeddata.py     # Step 2: Clean and finalize
├── functions.py          # Helper functions
├── file_discovery.py     # File detection logic
├── state.json            # Internal state (auto-generated)
└── README.md             # This file
```
