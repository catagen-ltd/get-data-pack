from __future__ import annotations

from pathlib import Path
import json
import pandas as pd 
from typing import Dict, List, Iterable, NamedTuple




PROJECT_DIR = Path(__file__).resolve().parent
STATE = PROJECT_DIR / "state.json"
CONSTANT_SENTINELS: tuple[float, ...] = (0.0, 1372.0)

#
def set_state(datalog_path: Path, mfc_path: Path) -> None:
    """call to set the filepaths in the state file

    Args:
        datalog_path (Path): the datalog filepath to set
        mfc_path (Path): the mfc filepath to set
    """
    STATE.write_text(json.dumps({
        "datalog_last_used_path": str(datalog_path),
        "mfc_last_used_path": str(mfc_path)
    }, indent=2), encoding="utf-8")

def get_state_filepath() -> Path:
    """get the datalog filepath from the state file

    Returns:
        Path: the datalog filepath 
    """
    if STATE.exists():
        state_data = json.loads(STATE.read_text(encoding="utf-8"))
    else:
        print("State file not found, using default path.")
    
    return Path(state_data.get("datalog_last_used_path"))


def get_state_mfc_filepath() -> Path:
    """get the mfc filepath from the state file

    Returns:
        Path: the mfc filepath 
    """
    if STATE.exists():
        state_data = json.loads(STATE.read_text(encoding="utf-8"))
    else:
        print("State file not found, using default path.")
    
    return Path(state_data.get("mfc_last_used_path"))


def has_state() -> bool:
    """find out if the state file exists

    Returns:
        bool: whether the state file exists or not 
    """
    return STATE.exists()  


def deduplicate_timestamps(data: pd.DataFrame) -> pd.DataFrame:
    """Resolve duplicate timestamps by expanding them at 1 Hz.

    The first row in each duplicate block remains unchanged. Later duplicates
    are redistributed at a 1 Hz cadence, first filling the forward window up to
    the next unique timestamp and then any available window back toward the
    previous unique timestamp. When no unique one-second slot remains, the
    surplus rows are dropped.

    Args:
        data (pd.DataFrame): Input data with a datetime column in position 0.

    Returns:
        pd.DataFrame: DataFrame with duplicates adjusted or removed.
    """

    ##if no data return empty df
    if data.empty:
        return data.copy()


    df = data.copy()
    timestamp_col = df.columns[0]

    # list to hold indices to drop when no unique slot exists
    drop_indices: list[int] = []
    i = 0

    timestamp_pos = df.columns.get_loc(timestamp_col)

    # Iterate through the DataFrame to find and adjust duplicate timestamps
    while i < len(df):
        # Locate the run of identical timestamps that starts at position i
        current_time = df.iat[i, timestamp_pos]
        run_end = i
        while (
            run_end + 1 < len(df)
            and df.iat[run_end + 1, timestamp_pos] == current_time
        ):
            run_end += 1

        run_length = run_end - i + 1

        if run_length > 1:
            #Identify the neighbouring distinct timestamps to understand our window to work with
            next_time = (
                df.iat[run_end + 1, timestamp_pos]
                if run_end + 1 < len(df)
                else None
            )
            prev_time = (
                df.iat[i - 1, timestamp_pos]
                if i - 1 >= 0
                else None
            )

            duplicates_in_run = run_length

            # Collect forward slots strictly between current_time and next_time
            forward_slots: list[pd.Timestamp] = []
            if next_time is not None and pd.notna(next_time):
                step = 1
                while True:
                    candidate = current_time + pd.Timedelta(seconds=step)
                    if candidate >= next_time:
                        break
                    forward_slots.append(candidate)
                    step += 1

            # Collect backward slots strictly between prev_time and current_time
            backward_slots: list[pd.Timestamp] = []
            if prev_time is not None and pd.notna(prev_time):
                step = 1
                while True:
                    candidate = current_time - pd.Timedelta(seconds=step)
                    if candidate <= prev_time:
                        break
                    backward_slots.append(candidate)
                    step += 1
            else:
                # No lower bound: create as many slots as needed, extending backwards
                for step in range(1, duplicates_in_run):
                    backward_slots.append(current_time - pd.Timedelta(seconds=step))

            # Build the list of candidate timestamps including the original value
            candidate_slots = sorted(backward_slots + [current_time] + forward_slots)
            print(backward_slots)
            print(current_time)
            print(forward_slots)
            print(candidate_slots)

            # Assign candidate slots to rows in order; drop any surplus rows
            for offset, row_pos in enumerate(range(i, run_end + 1)):
                if offset < len(candidate_slots):
                    df.iat[row_pos, timestamp_pos] = candidate_slots[offset]
                else:
                    drop_indices.append(row_pos)

        # Move to the next block of timestamps
        i = run_end + 1

    if drop_indices:
        # Remove rows that could not be uniquely reassigned
        df.drop(index=df.index[drop_indices], inplace=True)

    # Final tidy-up to leave the caller with a chronologically ordered frame
    df.sort_values(by=timestamp_col, inplace=True)
    df.reset_index(drop=True, inplace=True)
    if drop_indices:
        print(
            "Dropped {count} rows due to insufficient spacing for duplicates."
            .format(count=len(drop_indices))
        )
    return df



def build_output_headers(base_headers: List[str], additions: Dict[str, str]) -> List[str]:
    ordered = base_headers.copy()
    for new_col, reference in additions.items():
        if reference not in ordered:
            raise ValueError(f"Reference column '{reference}' not found while inserting '{new_col}'.")
        if new_col in ordered:
            ordered.remove(new_col)
        ref_index = ordered.index(reference)
        ordered.insert(ref_index + 1, new_col)
    return ordered


def exclude_columns(df: pd.DataFrame, excluded_columns: List[str]) -> pd.DataFrame:
    """Replace all data in specified columns with hyphens.
    
    Args:
        df: The DataFrame to process.
        excluded_columns: List of column names to replace with hyphens.
    
    Returns:
        DataFrame with excluded columns filled with hyphens.
    """
    result = df.copy()
    
    for col in excluded_columns:
        if col in result.columns:
            result[col] = "-"
    
    return result


def replace_constant_numeric_columns(df: pd.DataFrame, values: Iterable[float] = CONSTANT_SENTINELS) -> pd.DataFrame:
    """Replace numeric columns that are entirely one of the sentinel values with hyphens."""

    result = df.copy()
    numeric_columns = result.select_dtypes(include=["number"]).columns

    for col in numeric_columns:
        series = result[col]
        non_na = series.dropna()
        if non_na.empty:
            continue

        hyphenate = False

        for sentinel in values:
            if non_na.eq(sentinel).all():
                hyphenate = True
                break

        if not hyphenate and non_na.lt(0).all():
            hyphenate = True

        if hyphenate:
            result[col] = "-"

    return result


class DiscoveredFiles(NamedTuple):
    """Container for categorized file paths from a raw data directory."""

    mfc_files: list[Path]
    datalog_files: list[Path]


def discover_files(directory: Path) -> DiscoveredFiles:
    """Scan directory and separate MFC files from datalog files.

    Files are classified as MFC if their name (case-insensitive) contains "mfc".
    Files with extensions .parquet, .csv, or .xlsx are skipped.
    All other files are treated as datalog files.

    Args:
        directory: Path to the directory containing raw data files.

    Returns:
        DiscoveredFiles with separate lists for MFC and datalog file paths.
    """
    if not directory.is_dir():
        raise ValueError(f"Path is not a directory: {directory}")

    skip_extensions = {".parquet", ".csv", ".xlsx"}
    mfc_files: list[Path] = []
    datalog_files: list[Path] = []

    for file_path in directory.iterdir():
        if not file_path.is_file():
            continue

        if file_path.suffix.lower() in skip_extensions:
            continue

        if "mfc" in file_path.name.lower():
            mfc_files.append(file_path)
        else:
            datalog_files.append(file_path)

    return DiscoveredFiles(
        mfc_files=sorted(mfc_files),
        datalog_files=sorted(datalog_files),
    )
