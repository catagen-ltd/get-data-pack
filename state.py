from pathlib import Path
import json
import pandas as pd 

PROJECT_DIR = Path(__file__).resolve().parent
STATE = PROJECT_DIR / "state.json"

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
    