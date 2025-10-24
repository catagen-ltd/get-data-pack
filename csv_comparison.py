import pandas as pd
import numpy as np 
from pathlib import Path




original_filepath = Path(r"C:\LocalOnly\Data packs raw data\SCA005_SCA006_AgeingTuning_240925_DataPack 1.csv").resolve()
comparison_filepath = Path(r"C:\LocalOnly\Data packs raw data\SCA005_SCA006_AgeingTuning_240925 41 comparison.csv").resolve()


def load_csv(path: Path) -> pd.DataFrame:
    """Load CSV while handling Windows-encoded exports from Excel."""

    encodings_to_try = ("utf-8", "utf-8-sig", "cp1252", "latin1")
    last_error: Exception | None = None

    for enc in encodings_to_try:
        try:
            return pd.read_csv(path, encoding=enc)
        except UnicodeDecodeError as exc:
            last_error = exc
            continue

    msg = f"Unable to decode {path} with tried encodings {encodings_to_try}."
    if last_error is not None:
        raise UnicodeDecodeError(last_error.encoding, last_error.object, last_error.start, last_error.end, msg)
    raise UnicodeDecodeError("unknown", b"", 0, 0, msg)



def align_frames(df1: pd.DataFrame, df2: pd.DataFrame, key: str | None = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Align by an optional key (e.g., 'Date/Time'), sort rows, and align columns.
    Assumes headers are the first row already parsed by read_csv(names=...).
    """
    a, b = df1.copy(), df2.copy()

    # If there’s a key column, use it to align rows
    if key is not None:
        a[key] = pd.to_datetime(a[key], errors="coerce")
        b[key] = pd.to_datetime(b[key], errors="coerce")
        a = a.sort_values(key).set_index(key)
        b = b.sort_values(key).set_index(key)

    # Put columns in the same order (intersection only by default)
    common_cols = [c for c in a.columns if c in b.columns]
    a = a[common_cols]
    b = b[common_cols]

    # Align row index (outer join so we can spot missing/extra rows)
    a, b = a.align(b, join="outer")

    return a, b

def compare_exact(a: pd.DataFrame, b: pd.DataFrame) -> pd.DataFrame:
    """Exact, type-sensitive comparison (no tolerance)."""
    if a.shape != b.shape or list(a.columns) != list(b.columns) or not a.index.equals(b.index):
        # Force alignment before exact compare
        a, b = a.align(b, join="outer")
    equal = a.equals(b)
    if equal:
        return pd.DataFrame()  # no differences
    # pandas built-in diff (pairs): shows left/right values where different
    return a.compare(b, align_axis=0)

def compare_numeric_with_tol(a: pd.DataFrame, b: pd.DataFrame, atol=0.0, rtol=1e-12) -> pd.DataFrame:
    """
    Numeric-aware comparison:
    - For numeric columns: uses np.isclose with atol/rtol.
    - For non-numeric: exact match.
    Returns a tall diff table with columns: column, index, left, right, abs_diff.
    """
    diffs = []
    for col in a.columns:
        s1, s2 = a[col], b[col]
        # Align types for comparison
        if pd.api.types.is_numeric_dtype(s1) and pd.api.types.is_numeric_dtype(s2):
            # Treat NaNs as equal if both NaN
            both_nan = s1.isna() & s2.isna()
            close = np.isclose(s1.fillna(0), s2.fillna(0), rtol=rtol, atol=atol)
            mask = ~(close | both_nan)
        else:
            mask = ~(s1.eq(s2) | (s1.isna() & s2.isna()))

        if mask.any():
            sub = pd.DataFrame({
                "column": col,
                "index": a.index[mask],
                "left": s1[mask],
                "right": s2[mask],
            })
            # Add absolute difference for numerics (NaN for non-numeric)
            with np.errstate(invalid="ignore"):
                sub["abs_diff"] = (sub["left"] - sub["right"]).abs() if pd.api.types.is_numeric_dtype(s1) and pd.api.types.is_numeric_dtype(s2) else np.nan
            diffs.append(sub)

    if diffs:
        out = pd.concat(diffs, ignore_index=True)
        # optional: sort by column then index
        return out.sort_values(["column", "index"], kind="stable").reset_index(drop=True)
    return pd.DataFrame()

def diff_summary(diff_df: pd.DataFrame) -> pd.DataFrame:
    """Compact per-column counts and (for numeric) max abs diff."""
    if diff_df.empty:
        return pd.DataFrame({"column": [], "count": [], "max_abs_diff": []})
    grp = diff_df.groupby("column", as_index=False).agg(
        count=("column", "size"),
        max_abs_diff=("abs_diff", "max"),
    )
    return grp.sort_values("count", ascending=False, kind="stable").reset_index(drop=True)


df = load_csv(original_filepath)
df_compare = load_csv(comparison_filepath)

a, b = align_frames(df, df_compare, key=None)

exact_diff = compare_exact(a,b)

if exact_diff.empty:
    print("The dataframes are exactly the same.")
else:
    print("Differences found between dataframes:")
    print(exact_diff)

numeric_diff = compare_numeric_with_tol(a, b, atol=1e-6, rtol=1e-12)
if numeric_diff.empty:
    print("No differences within tolerance ✅")
else:
    print("Differences beyond tolerance:")
    print(diff_summary(numeric_diff)) 



