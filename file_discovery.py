"""Utilities for discovering and categorizing raw data files in a directory."""

from __future__ import annotations

from pathlib import Path
from typing import NamedTuple


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
