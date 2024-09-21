"""
Setup the project environment for the data pipeline. This includes creating a project
directory, managing the data directory, and setting up the project configuration.
"""

import os
import sys
import logging
from pathlib import Path
import h5py


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler(sys.stdout))


def make_dir(str: str) -> None:
    """
    Create a directory if it does not exist.

    Args:
        str (str): The path to the directory to create.
    """
    Path(str).mkdir(parents=True, exist_ok=True)


def fetch_files(pattern: str, directory: str) -> list[Path]:
    """
    Function to return all files found in a given directory

    Args:
        pattern (str): Identify the proper files to grab
        directory (str): Points to the location of the directory

    Returns:
        List[str]: Sorted list of file paths as strings
    """
    files = sorted(Path(directory).glob(pattern))

    logger.debug("Found %s files to import in directory %s.", len(files), directory)

    return files

def open_hdf5(file_path: Path, *args) -> h5py.File:
    """
    Open an HDF5 file in a given mode.

    Args:
        file_path (Path): The path to the HDF5 file.
        mode (str): The mode to open the file in.

    Returns:
        h5py.File: The opened HDF5 file.
    """
    with h5py.File(file_path, "r") as f:
        keys = args if args else f.keys()

        # check for missing keys
        missing_keys = set(keys) - set(f.keys())
        if missing_keys:
            raise ValueError(f"Keys {missing_keys} not found in file {file_path}")

        return {key: f[key][:] for key in keys}

def make_h5(file: str) -> h5py.File:
    """
    Open or create an HDF5 file.

    Args:
        files (str): The path to the HDF5 file.

    Returns:
        h5py.File: The opened or created HDF5 file.
    """
    mode = "a" if Path(file).exists() else "w"
    return h5py.File(file, mode)

def get_h5_object(file: str, method) -> h5py.File:
    """
    Open or create an HDF5 file.

    Args:
        files (str): The path to the HDF5 file.

    Returns:
        h5py.File: The opened or created HDF5 file.
    """
    h5 = make_h5(file)
    return h5.require_group(method)

def save_h5(h5: h5py.File, col, data, compression="gzip") -> None:
    """
    Save an HDF5 file.

    Args:
        h5 (h5py.File): The HDF5 file to save.
        file (str): The path to save the HDF5 file to.
    """
    if col in h5:
        del h5[col]
    return h5.create_dataset(col, data=data, compression=compression)

