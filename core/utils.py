import logging.config
import re
import json
import yaml
from pathlib import Path
from configparser import ConfigParser
from typing import List, Tuple, Dict
from flask import jsonify, Response
import nibabel as nib
from nibabel.spatialimages import SpatialImage

with open(file="logging.yaml", mode="r") as f:
    config = yaml.safe_load(stream=f)
    logging.config.dictConfig(config)

# Create the app logger
logger = logging.getLogger('simpleLogger')

# Read configuration for base data path
config = ConfigParser()
config.read(filenames="./config.ini")
BASE_DATA_PATH = Path(config.get(section="DATA", option="real_data"))


def add_dcm_extension(filename: str) -> str:
    """
    Append '.dcm' to the filename if it does not already end with it.

    This function ensures that the provided filename has the proper DICOM extension,
    checking the extension in a case-insensitive manner.

    Args:
        filename (str): The original filename.

    Returns:
        str: The filename ending with '.dcm'.
    """
    return filename if filename.lower().endswith(".dcm") else f"{filename}.dcm"


def get_folder_names(directory: Path) -> List[str]:
    """
    Retrieve the names of all subdirectories within a given directory.

    Iterates over the contents of the directory and returns a list containing the names
    of items that are directories.

    Args:
        directory (Path): The directory to search within.

    Returns:
        List[str]: A list of folder names.
    """
    if not directory.exists():
        return []
    return [p.name for p in directory.iterdir() if p.is_dir()]


def list_folder_subfolders(directory_path: Path) -> List[Tuple[str, str]]:
    """
    List each folder within the directory along with its immediate subfolders.

    For every folder found in the provided directory, the function returns tuples where the first
    element is the folder name and the second element is the name of one of its subfolders.

    Args:
        directory_path (Path): The directory to search within.

    Returns:
        List[Tuple[str, str]]: A list of tuples in the format (folder_name, subfolder_name).
    """
    folder_subfolder_pairs = []

    for folder in sorted(directory_path.iterdir()):
        if folder.is_dir():
            subfolders = [subfolder.name for subfolder in sorted(folder.iterdir()) if subfolder.is_dir()]
            for subfolder in subfolders:
                folder_subfolder_pairs.append((folder.name, subfolder))

    return folder_subfolder_pairs


def sanitize_name(name: str) -> str:
    """
    Sanitize an input string to prevent path traversal and remove unsafe characters.

    The function allows only alphanumeric characters, underscores, and dashes.

    Args:
        name (str): The original name string.

    Returns:
        str: The sanitized name.
    """
    return re.sub(r'[^A-Za-z0-9_-]', '', name)


def create_folders(base_path: Path) -> Dict[str, Path]:
    """
    Create necessary processing folders and return a mapping of folder names to their paths.

    This function creates a set of predefined folders (e.g., DICOM, NIFTI, FREESURFER, etc.)
    under the given base directory. If a folder already exists, it is left intact.

    Args:
        base_path (Path): The root directory where folders will be created.

    Returns:
        Dict[str, Path]: A dictionary mapping folder names to their corresponding Path objects.
    """
    folders = {
        "dicom": base_path / "DICOM",
        "nifti": base_path / "NIFTI",
        "freesurfer": base_path / "FREESURFER",
        "samseg": base_path / "SAMSEG",
        "workflows": base_path / "WORKFLOWS",
        "json": base_path / "JSON",
        "viewer": base_path / "VIEWER",
    }
    for folder in folders.values():
        folder.mkdir(parents=True, exist_ok=True)
    return folders


def get_nifti_dimensions(file_path: Path) -> Tuple[int, ...]:
    """
    Return the dimensions (shape) of a NIfTI file.

    Uses nibabel to load the file and extract its shape. Raises a FileNotFoundError if the file
    does not exist.

    Args:
        file_path (Path): The path to the NIfTI file.

    Returns:
        Tuple[int, ...]: The dimensions of the NIfTI image.

    Raises:
        FileNotFoundError: If the specified file does not exist.
    """
    if not file_path.exists():
        raise FileNotFoundError(f"NIfTI file not found: {file_path}")
    nifti_image = nib.load(file_path)
    if not isinstance(nifti_image, SpatialImage):
        raise TypeError(f"Expected SpatialImage, got {type(nifti_image)}")
    return nifti_image.shape


def remove_double_extension(file: Path) -> str:
    """
    Remove the double extension from a NIfTI file (e.g. '.nii.gz') and return its base name.

    If the filename ends with '.nii.gz', the function strips this extension.
    Otherwise, it returns the stem of the file.

    Args:
        file (Path): The path to the NIfTI file.

    Returns:
        str: The base name of the file without the double extension.
    """
    name = file.name
    if name.endswith(".nii.gz"):
        return name[:-7]
    return file.stem


def read_json_file(json_path: Path) -> Dict:
    """
    Read a JSON file from disk.

    Parameters
    ----------
    json_path : Path
        Path to a ``.json`` file.

    Returns
    -------
    dict
        Parsed JSON object on success. Returns an empty dict if the file
        does not exist or cannot be read.

    Notes
    -----
    Missing files are logged at exception level for visibility, but the
    function handles the error by returning ``{}``.
    """
    try:
        with json_path.open("r") as f:
            return json.load(f)
    except FileNotFoundError:
        logger.exception(f"JSON file not found: {json_path}")
        return {}


def serve_json(patient: str, study: str, filename: str, err_msg: str) -> Tuple[Response, int]:
    """
     Serve a JSON file for a given patient/study or return an error payload.

     The path is constructed as:
     ``<BASE_DATA_PATH>/<patient>/<study>/JSON/<filename>`` with
     patient and study sanitized via :func:`sanitize_name`.

     Parameters
     ----------
     patient : str
         Patient identifier (unsanitized; will be sanitized internally).
     study : str
         Study identifier (unsanitized; will be sanitized internally).
     filename : str
         JSON filename to serve (e.g., ``"cortical.json"``).
     err_msg : str
         Error message to include in the 404 response payload when the file
         is missing or unreadable.

     Returns
     -------
     (flask.Response, int)
         ``(jsonify(payload), status_code)`` where status is 200 on success
         and 404 otherwise.

     Notes
     -----
     Uses :func:`read_json_file` which returns an empty dict if the file cannot
     be read. An empty result is treated as missing and yields a 404.
     """
    path = BASE_DATA_PATH / sanitize_name(patient) / sanitize_name(study) / "JSON" / filename
    data = read_json_file(path)
    if data:
        return jsonify(data), 200
    return jsonify({"error": err_msg}), 404
