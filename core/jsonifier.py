import json
import logging
import pathlib
from collections import defaultdict
from typing import Any, Dict, List, Optional, Union
import pandas as pd

logger = logging.getLogger(__name__)


def get_volume(name: str, nuclei: List[Dict[str, float]]) -> Optional[float]:
    """
    Retrieve the volume associated with a specific nucleus name from a list of dictionaries.

    The function iterates over the list of dictionaries (each representing a nucleus and its volume)
    and returns the volume corresponding to the provided name if found.

    Args:
        name (str): The nucleus name to look for.
        nuclei (List[Dict[str, float]]): A list of dictionaries where keys are nucleus names and
            values are their respective volumes.

    Returns:
        Optional[float]: The volume of the specified nucleus if present, otherwise None.
    """
    return next((entry[name] for entry in nuclei if name in entry), None)


def read_volume_file(file_path: pathlib.Path) -> List[List[str]]:
    """
    Read a text file containing volume data and return a list of tokenized rows.

    This function opens the file at the given path, reads its contents, and splits each non-empty
    line into a list of strings (tokens). Lines that are empty or only contain whitespace are skipped.

    Args:
        file_path (pathlib.Path): The path to the volume file.

    Returns:
        List[List[str]]: A list where each element is a list of tokens from a non-empty line.

    Raises:
        FileNotFoundError: If the file does not exist.
    """
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    lines = file_path.read_text().splitlines()
    return [line.strip().split() for line in lines if line.strip()]


def read_volume_file_skip(file_path: pathlib.Path, skip: int = 0) -> List[List[str]]:
    """
    Read a text file containing volume data and skip a specified number of header lines.

    The function reads the file, tokenizes each non-empty line, and returns the data after skipping
    the first `skip` lines.

    Args:
        file_path (pathlib.Path): The path to the volume file.
        skip (int, optional): The number of initial lines to skip. Defaults to 0.

    Returns:
        List[List[str]]: A list of tokenized rows from the file after skipping the header.
    """
    data = read_volume_file(file_path)
    return data[skip:]


def process_paired_volumes(left_file: pathlib.Path, right_file: pathlib.Path) -> List[Dict[str, Union[str, float]]]:
    """
    Process two volume files containing paired data (e.g., left and right hemisphere volumes).

    The function reads two files (one for each side), extracts the structure name and corresponding
    volume values, and rounds the volume values to two decimal places. If a row cannot be processed
    due to missing or invalid data, it logs a warning and skips that row.

    Args:
        left_file (pathlib.Path): Path to the file with left hemisphere volume data.
        right_file (pathlib.Path): Path to the file with right hemisphere volume data.

    Returns:
        List[Dict[str, Union[str, float]]]: A list of dictionaries, each containing the structure name
        and its left and right volumes.
    """
    left_data = read_volume_file(left_file)
    right_data = read_volume_file(right_file)
    volumes = []
    for idx, (left_row, right_row) in enumerate(zip(left_data, right_data), start=1):
        try:
            structure = left_row[0]
            lhs_volume = round(float(left_row[1]), 2)
            rhs_volume = round(float(right_row[1]), 2)
            volumes.append({
                "Structure": structure,
                "LHS Volume (mm3)": lhs_volume,
                "RHS Volume (mm3)": rhs_volume
            })
        except (IndexError, ValueError) as e:
            logger.warning(f"Row {idx}: Skipping row due to error: {e}")
    return volumes


def process_hippocampus(mri: pathlib.Path) -> List[Dict[str, Union[str, float]]]:
    """
    Process hippocampus volume data from MRI files.

    This function reads volume data from specific files corresponding to left and right hippocampus
    volumes and processes them using paired volume processing.

    Args:
        mri (pathlib.Path): The directory containing MRI data files.

    Returns:
        List[Dict[str, Union[str, float]]]: A list of dictionaries with hippocampus volume data for
        both hemispheres.
    """
    return process_paired_volumes(mri / "lh.hippoSfVolumes.txt", mri / "rh.hippoSfVolumes.txt")


def process_amygdala(mri: pathlib.Path) -> List[Dict[str, Union[str, float]]]:
    """
    Process amygdala volume data from MRI files.

    Reads left and right amygdala volume files and returns a paired volume result.

    Args:
        mri (pathlib.Path): The directory containing MRI data files.

    Returns:
        List[Dict[str, Union[str, float]]]: A list of dictionaries with amygdala volume data for
        both hemispheres.
    """
    return process_paired_volumes(mri / "lh.amygNucVolumes.txt", mri / "rh.amygNucVolumes.txt")


def process_brain_stem(mri: pathlib.Path) -> List[Dict[str, Union[str, float]]]:
    """
    Process brain stem volume data from an MRI file.

    The function reads a file containing brain stem volumes and converts the volume values
    to a rounded float (two decimal places). If a row is malformed or contains invalid data,
    a warning is logged.

    Args:
        mri (pathlib.Path): The directory containing MRI data files.

    Returns:
        List[Dict[str, Union[str, float]]]: A list of dictionaries with brain stem volume data.
    """
    data = read_volume_file(mri / "brainstemSsLabels.volumes.txt")
    volumes = []
    for idx, row in enumerate(data, start=1):
        try:
            volumes.append({
                "Structure": row[0],
                "Volume (mm3)": round(float(row[1]), 2)
            })
        except (IndexError, ValueError) as e:
            logger.warning(f"Brain stem row {idx} error with row {row}: {e}")
    return volumes


def process_thalamus(mri: pathlib.Path) -> List[Dict[str, Union[str, float]]]:
    """
    Process thalamic nuclei volumes from an MRI file.

    Reads the thalamic nuclei volume file, separates left and right data, and then pairs
    them based on the structure name. Volume values are rounded to two decimal places.

    Args:
        mri (pathlib.Path): The directory containing MRI data files.

    Returns:
        List[Dict[str, Union[str, float]]]: A list of dictionaries where each dictionary
        contains a structure name with its corresponding left and right volumes.
    """
    data = read_volume_file(mri / "ThalamicNuclei.volumes.txt")
    lhs_nuclei = []
    rhs_nuclei = []
    structure_names = []
    for idx, row in enumerate(data, start=1):
        try:
            if "Left" in row[0]:
                name = row[0].replace("Left-", "")
                lhs_nuclei.append({name: round(float(row[1]), 2)})
                structure_names.append(name)
            elif "Right" in row[0]:
                name = row[0].replace("Right-", "")
                rhs_nuclei.append({name: round(float(row[1]), 2)})
        except (IndexError, ValueError) as e:
            logger.warning(f"Thalamus row {idx} error with row {row}: {e}")
    return [{
        "Structure": name,
        "LHS Volume (mm3)": get_volume(name, lhs_nuclei),
        "RHS Volume (mm3)": get_volume(name, rhs_nuclei),
    } for name in structure_names]


def process_hypothalamus(mri: pathlib.Path) -> List[Dict[str, Union[str, float]]]:
    """
    Process FreeSurfer-derived hypothalamus volumes from a CSV file.

    The CSV file is expected to have one record where keys correspond to left/right subunit names.
    The function renames keys for consistency, separates left and right volumes, and then pairs them.

    Args:
        mri (pathlib.Path): The directory containing the MRI CSV file.

    Returns:
        List[Dict[str, Union[str, float]]]: A list of dictionaries where each dictionary contains
        a structure name and its left and right volumes.
    """
    df = pd.read_csv(mri / "hypothalamic_subunits_volumes.v1.csv")
    records = df.to_dict(orient="records")[0]
    records.pop("subject", None)

    # Rename keys for consistency
    records["left whole"] = records.pop("whole left")
    records["right whole"] = records.pop("whole right")

    lhs = [{k.replace("left ", ""): v} for k, v in records.items() if "left" in k]
    rhs = [{k.replace("right ", ""): v} for k, v in records.items() if "right" in k]
    names = [k.replace("left ", "") for k in records if "left" in k]

    return [{
        "Structure": name,
        "LHS Volume (mm3)": get_volume(name, lhs),
        "RHS Volume (mm3)": get_volume(name, rhs),
    } for name in names]


def get_subcortical(freesurfer_path: pathlib.Path) -> Dict[str, Any]:
    """
    Extract subcortical volume data from multiple MRI-derived files.

    Processes and consolidates volume data for several subcortical structures (hippocampus,
    thalamus, amygdala, brain stem, and hypothalamus) using their respective processing functions.

    Args:
        freesurfer_path (pathlib.Path): The root directory of FreeSurfer MRI data (typically containing a "mri" subfolder).

    Returns:
        Dict[str, Any]: A dictionary with keys corresponding to subcortical structure types and values
        as lists of volume dictionaries.
    """
    return {
        "hippocampus": process_hippocampus(mri=freesurfer_path),
        "thalamus": process_thalamus(mri=freesurfer_path),
        "amygdala": process_amygdala(mri=freesurfer_path),
        "brain_stem": process_brain_stem(mri=freesurfer_path),
        "hypothalamus": process_hypothalamus(mri=freesurfer_path)
    }


def get_lesions(fs_stats: pathlib.Path, samseg_path: pathlib.Path) -> List[Dict[str, Union[str, float]]]:
    """
    Extract lesion and hypointensity volume data from MRI statistics files.

    Reads two files: one containing hypointensities from FreeSurfer statistics and one containing
    lesion data from Samseg statistics. It filters rows based on keywords in the structure name.

    Args:
        fs_stats (pathlib.Path): The path to the FreeSurfer stats directory.
        samseg_path (pathlib.Path): The path to the Samseg stats directory.

    Returns:
        List[Dict[str, Union[str, float]]]: A combined list of dictionaries containing both hypointensity
        and lesion data.
    """
    hypointensities = [
        {"Structure": row[4], "Volume (mm3)": float(row[3])}
        for row in read_volume_file_skip(fs_stats / "aseg.stats", skip=80)
        if "hypointensities" in row[4]
    ]
    lesions = [
        {"Structure": row[2].replace(",", ""), "Volume (mm3)": float(row[3].replace(",", ""))}
        for row in read_volume_file(samseg_path / "samseg.stats")
        if "Lesions" in row[2]
    ]
    return hypointensities + lesions


def get_brainvol(stats: pathlib.Path) -> List[Dict[str, str | int]]:
    """
    Process overall brain volume data from a stats file.

    Reads the brain volume file, extracts the structure name and volume, converts the volume to an
    integer (after converting from float), and removes any commas from the structure name. Malformed
    rows are logged and skipped.

    Args:
        stats (pathlib.Path): The path to the statistics directory containing "brainvol.stats".

    Returns:
        List[Dict[str, Union[str, int]]]: A list of dictionaries, each containing a structure name and its volume.
    """
    brainvol = []
    for idx, row in enumerate(read_volume_file(stats / "brainvol.stats"), start=1):
        try:
            brainvol.append({
                "Structure": row[2].replace(",", ""),
                "Volume (mm3)": int(float(row[-2][:-1]))
            })
        except (IndexError, ValueError) as e:
            logger.warning(f"Brainvol row {idx} error with row {row}: {e}")
    return brainvol


def get_white_matter(stats: pathlib.Path) -> List[Dict[str, str | float | None]]:
    """
    Extract white matter volume data from a stats file.

    Reads the white matter statistics file (skipping header lines), filters for left and right
    hemisphere data, and then pairs the volumes based on the structure name.

    Args:
        stats (pathlib.Path): The path to the statistics directory containing "wmparc.stats".

    Returns:
        List[Dict[str, Union[str, float, None]]]: A list of dictionaries where each dictionary
        includes a structure name and its left and right white matter volumes.
    """
    wm_data = read_volume_file_skip(stats / "wmparc.stats", skip=66)
    wm_vol_lhs = [
        {row[4].replace("wm-lh-", ""): float(row[3])}
        for row in wm_data if "wm-lh" in row[4]
    ]
    wm_vol_rhs = [
        {row[4].replace("wm-rh-", ""): float(row[3])}
        for row in wm_data if "wm-rh" in row[4]
    ]
    names = [row[4].replace("wm-lh-", "") for row in wm_data if "wm-lh" in row[4]]
    wm_vols = [{
        "Structure": name,
        "LHS Volume (mm3)": get_volume(name, wm_vol_lhs),
        "RHS Volume (mm3)": get_volume(name, wm_vol_rhs)
    } for name in names]

    return wm_vols


def parse_dkt(file: pathlib.Path) -> List[Dict[str, Union[str, float]]]:
    """
    Parse DKT atlas statistics from a file.

    Reads a DKT atlas stats file (skipping header lines) and extracts key metrics for each brain structure,
    such as surface area, gray matter volume, average thickness, and mean curvature.
    Malformed rows are logged and skipped.

    Args:
        file (pathlib.Path): The path to the DKT atlas stats file.

    Returns:
        List[Dict[str, Union[str, float]]]: A list of dictionaries with the extracted metrics for each structure.
    """
    entries = []
    for id_row, fields in enumerate(read_volume_file_skip(file, skip=61), start=1):
        try:
            entries.append({
                "Structure": fields[0],
                "Surface Area (mm2)": int(fields[2]),
                "Gray Matter Vol (mm3)": int(fields[3]),
                "Thickness Avg (mm)": float(fields[4]),
                "Mean Curvature (mm-1)": float(fields[6])
            })
        except (IndexError, ValueError) as err:
            logger.warning(f"DKT row {id_row} error with fields {fields}: {err}")
    return entries


def get_cortical(stats: pathlib.Path) -> Dict[str, List]:
    """
    Consolidate cortical volume and surface statistics from multiple files.

    Processes brain volumes, white matter, and DKT atlas data (for both left and right hemispheres)
    from the provided statistics directory.

    Args:
        stats (pathlib.Path): The path to the statistics directory containing the required files.

    Returns:
        Dict[str, List]: A dictionary with keys 'brain', 'whitematter', 'lh_dkatlas', and 'rh_dkatlas'
        corresponding to their respective data lists.
    """
    return {
        "brain": get_brainvol(stats=stats),
        "whitematter": get_white_matter(stats=stats),
        "lh_dkatlas": parse_dkt(stats / "lh.aparc.DKTatlas.stats"),
        "rh_dkatlas": parse_dkt(stats / "rh.aparc.DKTatlas.stats")
    }


def get_general(stats: pathlib.Path, samseg_path: pathlib.Path) -> Dict[str, Any]:
    """
    Extract general subcortical volume and lesion information.

    Processes ASEG volume data from FreeSurfer (excluding hypointensities) and combines it with
    lesion data extracted from both FreeSurfer and Samseg files.

    Args:
        stats (pathlib.Path): The path to the FreeSurfer statistics directory.
        samseg_path (pathlib.Path): The path to the Samseg statistics directory.

    Returns:
        Dict[str, Any]: A dictionary with keys 'aseg' (for subcortical volumes) and 'lesions' (for lesion data).
    """
    aseg = [
        {"Structure": row[4], "Volume (mm3)": float(row[3])}
        for row in read_volume_file_skip(stats / "aseg.stats", skip=80)
        if "hypointensities" not in row[4]
    ]
    lesions = get_lesions(fs_stats=stats, samseg_path=samseg_path)
    return {"aseg": aseg, "lesions": lesions}


def run_jsonifier(
    freesurfer_path: pathlib.Path,
    samseg_path: pathlib.Path,
    output_folder: pathlib.Path
) -> None:
    """
    Generate JSON files for subcortical, cortical, and general volume data.

    Processes MRI data using various helper functions, then writes the results as JSON files
    (subcortical.json, cortical.json, general.json) to the specified output folder. Creates the output
    folder if it does not already exist.

    Args:
        freesurfer_path (pathlib.Path): The root directory for FreeSurfer MRI data.
        samseg_path (pathlib.Path): The directory containing Samseg statistics.
        output_folder (pathlib.Path): The directory where the JSON files will be written.

    Returns:
        None
    """
    output_folder.mkdir(parents=True, exist_ok=True)
    subcortical = get_subcortical(freesurfer_path=freesurfer_path / "mri")
    cortical = get_cortical(stats=freesurfer_path / "stats")
    general = get_general(stats=freesurfer_path / "stats", samseg_path=samseg_path)

    for fname, data in [
        ("subcortical.json", subcortical),
        ("cortical.json", cortical),
        ("general.json", general)
    ]:
        out_file = output_folder / fname
        try:
            with out_file.open("w") as f:
                # noinspection PyTypeChecker
                json.dump(data, f, indent=4)
            logger.info(f"Wrote {fname} to {out_file}")
        except Exception as e:
            logger.error(f"Error writing {fname}: {e}")


def run_json_average(json_path: pathlib.Path, folders: List[str], main_type: str) -> None:
    """
    Average numerical volume values across JSON files from multiple folders.

    For each JSON file located under the provided folder names, this function accumulates numerical
    volume data and computes an average per structure. The averaged results are written to an "AVERAGES"
    subfolder under the given json_path.

    Args:
        json_path (pathlib.Path): The base directory containing the JSON files.
        folders (List[str]): A list of folder names that each contain a JSON file of type `main_type`.
        main_type (str): The filename (within each folder) to process (e.g., "subcortical.json").

    Returns:
        None
    """
    cumulative_data: Dict[str, Dict[str, defaultdict]] = {}
    counts: Dict[str, Dict[str, int]] = {}
    json_files = [json_path / folder / main_type for folder in folders]

    for path in json_files:
        if not path.exists():
            logger.warning(f"File not found: {path}")
            continue
        try:
            with path.open("r") as f:
                data = json.load(f)
        except json.JSONDecodeError as e:
            logger.warning(f"JSON decode error in file {path}: {e}")
            continue

        for top_key, entries in data.items():
            if top_key not in cumulative_data:
                cumulative_data[top_key] = {}
                counts[top_key] = {}
            for entry in entries:
                structure = entry.get("Structure")
                if not structure:
                    logger.warning(f"Missing 'Structure' in entry {entry} in file {path}")
                    continue
                if structure not in cumulative_data[top_key]:
                    cumulative_data[top_key][structure] = defaultdict(float)
                    counts[top_key][structure] = 0
                counts[top_key][structure] += 1
                for key, value in entry.items():
                    if key == "Structure":
                        continue
                    if isinstance(value, (int, float)):
                        cumulative_data[top_key][structure][key] += value
                    else:
                        logger.warning(f"Non-numeric value for key '{key}' in entry {entry} in file {path}")

    averaged_result = {}
    for top_key, structures in cumulative_data.items():
        averaged_result[top_key] = []
        for structure, totals in structures.items():
            count = counts[top_key][structure]
            if count == 0:
                logger.warning(f"No data to average for '{structure}' under '{top_key}'")
                continue
            averaged_entry = {"Structure": structure}
            for key, total in totals.items():
                averaged_entry[key] = round(total / count, 2)
            averaged_result[top_key].append(averaged_entry)
        averaged_result[top_key].sort(key=lambda x: x["Structure"])

    averages_folder = json_path / "AVERAGES"
    averages_folder.mkdir(parents=True, exist_ok=True)
    output_file = averages_folder / main_type
    try:
        with output_file.open("w") as f:
            # noinspection PyTypeChecker
            json.dump(averaged_result, f, indent=4)
        logger.info(f"Averaged data written to {output_file}")
    except Exception as e:
        logger.error(f"Error writing to file {output_file}: {e}")


def run_global_json(json_path: pathlib.Path, folders: List[str]) -> None:
    """
    Consolidate individual JSON files from multiple folders and include averaged data.

    The function reads subcortical, cortical, and general JSON files from each specified folder.
    It then adds the corresponding averaged data (from the AVERAGES subfolder) and writes the global
    JSON files to the base json_path.

    Args:
        json_path (pathlib.Path): The base directory containing individual JSON files and the AVERAGES folder.
        folders (List[str]): A list of folder names to process.

    Returns:
        None
    """
    global_subcortical = {}
    global_cortical = {}
    global_general = {}

    for folder in folders:
        try:
            with (json_path / folder / "subcortical.json").open("r") as f:
                global_subcortical[folder] = json.load(f)
            with (json_path / folder / "cortical.json").open("r") as f:
                global_cortical[folder] = json.load(f)
            with (json_path / folder / "general.json").open("r") as f:
                global_general[folder] = json.load(f)
        except Exception as e:
            logger.error(f"Error reading JSON files from folder {folder}: {e}")

    try:
        with (json_path / "AVERAGES" / "subcortical.json").open("r") as f:
            global_subcortical["AVERAGES"] = json.load(f)
        with (json_path / "AVERAGES" / "cortical.json").open("r") as f:
            global_cortical["AVERAGES"] = json.load(f)
        with (json_path / "AVERAGES" / "general.json").open("r") as f:
            global_general["AVERAGES"] = json.load(f)
    except Exception as e:
        logger.error(f"Error reading AVERAGES JSON files: {e}")

    for fname, data in [
        ("subcortical.json", global_subcortical),
        ("cortical.json", global_cortical),
        ("general.json", global_general)
    ]:
        try:
            with (json_path / fname).open("w") as f:
                # noinspection PyTypeChecker
                json.dump(data, f, indent=4)
            logger.info(f"Wrote global JSON to {json_path / fname}")
        except Exception as e:
            logger.error(f"Error writing global JSON file {fname}: {e}")
