import json
import logging
import pathlib
from collections import defaultdict
from typing import Any, Dict, List, Optional, Union
import pandas as pd


logger = logging.getLogger(__name__)


def get_volume(name: str, nuclei: List[Dict[str, float]]) -> Optional[float]:
    """
    Retrieve the volume for a given nucleus name from a list of dictionaries.
    """
    return next((entry[name] for entry in nuclei if name in entry), None)


def read_volume_file(file_path: pathlib.Path) -> List[List[str]]:
    """
    Reads a text file and returns a list of split rows.
    Skips empty lines.
    """
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    lines = file_path.read_text().splitlines()
    return [line.strip().split() for line in lines if line.strip()]


def read_volume_file_skip(file_path: pathlib.Path, skip: int = 0) -> List[List[str]]:
    """
    Reads a file and skips the first `skip` lines.
    """
    data = read_volume_file(file_path)
    return data[skip:]


def process_paired_volumes(left_file: pathlib.Path, right_file: pathlib.Path) -> List[Dict[str, Union[str, float]]]:
    """
    Processes two files containing paired volume information.
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
    Process hippocampus volumes from MRI files.
    """
    return process_paired_volumes(mri / "lh.hippoSfVolumes.txt", mri / "rh.hippoSfVolumes.txt")


def process_amygdala(mri: pathlib.Path) -> List[Dict[str, Union[str, float]]]:
    """
    Process amygdala volumes from MRI files.
    """
    return process_paired_volumes(mri / "lh.amygNucVolumes.txt", mri / "rh.amygNucVolumes.txt")


def process_brain_stem(mri: pathlib.Path) -> List[Dict[str, Union[str, float]]]:
    """
    Process brain stem volumes from MRI files.
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
    Process thalamic nuclei volumes from MRI files.
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


def process_hypothalamus_v1(mri: pathlib.Path) -> List[Dict[str, Union[str, float]]]:
    """
    Process FreeSurfer hypothalamus volumes from a CSV file.
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


def process_hypothalamus_v2(path: pathlib.Path) -> List[Dict[str, Union[str, float]]]:
    """
    Process FastSurfer hypothalamus MRI data from a stats file.
    """
    lines = read_volume_file_skip(path, skip=55)
    volumes = []
    for idx, row in enumerate(lines, start=1):
        if len(row) < 5:
            logger.warning(f"Hypothalamus row {idx} skipped: insufficient columns.")
            continue
        try:
            volume = float(row[3])
            name = row[4]
            if name.startswith("L-"):
                name = "Left" + name[2:]
            elif name.startswith("R-"):
                name = "Right" + name[2:]
            volumes.append({"Structure": name, "Volume (mm3)": round(volume, 2)})
        except ValueError as e:
            logger.warning(f"Hypothalamus row {idx} error with row {row}: {e}")
    return volumes


def process_cerebellum(file_path: pathlib.Path) -> List[Dict[str, Union[str, float]]]:
    """
    Process cerebellum volumes from a stats file.
    """
    lines = read_volume_file_skip(file_path, skip=55)
    volumes = []
    for idx, row in enumerate(lines, start=1):
        if len(row) < 5:
            logger.warning(f"Cerebellum row {idx} skipped: insufficient columns.")
            continue
        try:
            volume = float(row[3])
            name = row[4]
            volumes.append({"Structure": name, "Volume (mm3)": round(volume, 2)})
        except ValueError as e:
            logger.warning(f"Cerebellum row {idx} error with row {row}: {e}")
    return volumes


def get_subcortical(freesurfer_path: pathlib.Path) -> Dict[str, Any]:
    """
    Extract subcortical volumes from various MRI data files.
    """
    return {
        "hippocampus": process_hippocampus(freesurfer_path),
        "thalamus": process_thalamus(freesurfer_path),
        "amygdala": process_amygdala(freesurfer_path),
        "brain_stem": process_brain_stem(freesurfer_path),
        "hypothalamus": process_hypothalamus_v1(freesurfer_path)
    }


def get_lesions(fs_stats: pathlib.Path, samseg_path: pathlib.Path) -> List[Dict[str, Union[str, float]]]:
    """
    Extract lesions and hypointensities from MRI stats files.
    """
    hypointensities = [
        {"Structure": row[4], "Volume (mm3)": float(row[3])}
        for row in read_volume_file_skip(fs_stats / "aseg.stats", skip=80)
        if "hypointensities" in row[4]
    ]
    lesions = [
        {"Structure": row[4], "Volume (mm3)": float(row[3])}
        for row in read_volume_file(samseg_path / "samseg.fs.stats")
        if "Lesions" in row[4]
    ]
    return hypointensities + lesions


def get_brainvol(stats: pathlib.Path) -> List[Dict[str, str | int]]:
    """
    Process brain, white matter, and cortical parcellation volumes.
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

    return {
        "brain": get_brainvol(stats=stats),
        "whitematter": get_white_matter(stats=stats),
        "lh_dkatlas": parse_dkt(stats / "lh.aparc.DKTatlas.stats"),
        "rh_dkatlas": parse_dkt(stats / "rh.aparc.DKTatlas.stats")
    }


def get_general(stats: pathlib.Path, samseg_path: pathlib.Path) -> Dict[str, Any]:
    """
    Extract general subcortical volumes (ASEG) and lesion information.
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
    Generate JSON files for subcortical, cortical, and general volumes.
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
                json.dump(data, f, indent=4)
            logger.info(f"Wrote {fname} to {out_file}")
        except Exception as e:
            logger.error(f"Error writing {fname}: {e}")


def run_json_average(json_path: pathlib.Path, folders: List[str], main_type: str) -> None:
    """
    Averages the numerical values in JSON files across multiple folders.
    The result is written to an "AVERAGES" subfolder.
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
            json.dump(averaged_result, f, indent=4)
        logger.info(f"Averaged data written to {output_file}")
    except Exception as e:
        logger.error(f"Error writing to file {output_file}: {e}")


def run_global_json(json_path: pathlib.Path, folders: List[str]) -> None:
    """
    Consolidate individual JSON files from each folder and add the averaged data.
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
                json.dump(data, f, indent=4)
            logger.info(f"Wrote global JSON to {json_path / fname}")
        except Exception as e:
            logger.error(f"Error writing global JSON file {fname}: {e}")
