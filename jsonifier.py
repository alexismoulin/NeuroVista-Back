import pandas as pd
import json
import pathlib
from typing import Dict, List, Union, Any
from collections import defaultdict


def get_volume(name: str, nuclei: List[Dict[str, float]]) -> Union[float, None]:
    """Helper function to retrieve the volume for a given nucleus name."""
    for d in nuclei:
        if name in d:
            return d[name]
    return None


def read_volume_file(file_path: pathlib.Path) -> List[List[str]]:
    """Reads a text file and returns a list of split rows."""
    if file_path.exists():
        with open(file=file_path, mode="r") as f:
            return [row.split() for row in f.readlines()]
    else:
        raise FileNotFoundError(f"File not found: {file_path}")


def process_hippocampus(mri: pathlib.Path) -> List[Dict[str, Union[str, float]]]:
    """Process hippocampus volumes from MRI files."""
    lhs_data = read_volume_file(mri / "lh.hippoSfVolumes.txt")
    rhs_data = read_volume_file(mri / "rh.hippoSfVolumes.txt")
    
    return [{
        "Structure": field[0][0],
        "LHS Volume (mm3)": round(float(field[0][1]), 2),
        "RHS Volume (mm3)": round(float(field[1][1]), 2)
    } for field in zip(lhs_data, rhs_data)]


def process_thalamus(mri: pathlib.Path) -> List[Dict[str, Union[str, float]]]:
    """Process thalamic nuclei volumes."""
    thalamic_data = read_volume_file(mri / "ThalamicNuclei.volumes.txt")

    lhs_thalamic_nuclei = [
        {field[0].replace("Left-", ""): round(float(field[1]), 2)} for field in thalamic_data if "Left" in field[0]
    ]
    rhs_thalamic_nuclei = [
        {field[0].replace("Right-", ""): round(float(field[1]), 2)} for field in thalamic_data if "Right" in field[0]
    ]
    
    names = [field[0].replace("Left-", "") for field in thalamic_data if "Left" in field[0]]
    
    return [{
        "Structure": name,
        "LHS Volume (mm3)": get_volume(name=name, nuclei=lhs_thalamic_nuclei),
        "RHS Volume (mm3)": get_volume(name=name, nuclei=rhs_thalamic_nuclei),
    } for name in names]


def process_amygdala(mri: pathlib.Path) -> List[Dict[str, Union[str, float]]]:
    """Process amygdala volumes."""
    lhs_data = read_volume_file(mri / "lh.amygNucVolumes.txt")
    rhs_data = read_volume_file(mri / "rh.amygNucVolumes.txt")

    return [{
        "Structure": field[0][0],
        "LHS Volume (mm3)": round(float(field[0][1]), 2),
        "RHS Volume (mm3)": round(float(field[1][1]), 2)
    } for field in zip(lhs_data, rhs_data)]


def process_brain_stem(mri: pathlib.Path) -> List[Dict[str, Union[str, float]]]:
    """Process brain stem volumes."""
    brain_stem_data = read_volume_file(mri / "brainstemSsLabels.volumes.txt")
    return [{"Structure": field[0], "Volume (mm3)": round(float(field[1]), 2)} for field in brain_stem_data]


def process_hypothalamus(mri: pathlib.Path) -> List[Dict[str, Union[str, float]]]:
    """Process hypothalamus volumes."""
    hypothalamus_data = pd.read_csv(mri / "hypothalamic_subunits_volumes.v1.csv").to_dict(orient="records")[0]
    del hypothalamus_data["subject"]

    hypothalamus_data["left whole"] = hypothalamus_data.pop("whole left")
    hypothalamus_data["right whole"] = hypothalamus_data.pop("whole right")

    lhs_hypothalamus = [{key.replace("left ", ""): value} for key, value in hypothalamus_data.items() if "left" in key]
    rhs_hypothalamus = [{key.replace("right ", ""): value} for key, value in hypothalamus_data.items() if "right" in key]

    names = [key.replace("left ", "") for key in hypothalamus_data.keys() if "left" in key]

    return [{
        "name": name,
        "lhs_volume": get_volume(name=name, nuclei=lhs_hypothalamus),
        "rhs_volume": get_volume(name=name, nuclei=rhs_hypothalamus),
    } for name in names]


# FastSurfer files
def process_hypothalamus_v2(path: pathlib.Path) -> List[Dict[str, float]]:
    """Processes hypothalamus MRI data file, extracting only the name and volume accurately."""
    hypothalamus_data = read_volume_file(file_path=path)[55:]  # Skipping header

    hypothalamus = []
    for line in hypothalamus_data:
        # Ensure the line has enough columns for both name and volume
        if len(line) >= 5:
            try:
                # Parsing volume and struct name based on expected positions
                volume = float(line[3])  # Volume_mm3 column
                name = line[4]  # Start with the StructName part (ignoring additional numbers)

                # Replace prefixes "L-" and "R-" with "LHS" and "RHS" respectively
                if name.startswith("L-"):
                    name = "Left" + name[1:]
                elif name.startswith("R-"):
                    name = "Right" + name[1:]

                # Add the structured data to the result list
                hypothalamus.append({"Structure": name, "Volume (mm3)": volume})
            except ValueError:
                # Skip rows where volume is not a valid float
                continue

    return hypothalamus


def process_cerebellum(file_path: pathlib.Path) -> List[Dict[str, float]]:
    """Processes MRI data file, extracting only the name and volume accurately."""
    data = read_volume_file(file_path)[55:]  # Skipping header

    result = []
    for line in data:
        # Ensure the line has enough columns for both name and volume
        if len(line) >= 5:
            try:
                # Parsing volume and struct name based on expected positions
                volume = float(line[3])  # Volume_mm3 column
                name = line[4]  # Start with the StructName part (ignoring additional numbers)

                # Add the structured data to the result list
                result.append({"Structure": name, "Volume (mm3)": volume})
            except ValueError:
                # Skip rows where volume is not a valid float
                continue

    return result


def get_subcortical(freesurfer_path: pathlib.Path, fastsurfer_path: pathlib.Path) -> Dict[str, list]:
    """Extracts subcortical volumes."""
    subcortical = {
        "hippocampus": process_hippocampus(mri=freesurfer_path),
        "thalamus": process_thalamus(mri=freesurfer_path),
        "amygdala": process_amygdala(mri=freesurfer_path),
        "brain_stem": process_brain_stem(mri=freesurfer_path),
        "hypothalamus": process_hypothalamus_v2(path=fastsurfer_path / "hypothalamus.HypVINN.stats"),
        "cerebellum": process_cerebellum(file_path=fastsurfer_path / "cerebellum.CerebNet.stats")
    }
    return subcortical


def get_lesions(fs_stats: pathlib.Path, samseg_path: pathlib.Path) -> List[Dict]:
    hypointensities = [
        {"Structure": row[4], "Volume (mm3)": float(row[3])} 
        for row in read_volume_file(fs_stats / "aseg.stats")[79:] if "hypointensities" in row[4]
    ]
    lesions = [
        {"Structure": row[4], "Volume (mm3)": float(row[3])} 
        for row in read_volume_file(samseg_path / "samseg.fs.stats") if "Lesions" in row[4]
    ]

    return hypointensities + lesions


def get_cortical(stats: pathlib.Path, samseg_path: pathlib.Path) -> Dict[str, list]:
    """Extracts cortical volumes and parcellations."""
    # ASEG
    aseg = [
        {"Structure": row[4], "Volume (mm3)": float(row[3])} 
        for row in read_volume_file(stats / "aseg.stats")[79:]
        if "hypointensities" not in row[4]
    ]
    # Brain
    brainvol = [{"Structure": row[2].replace(",", ""), "Volume (mm3)": int(float(row[-2][:-1]))} for row in read_volume_file(stats / "brainvol.stats")]
    # Lesions
    lesions = get_lesions(fs_stats=stats, samseg_path=samseg_path)

    wm_data = read_volume_file(stats / "wmparc.stats")[65:]
    wm_vol_lhs = [{row[4].replace("wm-lh-", ""): float(row[3])} for row in wm_data if "wm-lh" in row[4]]
    wm_vol_rhs = [{row[4].replace("wm-rh-", ""): float(row[3])} for row in wm_data if "wm-rh" in row[4]]
    names = [row[4].replace("wm-lh-", "") for row in wm_data if "wm-lh" in row[4]]

    wm_vols = [{
        "Structure": name,
        "LHS Volume (mm3)": get_volume(name=name, nuclei=wm_vol_lhs),
        "RHS Volume (mm3)": get_volume(name=name, nuclei=wm_vol_rhs),
    } for name in names]

    lh_dkt_atlas = [{
        "Structure": row[0],
        "Surface Area (mm2)": int(row[2]),
        "Gray Matter Vol (mm3)": int(row[3]),
        "Thickness Avg (mm)": float(row[4]),
        "Mean Curvature (mm-1)": float(row[6])
    } for row in read_volume_file(stats / "lh.aparc.DKTatlas.stats")[61:]]

    rh_dkt_atlas = [{
        "Structure": row[0],
        "Surface Area (mm2)": int(row[2]),
        "Gray Matter Vol (mm3)": int(row[3]),
        "Thickness Avg (mm)": float(row[4]),
        "Mean Curvature (mm-1)": float(row[6])
    } for row in read_volume_file(stats / "rh.aparc.DKTatlas.stats")[61:]]

    cortical = {
        "aseg": aseg,
        "brain": brainvol,
        "whitematter": wm_vols,
        "lh_dkatlas": lh_dkt_atlas,
        "rh_dkatlas": rh_dkt_atlas,
        "lesions": lesions
    }

    return cortical


def run_jsonifier(freesurfer_path: pathlib.Path, fastsurfer_path: pathlib.Path, samseg_path: pathlib.Path, output_folder: pathlib.Path):
    """Runs the process of generating JSON files for subcortical and cortical volumes."""
    subcortical_dict = get_subcortical(freesurfer_path=freesurfer_path / "mri", fastsurfer_path=fastsurfer_path / "stats")
    cortical_dict = get_cortical(stats=freesurfer_path / "stats", samseg_path=samseg_path)

    # Write JSON objects to files
    with open(file=output_folder / "subcortical.json", mode="w") as outfile:
        json.dump(obj=subcortical_dict, fp=outfile, indent=4)

    with open(file=output_folder / "cortical.json", mode="w") as outfile:
        json.dump(obj=cortical_dict, fp=outfile, indent=4)


def run_json_average(json_path: pathlib.Path, folders: List[str], main_type: str):
    """
    Averages the numerical values in 'cortical.json' files across multiple folders.

    Parameters:
    - json_path (Path): The base path where the folders are located.
    - folders (List[str]): A list of folder names containing 'cortical.json' files.

    The function writes the averaged result to 'cortical.json' in the base path.
    """

    # Initialize dictionaries to hold the cumulative sums and counts
    cumulative_data = {}
    counts = {}
    json_paths = [json_path / f / main_type for f in folders]

    for path in json_paths:
        if not path.exists():
            print(f"Warning: File not found - {path}")
            continue
        try:
            with open(path, 'r') as file:
                data = json.load(file)
        except json.JSONDecodeError as e:
            print(f"Warning: JSON decode error in file {path}: {e}")
            continue
        except Exception as e:
            print(f"Warning: Unexpected error reading file {path}: {e}")
            continue

        for top_key, entries in data.items():
            if top_key not in cumulative_data:
                cumulative_data[top_key] = {}
                counts[top_key] = {}
            for entry in entries:
                name = entry.get("Structure")
                if not name:
                    print(f"Warning: Missing 'Structure' in entry {entry} in file {path}")
                    continue
                if name not in cumulative_data[top_key]:
                    cumulative_data[top_key][name] = defaultdict(float)
                    counts[top_key][name] = 0
                counts[top_key][name] += 1
                for key, value in entry.items():
                    if key != "Structure":
                        if isinstance(value, (int, float)):
                            cumulative_data[top_key][name][key] += value
                        else:
                            print(f"Warning: Non-numeric value for key '{key}' in entry {entry} in file {path}")
                            continue

    # Compute the averages
    averaged_result = {}
    for top_key, names_dict in cumulative_data.items():
        averaged_result[top_key] = []
        for name, values_dict in names_dict.items():
            count = counts[top_key][name]
            if count == 0:
                print(f"Warning: No data to average for '{name}' under '{top_key}'")
                continue
            averaged_entry = {"Structure": name}
            for key, total in values_dict.items():
                average_value = total / count
                averaged_entry[key] = round(average_value, 2)
            averaged_result[top_key].append(averaged_entry)
        # Optionally sort the entries by 'name'
        averaged_result[top_key].sort(key=lambda x: x["Structure"])

    output_file = json_path / "AVERAGES" / main_type
    try:
        with open(output_file, mode="w") as outfile:
            json.dump(obj=averaged_result, fp=outfile, indent=4)
        print(f"Averaged data written to {output_file}")
    except Exception as e:
        print(f"Error writing to file {output_file}: {e}")


def run_global_json(folders: List[str]):

    global_subcortical_dict = dict()
    global_cortical_dict = dict()
    json_path = pathlib.Path("./DATA/ST1/JSON")

    for folder in folders:
        with open(file=json_path / folder / "subcortical.json", mode='r') as file:
            global_subcortical_dict[folder] = json.load(file)
        with open(file=json_path / folder / "cortical.json", mode='r') as file:
            global_cortical_dict[folder] = json.load(file)

    with open(file=json_path / "AVERAGES" / "subcortical.json", mode='r') as file:
        global_subcortical_dict["AVERAGES"] = json.load(file)

    with open(file=json_path / "AVERAGES" / "cortical.json", mode='r') as file:
        global_cortical_dict["AVERAGES"] = json.load(file)

    with open(file=json_path / "subcortical.json", mode="w") as outfile:
        json.dump(obj=global_subcortical_dict, fp=outfile, indent=4)

    with open(file=json_path / "cortical.json", mode="w") as outfile:
        json.dump(obj=global_cortical_dict, fp=outfile, indent=4)
