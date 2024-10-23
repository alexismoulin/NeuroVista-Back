import pandas as pd
import json
import pathlib
from typing import Dict, List, Union


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
        "name": field[0][0],
        "lhs_volume": round(float(field[0][1]), 2),
        "rhs_volume": round(float(field[1][1]), 2)
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
        "name": name,
        "lhs_volume": get_volume(name=name, nuclei=lhs_thalamic_nuclei),
        "rhs_volume": get_volume(name=name, nuclei=rhs_thalamic_nuclei),
    } for name in names]


def process_amygdala(mri: pathlib.Path) -> List[Dict[str, Union[str, float]]]:
    """Process amygdala volumes."""
    lhs_data = read_volume_file(mri / "lh.amygNucVolumes.txt")
    rhs_data = read_volume_file(mri / "rh.amygNucVolumes.txt")

    return [{
        "name": field[0][0],
        "lhs_volume": round(float(field[0][1]), 2),
        "rhs_volume": round(float(field[1][1]), 2)
    } for field in zip(lhs_data, rhs_data)]


def process_brain_stem(mri: pathlib.Path) -> List[Dict[str, Union[str, float]]]:
    """Process brain stem volumes."""
    brain_stem_data = read_volume_file(mri / "brainstemSsLabels.volumes.txt")
    return [{"name": field[0], "volume": round(float(field[1]), 2)} for field in brain_stem_data]


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
def process_hypothalamus_v2(mri: pathlib.Path):
    return None


def process_cerebellum(mri: pathlib.Path):
    return None


def get_subcortical(mri: pathlib.Path) -> Dict[str, list]:
    """Extracts subcortical volumes."""
    subcortical = {
        "hippocampus": process_hippocampus(mri),
        "thalamus": process_thalamus(mri),
        "amygdala": process_amygdala(mri),
        "brain_stem": process_brain_stem(mri),
        "hypothalamus": process_hypothalamus(mri)
    }
    return subcortical


def get_cortical(stats: pathlib.Path) -> Dict[str, list]:
    """Extracts cortical volumes and parcellations."""
    aseg = [{"name": row[4], "volume": float(row[3])} for row in read_volume_file(stats / "aseg.stats")[79:]]
    brainvol = [{"name": row[2].replace(",", ""), "volume": int(float(row[-2][:-1]))} for row in read_volume_file(stats / "brainvol.stats")]

    wm_data = read_volume_file(stats / "wmparc.stats")[65:]
    wm_vol_lhs = [{row[4].replace("wm-lh-", ""): float(row[3])} for row in wm_data if "wm-lh" in row[4]]
    wm_vol_rhs = [{row[4].replace("wm-rh-", ""): float(row[3])} for row in wm_data if "wm-rh" in row[4]]
    names = [row[4].replace("wm-lh-", "") for row in wm_data if "wm-lh" in row[4]]

    wm_vols = [{
        "name": name,
        "lhs_volume": get_volume(name=name, nuclei=wm_vol_lhs),
        "rhs_volume": get_volume(name=name, nuclei=wm_vol_rhs),
    } for name in names]

    lh_dkt_atlas = [{"name": row[0], "SurfArea": int(row[2]), "GrayVol": int(row[3]), "ThickAvg": float(row[4]), "MeanCurv": float(row[6])} for row in read_volume_file(stats / "lh.aparc.DKTatlas.stats")[61:]]
    rh_dkt_atlas = [{"name": row[0], "SurfArea": int(row[2]), "GrayVol": int(row[3]), "ThickAvg": float(row[4]), "MeanCurv": float(row[6])} for row in read_volume_file(stats / "rh.aparc.DKTatlas.stats")[61:]]

    cortical = {
        "aseg": aseg,
        "brain": brainvol,
        "whitematter": wm_vols,
        "lh_dkatlas": lh_dkt_atlas,
        "rh_dkatlas": rh_dkt_atlas
    }

    return cortical


def run_jsonifier(freesurfer_path: pathlib.Path, output_folder: pathlib.Path):
    """Runs the process of generating JSON files for subcortical and cortical volumes."""
    subcortical_dict = get_subcortical(mri=freesurfer_path / "mri")
    cortical_dict = get_cortical(stats=freesurfer_path / "stats")

    # Write JSON objects to files
    with open(output_folder / "subcortical.json", mode="w") as outfile:
        json.dump(subcortical_dict, outfile, indent=4)

    with open(output_folder / "cortical.json", mode="w") as outfile:
        json.dump(cortical_dict, outfile, indent=4)
