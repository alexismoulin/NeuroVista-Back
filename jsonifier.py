import pandas as pd
import json
import pathlib
from typing import Dict, List
import argparse


def get_volume(name: str, nuclei: List[Dict[str, float]]) -> float:
    for d in nuclei:
        try:
            return d[name]
        except KeyError:
            pass


def get_subcortical() -> Dict[str, list]:
    # Hippocampus volumes
    # LHS
    with open(file=MRI / "lh.hippoSfVolumes.txt", mode="r") as lhs:
        lhs_temp_list = [row.split() for row in lhs.readlines()]
    # RHS
    with open(file=MRI / "rh.hippoSfVolumes.txt", mode="r") as rhs:
        rhs_temp_list = [row.split() for row in rhs.readlines()]

    hippo_volumes = [{
        "name": field[0][0],
        "lhs_volume": round(float(field[0][1]), 2),
        "rhs_volume": round(float(field[1][1]), 2)
    } for field in zip(lhs_temp_list, rhs_temp_list)]

    # Thalamus volumes
    with open(file=MRI / "ThalamicNuclei.volumes.txt", mode="r") as f:
        temp_list = [row.split() for row in f.readlines()]

    lhs_thalamic_nuclei = [
        {field[0].replace("Left-", ""): round(float(field[1]), 2)} for field in temp_list if "Left" in field[0]
    ]
    rhs_thalamic_nuclei = [
        {field[0].replace("Right-", ""): round(float(field[1]), 2)} for field in temp_list if "Right" in field[0]
    ]
    names = [field[0].replace("Left-", "") for field in temp_list if "Left" in field[0]]

    thalamic_nuclei: List[Dict[str, float]] = [{
        "name": name,
        "lhs_volume": get_volume(name=name, nuclei=lhs_thalamic_nuclei),
        "rhs_volume": get_volume(name=name, nuclei=rhs_thalamic_nuclei),
    } for name in names]

    # Amygdala volumes
    # LHS
    with open(file=MRI / "lh.amygNucVolumes.txt", mode="r") as lhs:
        lhs_temp_list = [row.split() for row in lhs.readlines()]
    # RHS
    with open(file=MRI / "rh.amygNucVolumes.txt", mode="r") as rhs:
        rhs_temp_list = [row.split() for row in rhs.readlines()]

    amygdala = [{
        "name": field[0][0],
        "lhs_volume": round(float(field[0][1]), 2),
        "rhs_volume": round(float(field[1][1]), 2)
    } for field in zip(lhs_temp_list, rhs_temp_list)]

    # Brain Stem
    with open(file=MRI / "brainstemSsLabels.volumes.txt", mode="r") as f:
        temp_list = [row.split() for row in f.readlines()]
        brain_stem = [{"name": field[0], "volume": round(float(field[1]), 2)} for field in temp_list]

    # Hypothalamus
    hypothalamus = pd.read_csv(MRI / "hypothalamic_subunits_volumes.v1.csv").to_dict(orient="records")[0]
    del hypothalamus["subject"]
    hypothalamus["left whole"] = hypothalamus.pop("whole left")
    hypothalamus["right whole"] = hypothalamus.pop("whole right")

    lhs_hypothalamus = [{key.replace("left ", ""): value} for key, value in hypothalamus.items() if "left" in key]
    rhs_hypothalamus = [{key.replace("right ", ""): value} for key, value in hypothalamus.items() if "right" in key]

    names = [key.replace("left ", "") for key, _ in hypothalamus.items() if "left" in key]

    hypothalamic_nuclei = [{
        "name": name,
        "lhs_volume": get_volume(name=name, nuclei=lhs_hypothalamus),
        "rhs_volume": get_volume(name=name, nuclei=rhs_hypothalamus),
    } for name in names]

    # Create dictionary
    subcortical: Dict[str, list] = {
        "hippocampus": hippo_volumes,
        "thalamus": thalamic_nuclei,
        "amygdala": amygdala,
        "brain_stem": brain_stem,
        "hypothalamus": hypothalamic_nuclei,
    }

    return subcortical


def get_cortical():
    # General segmentations
    with open(file=STATS / "aseg.stats", mode="r") as f:
        lines = [line for line in f.readlines()][79:]
        temp = [row.split() for row in lines]

    aseg = [{"name": row[4], "volume": float(row[3])} for row in temp]

    # General Brain Volumes
    with open(file=STATS / "brainvol.stats", mode="r") as f:
        lines = [line for line in f.readlines()]
        temp = [row.split() for row in lines]

    brainvol = [{"name": row[2].replace(",", ""), "volume": int(float(row[-2][:-1]))} for row in temp]

    # White Matter
    with open(file=STATS / "wmparc.stats", mode="r") as f:
        lines = [line for line in f.readlines()][65:]
        temp = [row.split() for row in lines]

    wm_vol_lhs = [{row[4].replace("wm-lh-", ""): float(row[3])} for row in temp if "wm-lh" in row[4]]
    wm_vol_rhs = [{row[4].replace("wm-rh-", ""): float(row[3])} for row in temp if "wm-rh" in row[4]]

    names = [row[4].replace("wm-lh-", "") for row in temp if "wm-lh" in row[4]]

    wm_vols = [{
        "name": name,
        "lhs_volume": get_volume(name=name, nuclei=wm_vol_lhs),
        "rhs_volume": get_volume(name=name, nuclei=wm_vol_rhs),
    } for name in names]

    # LHS parcellations

    with open(file=STATS / "lh.aparc.DKTatlas.stats", mode="r") as f:
        lines = [line for line in f.readlines()][61:]
        temp = [row.split() for row in lines]
        # columns 0, 2, 3, 4 et 6 ('StructName', 'SurfArea', 'GrayVol', 'ThickAvg', 'MeanCurv')
    lh_dkt_atlas = [
        {
            'StructName': row[0],
            'SurfArea': int(row[2]),
            'GrayVol': int(row[3]),
            'ThickAvg': float(row[4]),
            'MeanCurv': float(row[6])
        } for row in temp
    ]

    with open(file=STATS / "rh.aparc.DKTatlas.stats", mode="r") as f:
        lines = [line for line in f.readlines()][61:]
        temp = [row.split() for row in lines]
        # columns 0, 2, 3, 4 et 6 ('StructName', 'SurfArea', 'GrayVol', 'ThickAvg', 'MeanCurv')
    rh_dkt_atlas = [
        {
            'StructName': row[0],
            'SurfArea': int(row[2]),
            'GrayVol': int(row[3]),
            'ThickAvg': float(row[4]),
            'MeanCurv': float(row[6])
        } for row in temp
    ]

    cortical = {
        "aseg": aseg,
        "brain": brainvol,
        "whitematter": wm_vols,
        "lh_dkatlas": lh_dkt_atlas,
        "rh_dkatlas": rh_dkt_atlas
    }

    return cortical


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-i",
        "--input_folder",
        required=True,
        help="input folder where the DICOM images have been uploaded"
    )
    args = parser.parse_args()
    SUBJECTS = pathlib.Path("/usr/local/freesurfer/7.4.1/subjects")
    MRI = SUBJECTS / f"{args.input_folder}/mri"
    STATS = SUBJECTS / f"{args.input_folder}/stats"

    subcortical_dict = get_subcortical()
    cortical_dict = get_cortical()

    # Convert and write JSON object to file
    with open(file="./subcortical.json", mode="w") as outfile:
        json.dump(subcortical_dict, outfile)

    with open(file="./cortical.json", mode="w") as outfile:
        json.dump(cortical_dict, outfile)
