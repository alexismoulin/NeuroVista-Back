import pandas as pd
import json
import pathlib
from typing import Dict
import argparse


def get_subcortical():
    # Hippocampus volumes
    # LHS
    with open(file=MRI / "lh.hippoSfVolumes.txt", mode="r") as f:
        temp_list = [row.split() for row in f.readlines()]
        lh_hippo_volumes = {field[0]: float(field[1]) for field in temp_list}

    # RHS
    with open(file=MRI / "rh.hippoSfVolumes.txt", mode="r") as f:
        temp_list = [row.split() for row in f.readlines()]
        rh_hippo_volumes = {field[0]: float(field[1]) for field in temp_list}

    # Thalamus volumes
    with open(file=MRI / "ThalamicNuclei.volumes.txt", mode="r") as f:
        temp_list = [row.split() for row in f.readlines()]
        thalamic_nuclei = {field[0]: float(field[1]) for field in temp_list}

    # Amygdala volumes
    # LHS
    with open(file=MRI / "lh.amygNucVolumes.txt", mode="r") as f:
        temp_list = [row.split() for row in f.readlines()]
        lh_amygdala = {field[0]: float(field[1]) for field in temp_list}

    # RHS
    with open(file=MRI / "rh.amygNucVolumes.txt", mode="r") as f:
        temp_list = [row.split() for row in f.readlines()]
        rh_amygdala = {field[0]: float(field[1]) for field in temp_list}

    # Brain Stem
    with open(file=MRI / "brainstemSsLabels.volumes.txt", mode="r") as f:
        temp_list = [row.split() for row in f.readlines()]
        brain_stem = {field[0]: float(field[1]) for field in temp_list}

    # Hypothalamus
    hypothalamus = pd.read_csv(MRI / "hypothalamic_subunits_volumes.v1.csv").to_dict(orient="records")[0]
    del hypothalamus["subject"]

    subcortical: Dict[str, Dict[str, float]] = {
        "lh_hippocampus": lh_hippo_volumes,
        "rh_hippocampus": rh_hippo_volumes,
        "thalamus": thalamic_nuclei,
        "lh_amygdala": lh_amygdala,
        "rh_amygdala": rh_amygdala,
        "brain_stem": brain_stem,
        "hypothalamus": hypothalamus,
    }

    return subcortical


def get_cortical():
    # General segmentations
    with open(file=STATS / "aseg.stats", mode="r") as f:
        lines = [line for line in f.readlines()][79:]
        temp = [row.split() for row in lines]
    aseg = {row[4]: float(row[3]) for row in temp}

    # General Brain Volumes
    with open(file=STATS / "brainvol.stats", mode="r") as f:
        lines = [line for line in f.readlines()]
        temp = [row.split() for row in lines]
    brainvol = {row[2]: float(row[-2][:-1]) for row in temp}

    # White Matter
    with open(file=STATS / "wmparc.stats", mode="r") as f:
        lines = [line for line in f.readlines()][65:]
        temp = [row.split() for row in lines]
    wmvol = {row[4]: float(row[3]) for row in temp}

    with open(file=STATS / "lh.aparc.DKTatlas.stats", mode="r") as f:
        lines = [line for line in f.readlines()][61:]
        temp = [row.split() for row in lines]
        # colonnes 0, 2, 3, 4 et 6 ('StructName', 'SurfArea', 'GrayVol', 'ThickAvg', 'MeanCurv')
    lh_dkatlas = [
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
        # colonnes 0, 2, 3, 4 et 6 ('StructName', 'SurfArea', 'GrayVol', 'ThickAvg', 'MeanCurv')
    rh_dkatlas = [
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
        "whitematter": wmvol,
        "lh_dkatlas": lh_dkatlas,
        "rh_dkatlas": rh_dkatlas
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
