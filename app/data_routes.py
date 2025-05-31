from pathlib import Path
from flask import Blueprint, jsonify, current_app, Response
from core.utils import sanitize_name, get_nifti_dimensions, list_folder_subfolders, get_folder_names
from core.processing import read_json_file
from typing import Tuple

data_bp = Blueprint(name="data", import_name=__name__)


def load_json(patient: str, study: str, label: str) -> Tuple[Response, int]:
    """
    Load a JSON file for the specified patient, study, and label, then return it as a Flask response.

    Constructs the path to the JSON file under the base data directory, using sanitized patient and study names,
    then attempts to read it. If the file exists and contains valid JSON, returns it with a 200 status code.
    Otherwise, returns a JSON error message with a 404 status code.

    Args:
        patient (str): The name of the patient (will be sanitized).
        study   (str): The name of the study (will be sanitized).
        label   (str): The JSON filename (without “.json”), e.g. "cortical", "subcortical", or "general".

    Returns:
        Tuple[Response, int]:
            - A Flask `Response` created by `jsonify(data)` and an integer HTTP status code.
            - If the JSON was found and loaded successfully, status code is 200.
            - If the JSON file does not exist or is empty, returns `jsonify(error="No Data")` with 404.
    """
    # Construct the relative subpath: <sanitized_patient>/<sanitized_study>
    subpath = Path(sanitize_name(patient)) / sanitize_name(study)

    # Build the full path to the JSON file under BASE_DATA_PATH
    json_folder = current_app.config["BASE_DATA_PATH"] / subpath / "JSON"
    json_path = json_folder / f"{label}.json"

    # Attempt to read the JSON data
    data = read_json_file(json_path)
    if data:
        # Return the JSON payload with HTTP 200
        return jsonify(data), 200

    # If no data found, return an error payload with HTTP 404
    return jsonify(error="No Data"), 404


@data_bp.route("/", methods=["GET"])
def home() -> Tuple[Response, int]:
    return jsonify(message="Service is up and running"), 200


@data_bp.route("/studies", methods=["GET"])
def studies() -> Tuple[Response, int]:
    couples = list_folder_subfolders(directory_path=current_app.config["BASE_DATA_PATH"])
    if couples:
        current_app.logger.info(msg=f"Found {len(couples)} study/patient pairs")
        return jsonify(couples), 200
    return jsonify(error="No Data"), 404


@data_bp.route("/cortical/<string:patient>/<string:study>", methods=["GET"])
def cortical(patient: str, study: str) -> Tuple[Response, int]:
    return load_json(patient, study, label="cortical")


@data_bp.route("/subcortical/<string:patient>/<string:study>", methods=["GET"])
def subcortical(patient: str, study: str) -> Tuple[Response, int]:
    return load_json(patient, study, label="subcortical")


@data_bp.route("/general/<string:patient>/<string:study>", methods=["GET"])
def general(patient: str, study: str) -> Tuple[Response, int]:
    return load_json(patient, study, label="general")


@data_bp.route("/series/<string:patient>/<string:study>", methods=["GET"])
def get_series(patient: str, study: str) -> Tuple[Response, int]:
    base = current_app.config["BASE_DATA_PATH"] / sanitize_name(name=patient) / sanitize_name(name=study)
    dicom_dir = base / "DICOM"
    series_list = get_folder_names(directory=dicom_dir)
    result = {}
    for series in series_list:
        nifti_path = base / "NIFTI" / f"{series}.nii.gz"
        result[series] = get_nifti_dimensions(file_path=nifti_path)
    return jsonify(result), 200
