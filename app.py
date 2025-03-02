import json
import logging
import os
import time
from pathlib import Path
from sys import platform
from queue import Queue
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Thread, Event
from typing import List, Dict

import dicom2nifti
import pydicom
from flask import Flask, jsonify, request, Response, make_response
from flask_cors import CORS
from werkzeug.datastructures import ImmutableMultiDict, FileStorage
from functools import partial

from jsonifier import run_jsonifier, run_json_average, run_global_json
from utils import (
    add_dcm_extension,
    get_folder_names,
    create_folders,
    get_nifti_dimensions,
    reconall,
    process_lesions,
    segment_subregions,
    segment_hypothalamus,
    run_fastsurfer,
    process_corestats,
)

app = Flask(__name__)
CORS(app, supports_credentials=True)
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Thread-safe queue for Server-Sent Events (SSE)
STEP_COMPLETION_QUEUE: Queue = Queue()
BASE_DATA_PATH: Path = Path("./DATA")
processing_event = Event()  # Using an Event for thread-safe status tracking


def notify_step(step: str) -> None:
    """
    Helper function to notify that a processing step is complete.
    """
    STEP_COMPLETION_QUEUE.put(step)


def save_dicoms(request_files: ImmutableMultiDict[str, FileStorage], dicom_directory: Path) -> None:
    """
    Save uploaded DICOM files into subdirectories based on their SeriesDescription.
    """
    for dicom_file in request_files.getlist("dicoms"):
        try:
            ds = pydicom.dcmread(dicom_file)
            series_description = getattr(ds, "SeriesDescription", "UNKNOWN").replace(" ", "_")
            series_dir = dicom_directory / series_description
            series_dir.mkdir(parents=True, exist_ok=True)
            dicom_file.stream.seek(0)
            dest_file = series_dir / add_dcm_extension(os.path.basename(dicom_file.filename))
            dicom_file.save(dst=str(dest_file))
        except Exception as e:
            logger.exception("Skipping file %s due to error: %s", dicom_file.filename, e)
    logger.info("DICOM files saved successfully")
    notify_step("dicom")


def convert_to_nifti(dicom_directory: Path, nifti_directory: Path) -> None:
    """
    Convert each DICOM series in the dicom_directory to a single NIfTI file.
    """
    for folder in get_folder_names(dicom_directory):
        input_dir = dicom_directory / folder
        output_file = nifti_directory / f"{folder}.nii.gz"
        try:
            dicom2nifti.dicom_series_to_nifti(
                original_dicom_directory=str(input_dir),
                output_file=str(output_file)
            )
        except Exception as e:
            logger.exception("Error converting folder %s: %s", folder, e)
    logger.info("NIFTI conversion completed")
    notify_step("nifti")


def run_reconall(base_dir: Path) -> None:
    """
    Execute FreeSurfer recon-all process on the given base directory.
    """
    start_time = time.time()
    try:
        reconall(base_dir=base_dir.resolve())
        elapsed = time.time() - start_time
        logger.info("FreeSurfer recon-all completed in %.2f seconds", elapsed)
    except Exception as e:
        logger.exception("Error during FreeSurfer recon-all: %s", e)
    notify_step("recon")


def process_lesions_for_series(series: str, freesurfer_path: Path, samseg_path: Path) -> None:
    """
    Process lesions for a single series.
    """
    try:
        process_lesions(freesurfer_path, samseg_path, series)
    except Exception as e:
        logger.exception("Error processing lesions for series %s: %s", series, e)


def process_lesions_for_all(folders: List[str], freesurfer_path: Path, samseg_path: Path) -> None:
    """
    Process lesions for all series in parallel.
    """
    with ThreadPoolExecutor(max_workers=max(1, os.cpu_count())) as executor:
        executor.map(partial(process_lesions_for_series, freesurfer_path=freesurfer_path, samseg_path=samseg_path), folders)
    logger.info("SAMSEG processing completed")
    notify_step("lesions")


def segment_subregions_for_all(folders: List[str], freesurfer_path: Path) -> None:
    """
    Run subcortical segmentation (thalamus, brainstem, hippo-amygdala) for each series.
    """
    for folder in folders:
        for structure in ["thalamus", "brainstem", "hippo-amygdala"]:
            try:
                segment_subregions(structure=structure, subject_id=folder, subject_dir=freesurfer_path)
            except Exception as e:
                logger.exception("Error segmenting %s for series %s: %s", structure, folder, e)
    logger.info("Subcortical segmentation completed")
    notify_step("subs1")


def segment_hypothalamus_for_all(folders: List[str], freesurfer_path: Path) -> None:
    """
    Run hypothalamus segmentation for each series.
    """
    for folder in folders:
        try:
            segment_hypothalamus(subject_id=folder, subject_dir=freesurfer_path)
        except Exception as e:
            logger.exception("Error segmenting hypothalamus for series %s: %s", folder, e)
    logger.info("FreeSurfer hypothalamus segmentation completed")


def run_fastsurfer_for_series(series: str, freesurfer_path: Path, fastsurfer_path: Path, workflows_path: Path) -> None:
    """
    Run FastSurfer for a single series.
    """
    try:
        run_fastsurfer(
            fs_dir=Path.home() / "FastSurfer",
            t1=freesurfer_path / series / "mri" / "T1.mgz",
            sid=series,
            sd=fastsurfer_path,
            wf_dir=workflows_path,
            parallel=True,
            threads=max(1, os.cpu_count()),
        )
    except Exception as e:
        logger.exception("Error in FastSurfer processing for series %s: %s", series, e)


def run_fastsurfer_for_all(folders: List[str],
                           freesurfer_path: Path,
                           fastsurfer_path: Path,
                           workflows_path: Path) -> None:
    """
    Run FastSurfer segmentation in parallel for all series.
    """
    if platform == "darwin":
        os.environ["PYTORCH_ENABLE_MPS_FALLBACK"] = "1"

    with ThreadPoolExecutor(max_workers=max(1, os.cpu_count())) as executor:
        executor.map(
            partial(run_fastsurfer_for_series, freesurfer_path=freesurfer_path, fastsurfer_path=fastsurfer_path, workflows_path=workflows_path),
            folders,
        )
    logger.info("Extra subcortical segmentation completed")
    notify_step("subs2")


def generate_json_files(folders: List[str],
                        freesurfer_path: Path,
                        fastsurfer_path: Path,
                        samseg_path: Path,
                        json_folder: Path) -> None:
    """
    Generate JSON files for each series and compute averages and global metrics.
    """
    for folder in folders:
        output_dir = json_folder / folder
        output_dir.mkdir(parents=True, exist_ok=True)
        try:
            run_jsonifier(
                freesurfer_path=freesurfer_path / folder,
                fastsurfer_path=fastsurfer_path / folder,
                samseg_path=samseg_path / folder,
                output_folder=output_dir,
            )
        except Exception as e:
            logger.exception("Error generating JSON for series %s: %s", folder, e)
    averages_dir = json_folder / "AVERAGES"
    averages_dir.mkdir(parents=True, exist_ok=True)
    try:
        run_json_average(json_path=json_folder, folders=folders, main_type="cortical.json")
        run_json_average(json_path=json_folder, folders=folders, main_type="subcortical.json")
        run_json_average(json_path=json_folder, folders=folders, main_type="general.json")
        run_global_json(json_path=json_folder, folders=folders)
    except Exception as e:
        logger.exception("Error generating average/global JSON files: %s", e)
    logger.info("JSON files generation completed")
    notify_step("json")


def process_corestats_for_series(series: str, freesurfer_path: Path, fastsurfer_path: Path, corestats_folder: Path) -> None:
    """
    Process core statistics for a single series.
    """
    try:
        fs_series_path = freesurfer_path / series
        fastsurfer_series_path = fastsurfer_path / series
        corestats_series_folder = corestats_folder / series
        process_corestats(fs_series_path, fastsurfer_series_path, corestats_series_folder)
        logger.info("Successfully processed corestats for series: %s", series)
    except Exception as e:
        logger.exception("Error processing corestats for series %s: %s", series, e)


def process_corestats_for_all(folders: List[str],
                              freesurfer_path: Path,
                              fastsurfer_path: Path,
                              corestats_folder: Path) -> None:
    """
    Process core statistics for all series in parallel.
    """
    with ThreadPoolExecutor(max_workers=max(1, os.cpu_count())) as executor:
        futures = [
            executor.submit(process_corestats_for_series, series, freesurfer_path, fastsurfer_path, corestats_folder)
            for series in folders
        ]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.exception("Exception in corestats processing: %s", e)
    logger.info("Core statistics processing completed for all series.")


def run_processing(base_path: Path, request_files: ImmutableMultiDict[str, FileStorage]) -> None:
    """
    Run the complete processing pipeline.
    """
    try:
        folders_dict = create_folders(base_path)
        dicom_dir = folders_dict["dicom"]
        nifti_dir = folders_dict["nifti"]
        fs_path = folders_dict["freesurfer"]
        samseg_path = folders_dict["samseg"]
        fastsurfer_path = folders_dict["fastsurfer"]
        wf_path = folders_dict["workflows"]
        json_folder = folders_dict["json"]
        corestats_folder = folders_dict["corestats"]

        save_dicoms(request_files=request_files, dicom_directory=dicom_dir)
        series_folders: List[str] = get_folder_names(dicom_dir)
        convert_to_nifti(dicom_directory=dicom_dir, nifti_directory=nifti_dir)
        run_reconall(base_dir=base_path)
        process_lesions_for_all(folders=series_folders, freesurfer_path=fs_path, samseg_path=samseg_path)
        segment_subregions_for_all(folders=series_folders, freesurfer_path=fs_path)
        segment_hypothalamus_for_all(folders=series_folders, freesurfer_path=fs_path)
        run_fastsurfer_for_all(folders=series_folders, freesurfer_path=fs_path,
                               fastsurfer_path=fastsurfer_path, workflows_path=wf_path)
        generate_json_files(
            folders=series_folders,
            freesurfer_path=fs_path,
            fastsurfer_path=fastsurfer_path,
            samseg_path=samseg_path,
            json_folder=json_folder,
        )
        process_corestats_for_all(
            folders=series_folders,
            freesurfer_path=fs_path,
            fastsurfer_path=fastsurfer_path,
            corestats_folder=corestats_folder,
        )
    except Exception as e:
        logger.exception("Error during processing: %s", e)
    finally:
        processing_event.clear()


def read_json_file(json_path: Path) -> Dict:
    """
    Helper function to read a JSON file.
    """
    try:
        with json_path.open("r") as f:
            return json.load(f)
    except FileNotFoundError:
        logger.exception("JSON file not found: %s", json_path)
        return {}


@app.get("/")
def home() -> str:
    """Basic home endpoint."""
    return "Home"


@app.get("/stream")
def stream() -> Response:
    """
    Stream processing step completions to the frontend in real time using Server-Sent Events (SSE).
    """
    def event_stream():
        while True:
            if not STEP_COMPLETION_QUEUE.empty():
                step_completed = STEP_COMPLETION_QUEUE.get()
                yield f"data: {step_completed}\n\n"
            time.sleep(1)

    return Response(event_stream(), mimetype="text/event-stream")


@app.post("/upload")
def upload() -> Response:
    """
    Handle file uploads and return submitted metadata.
    """
    try:
        form_data = {key: request.form[key] for key in request.form}
        file_names = [os.path.basename(file.filename) for file in request.files.getlist("dicoms")]
        response = make_response(jsonify({"form_data": form_data, "file_names": file_names}))
        response.status_code = 200
        return response
    except Exception as e:
        logger.exception("Error during file upload: %s", e)
        response = make_response(jsonify({"error": "File upload failed"}))
        response.status_code = 500
        return response


@app.post("/run_script")
def run_script() -> Response:
    """
    Initiate the processing pipeline.
    """
    if processing_event.is_set():
        response = make_response(jsonify({"error": "Processing already in progress"}))
        response.status_code = 400
        return response

    study = request.form.get("study", "").replace(" ", "_")
    patient = request.form.get("patient", "").replace(" ", "_")
    if not study or not patient:
        response = make_response(jsonify({"error": "Study and patient name are required"}))
        response.status_code = 400
        return response
    if not request.files.getlist("dicoms"):
        response = make_response(jsonify({"error": "No DICOM files provided"}))
        response.status_code = 400
        return response

    base_path = BASE_DATA_PATH / patient / study
    processing_event.set()
    Thread(target=run_processing, args=(base_path, request.files)).start()
    response = make_response(jsonify({"message": "Processing started"}))
    response.status_code = 202
    return response


@app.get("/cortical/<patient>/<study>")
def cortical(patient: str, study: str) -> Response:
    """
    Retrieve cortical JSON data.
    """
    json_path = BASE_DATA_PATH / patient / study / "JSON" / "cortical.json"
    cortical_json = read_json_file(json_path)
    if cortical_json:
        response = make_response(jsonify(cortical_json))
        response.status_code = 200
        return response
    else:
        response = make_response(jsonify({"error": "No Data"}))
        response.status_code = 404
        return response


@app.get("/subcortical/<patient>/<study>")
def subcortical(patient: str, study: str) -> Response:
    """
    Retrieve subcortical JSON data.
    """
    json_path = BASE_DATA_PATH / patient / study / "JSON" / "subcortical.json"
    subcortical_json = read_json_file(json_path)
    if subcortical_json:
        return jsonify(subcortical_json)
    else:
        response = make_response(jsonify({"error": "No Data"}))
        response.status_code = 404
        return response


@app.get("/general/<patient>/<study>")
def general(patient: str, study: str) -> Response:
    """
    Retrieve general JSON data.
    """
    json_path = BASE_DATA_PATH / patient / study / "JSON" / "general.json"
    general_json = read_json_file(json_path)
    if general_json:
        return jsonify(general_json)
    else:
        response = make_response(jsonify({"error": "No Data"}))
        response.status_code = 404
        return response


@app.get("/series/<patient>/<study>")
def get_series(patient: str, study: str) -> Response:
    """
    Retrieve available series along with their NIfTI dimensions.
    """
    dicoms = BASE_DATA_PATH / patient / study / "DICOM"
    series_list: List[str] = get_folder_names(dicoms)
    series_dict: Dict[str, tuple] = {
        s: get_nifti_dimensions(BASE_DATA_PATH / patient / study / f"NIFTI/{s}.nii.gz")
        for s in series_list
    }
    return jsonify(series_dict)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001)
