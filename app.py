import json
import logging
import os
import time
import queue
from pydicom.uid import MediaStorageDirectoryStorage
from pathlib import Path
from sys import platform
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Event
from typing import List, Dict

import dicom2nifti
import pydicom
from flask import Flask, jsonify, request, Response, make_response, stream_with_context
from flask_cors import CORS
from werkzeug.datastructures import ImmutableMultiDict, FileStorage
from functools import partial

from jsonifier import run_jsonifier, run_json_average, run_global_json
from utils import (
    add_dcm_extension,
    get_folder_names,
    create_folders,
    get_nifti_dimensions,
    list_folder_subfolders,
    reconall,
    process_lesions,
    segment_subregions,
    segment_hypothalamus,
    run_fastsurfer,
    process_corestats
)

app = Flask(__name__)
CORS(app, supports_credentials=True)
logger = logging.getLogger(__name__)

# Thread-safe queue for Server-Sent Events (SSE)
STEP_COMPLETION_QUEUE = queue.Queue()
BASE_DATA_PATH = Path("./DATA")
processing_event = Event()


def sanitize_name(name: str) -> str:
    """
    Sanitize input names to prevent path traversal and remove unsafe characters.
    Only alphanumeric characters, underscores, and dashes are allowed.
    """
    import re
    return re.sub(r'[^A-Za-z0-9_-]', '', name)


def notify_step(step: str) -> None:
    """
    Helper function to notify that a processing step is complete.
    """
    STEP_COMPLETION_QUEUE.put(step)


def notify_failure(step: str) -> None:
    """
    Helper function to notify that a processing step has failed.
    It prefixes the step key with 'failed_'.
    """
    notify_step(f"failed_{step}")


def save_dicoms(request_files: ImmutableMultiDict[str, FileStorage], dicom_directory: Path) -> None:
    """
    Save uploaded DICOM files into subdirectories based on their SeriesDescription.
    Only processes DICOM images and skips DICOMDIR files.
    """
    for dicom_file in request_files.getlist("dicoms"):
        try:
            # Skip files that are DICOMDIR based on their filename
            if "DICOMDIR" in dicom_file.filename.upper():
                logger.info("Skipping DICOMDIR file based on filename: %s", dicom_file.filename)
                continue

            ds = pydicom.dcmread(dicom_file)
            # Also skip based on SOPClassUID if available
            if str(getattr(ds, "SOPClassUID", "")) == str(MediaStorageDirectoryStorage):
                logger.info("Skipping DICOMDIR file based on SOPClassUID: %s", dicom_file.filename)
                continue

            series_description = getattr(ds, "SeriesDescription", "UNKNOWN").replace(" ", "_")
            series_dir = dicom_directory / series_description
            series_dir.mkdir(parents=True, exist_ok=True)
            dicom_file.stream.seek(0)
            dest_file = series_dir / add_dcm_extension(os.path.basename(dicom_file.filename))
            dicom_file.save(dst=str(dest_file))
        except Exception as e:
            logger.exception("Skipping file %s due to error: %s", dicom_file.filename, e)
    logger.info("DICOM files saved successfully")


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
        raise


def process_lesions_for_series(series: str, freesurfer_path: Path, samseg_path: Path) -> None:
    """
    Process lesions for a single series.
    """
    try:
        process_lesions(freesurfer_path, samseg_path, series)
    except Exception as e:
        logger.exception("Error processing lesions for series %s: %s", series, e)
        raise


def process_lesions_for_all(folders: List[str], freesurfer_path: Path, samseg_path: Path) -> None:
    """
    Process lesions for all series in parallel.
    """
    with ThreadPoolExecutor(max_workers=max(1, os.cpu_count())) as executor:
        executor.map(partial(process_lesions_for_series, freesurfer_path=freesurfer_path, samseg_path=samseg_path), folders)
    logger.info("SAMSEG processing completed")


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
            threads=max(1, os.cpu_count()),
        )
    except Exception as e:
        logger.exception("Error in FastSurfer processing for series %s: %s", series, e)
        raise


def run_fastsurfer_for_all(folders: List[str],
                           freesurfer_path: Path,
                           fastsurfer_path: Path,
                           workflows_path: Path) -> None:
    """
    Run FastSurfer segmentation in parallel for all series.
    """
    if platform == "darwin":
        os.environ["PYTORCH_ENABLE_MPS_FALLBACK"] = "1"

    try:
        with ThreadPoolExecutor(max_workers=max(1, os.cpu_count())) as executor:
            # Force iteration over the map iterator to trigger any exceptions
            list(executor.map(
                partial(run_fastsurfer_for_series, freesurfer_path=freesurfer_path, fastsurfer_path=fastsurfer_path,
                        workflows_path=workflows_path),
                folders,
            ))
        logger.info("Extra subcortical segmentation completed")
    except Exception as e:
        logger.exception("Error with FastSurfer: %s", e)
        raise


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
            raise
    averages_dir = json_folder / "AVERAGES"
    averages_dir.mkdir(parents=True, exist_ok=True)
    try:
        run_json_average(json_path=json_folder, folders=folders, main_type="cortical.json")
        run_json_average(json_path=json_folder, folders=folders, main_type="subcortical.json")
        run_json_average(json_path=json_folder, folders=folders, main_type="general.json")
        run_global_json(json_path=json_folder, folders=folders)
    except Exception as e:
        logger.exception("Error generating average/global JSON files: %s", e)
        raise
    logger.info("JSON files generation completed")


def process_corestats_for_series(series: str, freesurfer_path: Path, corestats_folder: Path) -> None:
    """
    Process core statistics for a single series.
    """
    try:
        fs_series_path = freesurfer_path / series
        corestats_series_folder = corestats_folder / series
        process_corestats(fs_series_path, corestats_series_folder)
        logger.info("Successfully processed corestats for series: %s", series)
    except Exception as e:
        logger.exception("Error processing corestats for series %s: %s", series, e)
        raise


def process_corestats_for_all(folders: List[str],
                              freesurfer_path: Path,
                              corestats_folder: Path) -> None:
    """
    Process core statistics for all series in parallel.
    """
    with ThreadPoolExecutor(max_workers=max(1, os.cpu_count())) as executor:
        futures = [
            executor.submit(process_corestats_for_series, series, freesurfer_path, corestats_folder)
            for series in folders
        ]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.exception("Exception in corestats processing: %s", e)
                raise
    logger.info("Core statistics processing completed for all series.")


def run_processing(base_path: Path, request_files: ImmutableMultiDict[str, FileStorage]) -> None:
    """
    Run the complete processing pipeline.
    If a step fails, notify the failure and stop further processing.
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

        # DICOM upload
        try:
            save_dicoms(request_files=request_files, dicom_directory=dicom_dir)
            notify_step("dicom")
        except Exception as e:
            logger.exception("Error during DICOM upload: %s", e)
            notify_failure("dicom")
            return

        series_folders = get_folder_names(dicom_dir)

        # NIFTI conversion
        try:
            convert_to_nifti(dicom_directory=dicom_dir, nifti_directory=nifti_dir)
            notify_step("nifti")
        except Exception as e:
            logger.exception("Error during NIFTI conversion: %s", e)
            notify_failure("nifti")
            return

        # Brain reconstruction (recon-all)
        try:
            run_reconall(base_dir=base_path)
            notify_step("recon")
        except Exception as e:
            logger.exception("Error during brain reconstruction: %s", e)
            notify_failure("recon")
            return

        # Lesions processing
        try:
            process_lesions_for_all(folders=series_folders, freesurfer_path=fs_path, samseg_path=samseg_path)
            notify_step("lesions")
        except Exception as e:
            logger.exception("Error during lesions processing: %s", e)
            notify_failure("lesions")
            return


        # Subcortical segmentation (Thalamus, Brain Stem, Amygdala)
        try:
            segment_subregions_for_all(folders=series_folders, freesurfer_path=fs_path)
            notify_step("subs1")
        except Exception as e:
            logger.exception("Error during subcortical segmentation: %s", e)
            notify_failure("subs1")
            return

        # Hypothalamus segmentation
        try:
            segment_hypothalamus_for_all(folders=series_folders, freesurfer_path=fs_path)
            notify_step("subs2")
        except Exception as e:
            logger.exception(msg=f"Error during hypothalamus segmentation: {e}")
            notify_failure("subs2")
            return

        # Extra segmentation (FastSurfer)
        # try:
        #     run_fastsurfer_for_all(
        #         folders=series_folders,
        #         freesurfer_path=fs_path,
        #         fastsurfer_path=fastsurfer_path,
        #         workflows_path=wf_path,
        #     )
        #     notify_step("subs2")
        # except Exception as e:
        #     logger.exception("Error during extra segmentation: %s", e)
        #     notify_failure("subs2")
        #     return

        # JSON file generation
        try:
            generate_json_files(
                folders=series_folders,
                freesurfer_path=fs_path,
                fastsurfer_path=fastsurfer_path,
                samseg_path=samseg_path,
                json_folder=json_folder,
            )
            notify_step("json")
        except Exception as e:
            logger.exception("Error during JSON file generation: %s", e)
            notify_failure("json")
            return

        # Core statistics processing
        try:
            process_corestats_for_all(folders=series_folders, freesurfer_path=fs_path, corestats_folder=corestats_folder)
            notify_step("corestats")
        except Exception as e:
            logger.exception("Error during core statistics processing: %s", e)
            notify_failure("corestats")
            return

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

    @stream_with_context
    def event_stream():
        while True:
            try:
                # Wait for a step completion with a timeout
                step_completed = STEP_COMPLETION_QUEUE.get(timeout=1)
                yield f"data: {step_completed}\n\n"
            except queue.Empty:
                # Send a heartbeat to keep the connection alive
                yield "data: heartbeat\n\n"
            except Exception as e:
                logger.error("Unexpected error in event stream: %s", e)
                break  # Optionally break out of the loop on unexpected errors

    # Optionally include a retry directive (in milliseconds)
    headers = {"Cache-Control": "no-cache"}
    return Response(event_stream(), headers=headers, mimetype="text/event-stream")


@app.post("/run_script")
def run_script() -> Response:
    """
    Initiate the processing pipeline.
    """
    if processing_event.is_set():
        response = make_response(jsonify({"error": "Processing already in progress"}))
        response.status_code = 400
        return response

    study = sanitize_name(request.form.get("study", ""))
    patient = sanitize_name(request.form.get("patient", ""))
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
    run_processing(base_path=base_path, request_files=request.files)
    response = make_response(jsonify({"message": "Processing started"}))
    response.status_code = 202
    return response

@app.get("/studies")
def studies() -> Response:
    """
    Retrieve Patient / Study couples.
    """
    couples = list_folder_subfolders(directory_path=BASE_DATA_PATH)
    if len(couples) > 0:
        response = make_response(jsonify(couples))
        response.status_code = 200
        logger.info(f"COUPLES: {couples}")
        return response
    else:
        response = make_response(jsonify({"error": "No Data"}))
        response.status_code = 404
        logger.info(f"COUPLES: NIET !!")
        return response


@app.get("/cortical/<patient>/<study>")
def cortical(patient: str, study: str) -> Response:
    """
    Retrieve cortical JSON data.
    """
    json_path = BASE_DATA_PATH / sanitize_name(patient) / sanitize_name(study) / "JSON" / "cortical.json"
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
    json_path = BASE_DATA_PATH / sanitize_name(patient) / sanitize_name(study) / "JSON" / "subcortical.json"
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
    json_path = BASE_DATA_PATH / sanitize_name(patient) / sanitize_name(study) / "JSON" / "general.json"
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
    dicoms = BASE_DATA_PATH / sanitize_name(patient) / sanitize_name(study) / "DICOM"
    series_list: List[str] = get_folder_names(dicoms)
    series_dict: Dict[str, tuple] = {
        s: get_nifti_dimensions(BASE_DATA_PATH / sanitize_name(patient) / sanitize_name(study) / f"NIFTI/{s}.nii.gz")
        for s in series_list
    }
    return jsonify(series_dict)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001)
