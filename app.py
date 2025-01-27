import json
import logging
import os
from pathlib import Path
from sys import platform
import time

import dicom2nifti
import pydicom
from flask import Flask, jsonify, request, Response
from flask_cors import CORS

from jsonifier import run_jsonifier, run_json_average, run_global_json
from utils import (add_dcm_extension, get_folder_names, create_folders, get_nifti_dimensions,
                   reconall, process_lesions, segment_subregions, run_fastsurfer)

app = Flask(__name__)
CORS(app, supports_credentials=True)
logging.basicConfig(level=logging.INFO)
# A global queue (in-memory) to hold messages about step completions
# In production, you might use Redis or a thread-safe queue, especially for multi-process setups
STEP_COMPLETION_QUEUE = []


@app.route("/")
def home() -> str:
    return "Home"


@app.route("/stream", methods=["GET"])
def stream():
    """
    This endpoint streams events to the frontend in real time using SSE.
    """
    def event_stream():
        """
        Generator function that yields Server-Sent Events (SSE) whenever a step is completed.
        We’ll use a while-True loop to wait for new messages from an internal queue,
        or we could yield them directly from the script if we integrate them differently.
        """
        # For demonstration, we’ll just listen to some global or internal queue.

        # If you store events in a queue, you can pop from that queue here.
        # or keep reading from a queue
        # This is a naive example of a wait/notify mechanism
        while True:
            if len(STEP_COMPLETION_QUEUE) > 0:
                step_completed = STEP_COMPLETION_QUEUE.pop(0)
                # SSE format: data: <your message>\n\n
                yield f"data: {step_completed}\n\n"
            time.sleep(1)  # Prevent 100% CPU usage

    return Response(event_stream(), mimetype="text/event-stream")


@app.route('/upload', methods=['POST'])
def upload():
    try:
        form_data = {key: request.form[key] for key in request.form}
        file_names = [os.path.basename(file.filename) for file in request.files.getlist('dicoms')]

        return jsonify({'form_data': form_data, 'file_names': file_names}), 200
    except Exception as e:
        logging.error(f"Error during file upload: {e}")
        return jsonify({"error": "File upload failed"}), 500


@app.route("/run_script", methods=["POST"])
def run_script() -> tuple[Response, int] | tuple[str, int]:
    # Preparing files
    print("FORM: ", request.form)
    study = request.form.get("study").replace(" ", "_")
    if not study:
        return jsonify({"error": "Study not provided"}), 400
    patient = request.form.get("patient").replace(" ", "_")
    if not patient:
        return jsonify({"error": "Patient name not provided"}), 400
    notes = request.form.get("notes")

    base_path = Path("./DATA") / study
    (dicom_directory, nifti_directory, freesurfer_path,
     samseg_path, fastsurfer_path, workflows_path, json_folder) = create_folders(base_path=base_path)

    with open(json_folder / 'notes.json', 'w') as f:
        json.dump({"notes": notes}, f)

    # Copying DICOMS
    try:
        for file in request.files.getlist("dicoms"):
            try:
                series_description = pydicom.dcmread(file).SeriesDescription.replace(" ", "_")
                ser_desc = series_description.replace(" ", "_")
                (dicom_directory / ser_desc).mkdir(parents=True, exist_ok=True)
                file.stream.seek(0)  # Reset the file pointer to the beginning
                file.save(dst=dicom_directory / ser_desc / add_dcm_extension(os.path.basename(file.filename)))
            except AttributeError:
                pass

        folders = get_folder_names(directory=dicom_directory)
        print("Folders: ", folders)
        logging.info("DICOM files saved successfully")
        STEP_COMPLETION_QUEUE.append("dicom")  # Notify SSE after step 1 completes

        # Creating NIFTI
        for folder in folders:
            dicom2nifti.dicom_series_to_nifti(
                original_dicom_directory=dicom_directory / folder,
                output_file=nifti_directory / f"{folder}.nii.gz"
            )
        logging.info("NIFTI conversion completed")
        STEP_COMPLETION_QUEUE.append("nifti")  # Notify SSE after step 2 completes

        # Freesurfer Recon-all
        reconall(base_dir=str(base_path.absolute()))
        logging.info("FreeSurfer recon-all processing completed")
        STEP_COMPLETION_QUEUE.append("recon")  # Notify SSE after step 3 completes

        # Freesurfer SAMSEG
        for folder in folders:
            process_lesions(freesurfer_path=freesurfer_path, samseg_path=samseg_path, series=folder)
        logging.info("SAMSEG processing completed")
        STEP_COMPLETION_QUEUE.append("lesions")  # Notify SSE after step 4 completes

        # Freesurfer subcortical
        for folder in folders:
            for structure in ["thalamus", "brainstem", "hippo-amygdala"]:
                segment_subregions(structure=structure, subject_id=folder, subject_dir=base_path / "FREESURFER")
                logging.info(f"{structure} segmentation completed")

        logging.info("Subcortical segmentation completed")
        STEP_COMPLETION_QUEUE.append("subs1")  # Notify SSE after step 5 completes

        # Fastsurfer subcortical
        if platform == "darwin":
            os.environ["PYTORCH_ENABLE_MPS_FALLBACK"] = "1"

        for folder in folders:
            run_fastsurfer(
                fs_dir=Path.home() / "FastSurfer",
                t1=freesurfer_path.absolute() / folder / "mri" / "T1.mgz",
                sid=folder,
                sd=fastsurfer_path.absolute(),
                wf_dir=workflows_path,
                parallel=True,
                threads=os.cpu_count()
            )

        logging.info("Extra subcortical segmentation completed")
        STEP_COMPLETION_QUEUE.append("subs2")  # Notify SSE after step 6 completes

        # Creating JSON Files
        for folder in folders:
            (json_folder / folder).mkdir(parents=True, exist_ok=True)
            run_jsonifier(
                freesurfer_path=freesurfer_path / folder,
                fastsurfer_path=fastsurfer_path / folder,
                samseg_path=samseg_path / folder,
                output_folder=json_folder / folder
            )

        # Creating JSON Averages
        (json_folder / "AVERAGES").mkdir(parents=True, exist_ok=True)
        run_json_average(json_path=json_folder, folders=folders, main_type="cortical.json")
        run_json_average(json_path=json_folder, folders=folders, main_type="subcortical.json")
        run_json_average(json_path=json_folder, folders=folders, main_type="general.json")

        run_global_json(folders=folders)

        logging.info("JSON files generation completed")
        STEP_COMPLETION_QUEUE.append("json")  # Notify SSE after step 7 completes

        return "done", 200
    except Exception as e:
        logging.error(f"Error during script execution: {e}")
        return jsonify({"error": "Processing failed"}), 500


@app.route("/cortical")
def cortical():
    try:
        with open(file="./DATA/ST1/JSON/cortical.json", mode="r") as f:
            cortical = json.load(fp=f)
        return jsonify(cortical)
    except FileNotFoundError as e:
        print(e)
        return "No Data"


@app.route("/subcortical")
def subcortical():
    try:
        with open(file="./DATA/ST1/JSON/subcortical.json", mode="r") as f:
            subcortical = json.load(fp=f)
        return jsonify(subcortical)
    except FileNotFoundError as e:
        print(e)
        return "No Data"


@app.route("/general")
def general():
    try:
        with open(file="./DATA/ST1/JSON/general.json", mode="r") as f:
            general = json.load(fp=f)
        return jsonify(general)
    except FileNotFoundError as e:
        print(e)
        return "No Data"


@app.route("/series")
def series():
    dicoms = Path("./DATA/ST1/DICOM")
    series_list = get_folder_names(directory=dicoms)
    series_dict = {s: get_nifti_dimensions(file_path=Path(f"./DATA/ST1/NIFTI/{s}.nii.gz")) for s in series_list}
    return jsonify(series_dict)


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5001)
