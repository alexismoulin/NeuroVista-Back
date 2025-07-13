from flask import Flask, jsonify, request, Response, make_response, stream_with_context
from flask_cors import CORS
import logging
import queue
from core.utils import sanitize_name, list_folder_subfolders
from core.processing import STEP_COMPLETION_QUEUE, BASE_DATA_PATH, processing_event, run_processing, read_json_file
from typing import Tuple

app = Flask(__name__)
CORS(app, supports_credentials=True)
logger = logging.getLogger(__name__)


def serve_json(patient: str, study: str, filename: str, err_msg: str) -> Tuple[Response, int]:
    path = BASE_DATA_PATH / sanitize_name(patient) / sanitize_name(study) / "JSON" / filename
    data = read_json_file(path)
    if data:
        return jsonify(data), 200
    return jsonify({"error": err_msg}), 404


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
                break

    headers = {"Cache-Control": "no-cache"}
    return Response(event_stream(), headers=headers, mimetype="text/event-stream")


@app.post("/run_script")
def run_script() -> Response:
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
    couples = list_folder_subfolders(directory_path=BASE_DATA_PATH)
    if couples:
        logger.info("Found study/patient couples: %s", couples)
        response = make_response(jsonify(couples))
        response.status_code = 200
        return response
    else:
        logger.info("No data found")
        response = make_response(jsonify({"error": "No Data"}))
        response.status_code = 404
        return response


@app.get("/cortical/<patient>/<study>")
def cortical(patient: str, study: str) -> Tuple[Response, int]:
    return serve_json(patient, study, filename="cortical.json", err_msg="No cortical data")


@app.get("/subcortical/<patient>/<study>")
def subcortical(patient: str, study: str) -> Tuple[Response, int]:
    return serve_json(patient, study, filename="subcortical.json", err_msg="No subcortical data")


@app.get("/general/<patient>/<study>")
def general(patient: str, study: str) -> Tuple[Response, int]:
    return serve_json(patient, study, filename="general.json", err_msg="No general data")


@app.get("/series/<patient>/<study>")
def get_nifti_dim(patient: str, study: str) -> Tuple[Response, int]:
    return serve_json(patient, study, filename="niftiDimensions.json", err_msg="No nifti dimension data")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001)
