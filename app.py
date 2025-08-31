from flask import Flask, jsonify, request, Response, make_response, stream_with_context, send_from_directory
from flask_cors import CORS
import queue
import mimetypes
from core.utils import BASE_DATA_PATH, sanitize_name, list_folder_subfolders, serve_json, logger
from core.processing import STEP_COMPLETION_QUEUE, processing_event, run_processing, prepare_processing
from typing import Tuple
from pathlib import Path

# Create the Flask app
app = Flask(__name__)
CORS(app, supports_credentials=True)

# Add MIME types once at startup
mimetypes.add_type("model/gltf+json", ".gltf")
mimetypes.add_type("model/gltf-binary", ".glb")


@app.get("/")
def home() -> str:
    """
    Basic health/home endpoint.

    Returns
    -------
    str
        Static string ``"Home"`` indicating the service is reachable.
    """
    return "Home - Backend service is reachable"


@app.get("/stream")
def stream() -> Response:
    """
    Stream step-by-step pipeline updates via Server-Sent Events (SSE).

    The stream emits a message whenever a step is placed into the global
    :data:`STEP_COMPLETION_QUEUE`. If no message is available within one
    second, a ``heartbeat`` is sent to keep the connection alive.

    Returns
    -------
    flask.Response
        Streaming response with MIME type ``text/event-stream`` and
        ``Cache-Control: no-cache``.

    Notes
    -----
    The payload format follows SSE conventions: lines prefixed with
    ``"data: "`` and terminated by a blank line.
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
    """
    Validate inputs and kick off the processing pipeline.

    Behavior:
    - Rejects the request with 400 if processing is already in progress.
    - Validates ``patient``, ``study``, and that at least one file is present
      under the ``dicoms`` form field; returns 400 on validation errors.
    - Creates the workspace and persists uploaded DICOMs via
      :func:`prepare_processing`.
    - Executes the processing pipeline via :func:`run_processing`.
    - Returns HTTP 202 with a message on successful start.

    Returns
    -------
    flask.Response
        JSON response with appropriate HTTP status code (400, 202).

    Notes
    -----
    The global :data:`processing_event` flag is set before preparation and
    cleared by the pipeline at the end. Progress can be monitored via the
    ``/stream`` SSE endpoint.
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
    folders_dict = prepare_processing(base_path, request_files=request.files)
    run_processing(base_path, folders_dict)
    response = make_response(jsonify({"message": "Processing started"}))
    response.status_code = 202
    return response


@app.get("/studies")
def studies() -> Response:
    """
    List all available ``(patient, study)`` pairs found on disk.

    Returns
    -------
    flask.Response
        200 with a JSON array of pairs on success, or 404 with an error
        payload when no data is found.

    Notes
    -----
    Uses :func:`list_folder_subfolders` rooted at :data:`BASE_DATA_PATH`.
    """
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


@app.get("/cortical/<string:patient>/<string:study>")
def cortical(patient: str, study: str) -> Tuple[Response, int]:
    """
    Retrieve cortical metrics JSON for a given patient/study.

    Parameters
    ----------
    patient : str
        Patient identifier.
    study : str
        Study identifier.

    Returns
    -------
    (flask.Response, int)
        JSON payload and HTTP status code (200 or 404).
    """
    return serve_json(patient, study, filename="cortical.json", err_msg="No cortical data")


@app.get("/subcortical/<string:patient>/<string:study>")
def subcortical(patient: str, study: str) -> Tuple[Response, int]:
    """
    Retrieve subcortical metrics JSON for a given patient/study.

    Parameters
    ----------
    patient : str
        Patient identifier.
    study : str
        Study identifier.

    Returns
    -------
    (flask.Response, int)
        JSON payload and HTTP status code (200 or 404).
    """
    return serve_json(patient, study, filename="subcortical.json", err_msg="No subcortical data")


@app.get("/general/<string:patient>/<string:study>")
def general(patient: str, study: str) -> Tuple[Response, int]:
    """
    Retrieve general metrics JSON for a given patient/study.

    Parameters
    ----------
    patient : str
        Patient identifier.
    study : str
        Study identifier.

    Returns
    -------
    (flask.Response, int)
        JSON payload and HTTP status code (200 or 404).
    """
    return serve_json(patient, study, filename="general.json", err_msg="No general data")


@app.get("/nifti_dim/<string:patient>/<string:study>")
def nifti_dim(patient: str, study: str) -> Tuple[Response, int]:
    """
    Retrieve NIfTI dimensions JSON for a given patient/study.

    Parameters
    ----------
    patient : str
        Patient identifier.
    study : str
        Study identifier.

    Returns
    -------
    (flask.Response, int)
        JSON payload and HTTP status code (200 or 404).
    """
    return serve_json(patient, study, filename="niftiDimensions.json", err_msg="No nifti dimension data")


@app.route("/models/<string:patient>/<string:study>/<string:series>/<string:filename>")
def serve_model(patient: str, study: str, series:str, filename: str) -> Response:
    """
    Serve a generated 3D model (``.gltf``/``.glb``) for a patient/study/series.

    The resolved path is:
    ``<CWD>/<BASE_DATA_PATH>/<patient>/<study>/VIEWER/<series>/<filename>``.

    A safety check ensures the requested file exists and is a child of the
    expected directory to avoid directory traversal.

    Parameters
    ----------
    patient : str
        Patient identifier.
    study : str
        Study identifier.
    series : str
        Series identifier (viewer subfolder).
    filename : str
        Model filename to serve (``.gltf`` or ``.glb``).

    Returns
    -------
    flask.Response
        404 JSON error if the file is missing or invalid; otherwise a file
        response with conditional requests enabled and a small ``max_age``.

    Notes
    -----
    The correct GLTF MIME types are registered at module import time via
    :mod:`mimetypes`.
    """
    path = Path.cwd() / BASE_DATA_PATH / sanitize_name(patient) / sanitize_name(study) / "VIEWER" / sanitize_name(series)
    requested = (path / filename).resolve()

    if not requested.is_file() or path not in requested.parents:
        logger.info(f"No model found @ {path / filename}")
        response = make_response(jsonify({"error": f"No model found @ {path / filename}"}))
        response.status_code = 404
        return response

    return send_from_directory(
        directory=path,
        path=filename,
        conditional=True,
        max_age=10
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001)
