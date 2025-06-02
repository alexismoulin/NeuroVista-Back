import queue
import time

from flask import Blueprint, request, Response, jsonify, current_app, stream_with_context
from core.utils import sanitize_name
from core.processing import STEP_COMPLETION_QUEUE, processing_event, run_processing, prepare_processing, create_folders, save_dicoms
from typing import Generator, Tuple
from threading import Thread

processing_bp = Blueprint(name="processing", import_name=__name__)

@processing_bp.route("/stream", methods=["GET"])
def stream() -> Response:
    """
    Create a Server-Sent Events (SSE) endpoint that continuously streams progress updates.

    Internally, this function defines an `event_stream` generator which:
    1. Waits for new messages on STEP_COMPLETION_QUEUE with a 1-second timeout.
       - If a step completion message is received, it yields it as an SSE-formatted string (`data: <step>\n\n`).
       - If the queue is empty for 1 second, it yields a heartbeat event (`data: heartbeat\n\n`) to keep the connection alive.
       - On any unexpected exception, logs the error and breaks out of the loop.

    The outer function wraps `event_stream` with `stream_with_context` so that Flask’s request context is
    preserved during streaming. The response is returned with:
      - `mimetype="text/event-stream"` to indicate SSE payload.
      - `Cache-Control: no-cache` header to prevent proxy caching.

    Returns:
        Response: A Flask `Response` object configured for SSE streaming.
    """
    def event_stream() -> Generator[str, None, None]:
        # Loop as long as a job is in progress
        while processing_event.is_set():
            try:
                # Wait up to 1 s for a real step
                step = STEP_COMPLETION_QUEUE.get(timeout=1)
                yield f"data: {step}\n\n"
            except queue.Empty:
                # No real step arrived – send a “comment” heartbeat to keep the socket alive
                yield ": heartbeat\n\n"
                # Small sleep to avoid a tight loop
                time.sleep(0.1)
        # Once processing_event is cleared, send a final “done” event and close
        yield "data: [DONE]\n\n"

    headers = {"Cache-Control": "no-cache"}
    return Response(
        stream_with_context(event_stream()),
        mimetype="text/event-stream",
        headers=headers
    )

@processing_bp.route("/run_script", methods=["POST"])
def run_script() -> Tuple[Response, int]:
    """
    Expects multipart/form-data with:
      - form['study'] (string)
      - form['patient'] (string)
      - files under “dicoms” (the DICOM files)

    STEP 1: synchronously create folders & save DICOMs to disk.
    STEP 2: spawn a background Thread to run the rest.
    """
    if processing_event.is_set():
        return jsonify(error="Processing already in progress"), 400

    study = sanitize_name(request.form.get(key="study", default=""))
    patient = sanitize_name(request.form.get(key="patient", default=""))
    files = request.files.getlist(key="dicoms")
    if not study or not patient:
        return jsonify(error="Study and patient name are required"), 400
    if not files:
        return jsonify(error="No DICOM files provided"), 400

    base_path = current_app.config["BASE_DATA_PATH"] / patient / study

    # === STEP 1 (synchronous): create folders & save DICOMs now, while files are still open ===
    try:
        # prepare_processing will create subfolders and save all DICOMs under base_path/DICOM/...
        folders_dict = prepare_processing(base_path=base_path, request_files=request.files)
    except Exception as e:
        # If saving DICOMs fails, return a 500 and do NOT set processing_event
        return jsonify(error=f"Failed to save DICOMs: {e}"), 500

    # Mark that “processing is in progress” (so no parallel jobs)
    processing_event.set()

    # === STEP 2: kick off the rest of the pipeline (NIfTI → recon‐all → JSON, etc.) in the background ===
    processing_event.set()

    worker = Thread(target=run_processing, args=(base_path, folders_dict))
    worker.daemon = True
    worker.start()

    return jsonify(message="Processing started"), 202
