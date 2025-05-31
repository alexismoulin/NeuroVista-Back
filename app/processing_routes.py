import queue
from flask import Blueprint, request, Response, jsonify, current_app, stream_with_context
from core.utils import sanitize_name
from core.processing import STEP_COMPLETION_QUEUE, processing_event, run_processing
from typing import Generator, Tuple

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
        while True:
            try:
                step = STEP_COMPLETION_QUEUE.get(timeout=1)
                yield f"data: {step}\n\n"
            except queue.Empty:
                # Send a heartbeat to keep the SSE connection alive
                yield "data: heartbeat\n\n"
            except Exception as e:
                current_app.logger.error(f"Error in SSE stream: {e}")
                break

    headers = {"Cache-Control": "no-cache"}
    return Response(
        stream_with_context(event_stream()),
        mimetype="text/event-stream",
        headers=headers
    )

@processing_bp.route("/run_script", methods=["POST"])
def run_script() -> Tuple[Response, int]:
    """
    Trigger backend processing if no other processing event is active.
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
    processing_event.set()
    run_processing(base_path=base_path, request_files=request.files)
    return jsonify(message="Processing started"), 202
