import pathlib

from flask import Flask, jsonify, request, Response
from flask_cors import CORS
from pathlib import Path
import dicom2nifti
import os
from utils import add_dcm_extension, freesurfer, segment_subregions, run_fastsurfer
from jsonifier import run_jsonifier
import logging
from sys import platform

app = Flask(__name__)
CORS(app, supports_credentials=True)
logging.basicConfig(level=logging.INFO)

@app.route("/")
def home() -> str:
    return "Home"

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
    series = request.form.get("series")
    if not series:
        return jsonify({"error": "Series not provided"}), 400

    base_path = Path("./DATA")
    dicom_directory = base_path / "DICOM"
    nifti_directory = base_path / "NIFTI" / series
    json_folder = base_path / "JSON" / series
    freesurfer_path = base_path / "FREESURFER" / series
    fastsurfer_path = base_path / "FASTSURFER"
    workflows_path = base_path / "WORKFLOWS"

    workflows_path.mkdir(parents=True, exist_ok=True)

    try:
        dicom_directory.mkdir(parents=True, exist_ok=True)
        for file in request.files.getlist("dicoms"):
            file.save(dicom_directory / add_dcm_extension(os.path.basename(file.filename)))

        logging.info("DICOM files saved successfully")

        nifti_directory.mkdir(parents=True, exist_ok=True)
        dicom2nifti.dicom_series_to_nifti(
            original_dicom_directory=dicom_directory,
            output_file=nifti_directory / "struct.nii.gz"
        )
        logging.info("NIFTI conversion completed")

        freesurfer(str(base_path.absolute()), series)
        logging.info("FreeSurfer processing completed")

        for structure in ["thalamus", "brainstem", "hippo-amygdala"]:
            segment_subregions(structure=structure, subject_id=series, subject_dir=base_path / "FREESURFER")
            logging.info(f"{structure} segmentation completed")
        
        # segment_hypothalamus(subject_id=series, subject_dir=str((base_path / "FREESURFER").absolute()))
        # logging.info("Hypothalamus segmentation completed")

        fastsurfer_path.mkdir(parents=True, exist_ok=True)

        if platform == "darwin":
            os.environ["PYTORCH_ENABLE_MPS_FALLBACK"] = "1"

        # Set up the FastSurfer node with inputs
        run_fastsurfer(
            fs_dir=pathlib.Path.home() / "FastSurfer",
            t1=freesurfer_path.absolute() / "mri" / "T1.mgz",
            sid=series,
            sd=fastsurfer_path.absolute(),
            wf_dir=workflows_path,
            parallel=True,
            threads=os.cpu_count()
        )


        json_folder.mkdir(parents=True, exist_ok=True)
        run_jsonifier(fs_subject_folder=freesurfer_path, output_folder=json_folder)
        logging.info("JSON file generation completed")

        return "done", 200
    except Exception as e:
        logging.error(f"Error during script execution: {e}")
        return jsonify({"error": "Processing failed"}), 500

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5001)