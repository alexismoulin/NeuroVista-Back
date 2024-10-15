from flask import Flask, Response, stream_with_context, jsonify, request
from flask_cors import CORS
from pathlib import Path
import subprocess
import dicom2nifti
from utils import add_dcm_extension, freesurfer, segment_subregions, segment_hypothalamus
from jsonifier import run_jsonifier
import os

app = Flask(__name__)
CORS(app=app, supports_credentials=True)

@app.route("/")
def home() -> str:
    return "Home"


@app.route('/upload', methods=['POST'])
def upload():
    # Initialize a dictionary to hold the regular data
    form_data = {}

    # Extract regular form data
    for key in request.form:
        form_data[key] = request.form[key]

    # Initialize a list to hold the names of uploaded files
    file_names = []

    # Extract files and store their names
    if 'dicoms' in request.files:
        files = request.files.getlist('dicoms')
        for file in files:
            file_names.append(os.path.basename(file.filename))

    # Prepare the response
    response = {
        'form_data': form_data,
        'file_names': file_names
    }

    return jsonify(response)

@app.post(rule="/test")
def test():
    @stream_with_context
    def generate():
        yield jsonify({"dicom": True})
        yield jsonify({"nifti": True})
        yield jsonify({"recon": True})
        yield jsonify({"subs": True})
        yield jsonify({"json": True})


@app.route("/run_script", methods=["POST"])
def run_script() -> str:

    # subject = request.form["subject"]
    series = request.form["series"]
    base_path = Path("./DATA")
    
    # Save dicoms on server
    dicom_directory = base_path / "DICOM"
    dicom_directory.mkdir(parents=True, exist_ok=True)
    for file in request.files.getlist("dicoms"):
        renamed_file = add_dcm_extension(filename=os.path.basename(file.filename))
        file.save(dst=dicom_directory / renamed_file)
    
    print({"dicom": True})
    
    # Process NIFTI
    t1_identifier = "struct.nii.gz"
    nifti_directory = base_path / "NIFTI" / series
    nifti_directory.mkdir(parents=True, exist_ok=True)
    dicom2nifti.dicom_series_to_nifti(
        original_dicom_directory=dicom_directory,
        output_file=nifti_directory / t1_identifier
    )
    
    print({"nifti": True})
    
    # FreeSurfer recon all
    experiment_dir = str(base_path.absolute())
    freesurfer(experiment_dir=experiment_dir, series=series)

    print({"recon": True})

    # FreeSurfer subcortical segmentations
    freesurfer_path = base_path / "FREESURFER" / series
    segment_subregions(structure="thalamus", subject_dir=freesurfer_path)
    segment_subregions(structure="brainstem", subject_dir=freesurfer_path)
    segment_subregions(structure="hippo-amygdala", subject_dir=freesurfer_path)
    segment_hypothalamus(subject_id=series, subject_dir=freesurfer_path)

    print({"subs": True})

    # Jsonifier
    json_folder = base_path / "JSON" / series
    json_folder.mkdir(parents=True, exist_ok=True)
    run_jsonifier(fs_subject_folder=base_path / "FREESURFER" /series, output_folder=json_folder)
    print({"json": True})

    return "done"



@app.route("/output")
def output() -> str:
    try:
        _ = Path("./subcortical.json").read_text()
        _ = Path("./cortical.json").read_text()
        output_text = "Success"
    except FileNotFoundError:
        output_text = "Data processing not yet completed - please wait until completion"
    return output_text


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5001)
