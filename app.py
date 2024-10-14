from flask import Flask, Response, stream_with_context, jsonify, request
from flask_cors import CORS
import subprocess
from pathlib import Path
import dicom2nifti
import os
from utils import add_dcm_extension
from jsonifier import run_jsonifier

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


@app.route("/run_script", methods=["POST"])
def run_script() -> str:

    subject = request.form["subject"]
    study = "ST1"  # To be done afterwards
    series = request.form["series"]
    base_path = Path(f"./SUBJECTS/{subject}/{study}/{series}")
    
    # Save dicoms on server
    dicom_directory = base_path / "DICOM"
    dicom_directory.mkdir(parents=True, exist_ok=True)
    for file in request.files.getlist("dicoms"):
        renamed_file = add_dcm_extension(filename=os.path.basename(file.filename))
        file.save(dst=dicom_directory / renamed_file)
    
    print({"dicom": True})
    
    # Process NIFTI
    nifti_directory = base_path / "NIFTI"
    nifti_directory.mkdir(parents=True, exist_ok=True)
    dicom2nifti.dicom_series_to_nifti(
        original_dicom_directory=dicom_directory,
        output_file=nifti_directory / f"nifti.nii"
    )
    
    print({"nifti": True})
    
    # FreeSurfer
    bash_script = f"./routine.sh {subject} {study} {series}"
    p = subprocess.run(args=bash_script, shell=True, executable="/bin/bash")
    if p == 0:
        print({"freesurfer": True})
    else:
        raise Exception( f'Freesurfer crash: { p.returncode }' )

    # Jsonifier
    run_jsonifier(fs_subject_folder=subject, output_folder=base_path)
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
