from flask import Flask, request, jsonify
from flask_cors import CORS
import subprocess
from pathlib import Path

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
            file_names.append(file.filename)

    # Prepare the response
    response = {
        'form_data': form_data,
        'file_names': file_names
    }

    return jsonify(response)


@app.route("/run_script", methods=["POST"])
def run_script() -> str:
    subject = request.form["subject"]  # To be done afterward
    study = request.form["study"]  # To be done afterward
    series = request.form["series"]
    dicom_directory = Path(f"./{series}/dicom_directory")
    dicom_directory.mkdir(parents=True, exist_ok=True)
    for file in request.files.getlist("dicoms"):
        file.save(dst=dicom_directory / file.filename)
    bash_script = f"./routine.sh {subject} {study} {series}"
    subprocess.run(args=bash_script, shell=True, executable="/bin/bash")
    return 'Files uploaded successfully'


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
