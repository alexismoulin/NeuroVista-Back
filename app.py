from flask import Flask, render_template, request
import subprocess
from pathlib import Path

app = Flask(__name__)


@app.route("/")
def home() -> str:
    return render_template("index.html")


@app.route("/run_script", methods=["POST"])
def run_script() -> str:
    _ = request.form["subject"]  # To be done afterward
    series = request.form["series"]
    dicom_directory = Path(f"./{series}/dicom_directory")
    dicom_directory.mkdir(parents=True, exist_ok=True)
    for file in request.files.getlist("dicoms"):
        file.save(dst=dicom_directory / file.filename)
    bash_script = f"./routine.sh {series}"
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
    app.run(debug=True)
