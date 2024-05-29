from flask import Flask, render_template, request
import subprocess

app = Flask(__name__)


@app.route("/")
def home() -> str:
    return render_template("index.html")


@app.route("/run_script", methods=["POST"])
def run_script() -> str:
    subject = request.form["subject"]
    series = request.form["series"]
    notes = request.form["notes"]
    dicom_files = request.form["dicoms"]
    subprocess.run(f'echo "coucou {subject} {series}"', shell=True, executable="/bin/bash")
    return "tu veux voir"


if __name__ == "__main__":
    app.run(debug=True)
