from flask import Flask, render_template
import subprocess

app = Flask(__name__)


@app.route("/")
def home() -> str:
    return render_template("index.html")


@app.route("/run_script", methods=["POST"])
def run_script():
    subprocess.run("echo coucou", shell=True, executable="/bin/bash")


if __name__ == "__main__":
    app.run(debug=True)
