# NeuroVista (Backend)

## 1. On device run

You can use uv with Python 3.12

```shell
uv python install 3.12
uv init --bare
uv add -r requirements.txt
uv run app.py 
``` 

## 2. Docker image

To build the base image, first run the command:

```shell
docker build -f Dockerfile.base -t freesurfer_ubuntu24:8.2.0 --platform linux/amd64 .
``` 

This will create a base image running on:
- Ubuntu 24.04
- Python 3.12
- FreeSurfer 8.2.0

Then you can build the final image with the command:

```shell
docker build -t neurovista_back:0.2.0 --platform linux/amd64 .
``` 
NB: You need at least 32GB of RAM to run the pipeline 
(32GB available in your containerized environment if you use the Docker Image)