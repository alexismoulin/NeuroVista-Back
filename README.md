# NeuroVista

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
