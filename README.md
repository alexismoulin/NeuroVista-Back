# NeuroVista

To build the image, first run the command:

```shell
docker build -f Dockerfile.base -t freesurfer_ubuntu22:7.4.1 --platform linux/amd64 .
``` 

This will create a base image running on:
- Ubuntu 22.04
- Python 3.10
- FreeSurfer 7.4.1

Then you can build the final image with the command:

```shell
docker build -t neurovista_back:0.2.0 --platform linux/amd64 .
``` 
