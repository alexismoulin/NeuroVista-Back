FROM ubuntu:22.04

# Basic installs
RUN apt-get update
RUN apt-get install -y wget python3-pip

# Install & Config de Freesurfer
RUN wget https://surfer.nmr.mgh.harvard.edu/pub/dist/freesurfer/7.4.1/freesurfer_ubuntu22-7.4.1_amd64.deb
RUN apt-get install -y ./freesurfer_ubuntu22-7.4.1_amd64.deb
RUN echo 'export FS_LICENSE=$HOME/license.txt' >> $HOME/.bashrc \
&& echo 'export FREESURFER_HOME=/usr/local/freesurfer/7.4.1' >> $HOME/.bashrc \
&& echo 'source $FREESURFER_HOME/SetUpFreeSurfer.sh' >> $HOME/.bashrc \
&& rm -f ./freesurfer_ubuntu22-7.4.1_amd64.deb

# Ubuntu updates
RUN apt-get update -y
RUN apt-get upgrade -y

# Installation of Python libraries on another layer
RUN pip install dicom2nifti Flask Flask-Cors

#Port 5000 pour flask
EXPOSE 5000

# files addition
COPY . /root

WORKDIR /root

# Server launch
CMD ["flask", "run", "--host", "0.0.0.0"]
