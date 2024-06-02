FROM ubuntu:22.04

SHELL ["/bin/bash", "-ec"]

# Update ubuntu and istall wget
RUN apt-get update -yq && apt-get install -y python3-pip wget

# Install & Config de Freeseurfer
RUN wget https://surfer.nmr.mgh.harvard.edu/pub/dist/freesurfer/7.4.1/freesurfer_ubuntu22-7.4.1_amd64.deb \
&& apt-get install ./freesurfer_ubuntu22-7.4.1_amd64.deb -yq \
&& echo 'export FS_LICENSE=$HOME/license.txt' >> $HOME/.bashrc \
&& echo 'export FREESURFER_HOME=/usr/local/freesurfer/7.4.1' >> $HOME/.bashrc \
&& echo 'source $FREESURFER_HOME/SetUpFreeSurfer.sh' >> $HOME/.bashrc \
&& rm -f ./freesurfer_ubuntu22-7.4.1_amd64.deb

# Installation des bibliotheques Python on another layer
RUN pip install dicom2nifti pandas flask

#Port 5000 pour flask
EXPOSE 5000

# Ajout des fichiers
COPY . .

# Lancement du serveur
CMD ["flask", "run", "--host", "0.0.0.0"]
