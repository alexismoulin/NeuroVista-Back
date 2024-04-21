FROM ubuntu:22.04

RUN apt-get update -yq \
# Install & Config de Freeseurfer
&& wget https://surfer.nmr.mgh.harvard.edu/pub/dist/freesurfer/7.4.1/freesurfer_ubuntu22-7.4.1_amd64.deb \
&& chmod 755 $HOME \
&& sudo apt-get install ./freesurfer_ubuntu22-7.4.1_amd64.deb -yq \
&& echo 'export FS_LICENSE=$HOME/license.txt' >> $HOME/.bashrc \
&& echo 'export FREESURFER_HOME=/usr/local/freesurfer/7.4.1' >> $HOME/.bashrc \
&& echo 'source $FREESURFER_HOME/SetUpFreeSurfer.sh' >> $HOME/.bashrc \
&& rm -f ./freesurfer_ubuntu22-7.4.1_amd64.deb \
# Installation des bibliotheques Python
pip install dicom2nifti pandas

ADD license.txt /usr/local/freesurfer/7.4.1/