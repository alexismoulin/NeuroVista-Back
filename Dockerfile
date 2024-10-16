FROM ubuntu:22.04

# Basic installs and updates
RUN apt-get update && \
    apt-get install -y wget python3-pip && \
    apt-get upgrade -y && \
    apt-get clean

# Install and configure FreeSurfer
RUN wget https://surfer.nmr.mgh.harvard.edu/pub/dist/freesurfer/7.4.1/freesurfer_ubuntu22-7.4.1_amd64.deb && \
    apt-get install -y ./freesurfer_ubuntu22-7.4.1_amd64.deb && \
    rm -f ./freesurfer_ubuntu22-7.4.1_amd64.deb

# Set environment variables for FreeSurfer
ENV FS_LICENSE=$HOME/license.txt
ENV FREESURFER_HOME=/usr/local/freesurfer/7.4.1
RUN echo "source $FREESURFER_HOME/SetUpFreeSurfer.sh" >> $HOME/.bashrc

# Install Python libraries
RUN pip install dicom2nifti Flask Flask-Cors nipype

# Expose port for Flask
EXPOSE 5000

# Copy application files and set the working directory
COPY . /root
WORKDIR /root

# Start Flask server
CMD ["flask", "run", "--host=0.0.0.0"]