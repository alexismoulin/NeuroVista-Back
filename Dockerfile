FROM ubuntu:22.04

# Basic installs and updates
RUN apt-get update && \
    apt-get install -y wget python3-pip && \
    apt-get upgrade -y && \
    apt-get clean

# Install and configure FreeSurfer 7.4.1
RUN wget https://surfer.nmr.mgh.harvard.edu/pub/dist/freesurfer/7.4.1/freesurfer_ubuntu22-7.4.1_amd64.deb
RUN apt-get install -y ./freesurfer_ubuntu22-7.4.1_amd64.deb
RUN rm -f ./freesurfer_ubuntu22-7.4.1_amd64.deb

# Set environment variables for FreeSurfer
ENV FS_LICENSE=/root/license.txt
ENV FREESURFER_HOME=/usr/local/freesurfer/7.4.1
ENV PATH=$FREESURFER_HOME/bin:$PATH

# Install Python libraries
RUN pip install -r requirements.txt

# Install FastSurfer 2.4.2 and dependencies
RUN apt-get install -y git ca-certificates file

RUN wget https://github.com/Deep-MI/FastSurfer/archive/refs/tags/v2.4.2.tar.gz
RUN tar -xvzf v2.4.2.tar.gz
RUN rm -rf v2.4.2.tar.gz
RUN mv FastSurfer-2.4.2 /root/FastSurfer

# Copy application files and set the working directory
COPY . /root
WORKDIR /root

# Expose port for Flask
EXPOSE 5001

# Start Flask server
CMD ["bash", "-c", "source $FREESURFER_HOME/SetUpFreeSurfer.sh && python3.10 app.py"]
