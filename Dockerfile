# Use Ubuntu 22.04 as the base image
FROM ubuntu:22.04

# Use an ARG for FreeSurfer version (can be overridden at build time)
ARG FS_VERSION=7.4.1

# Set working directory early
WORKDIR /root

# Combine apt-get commands into a single RUN for efficiency
RUN apt-get update && \
    apt-get install -y wget python3-pip && \
    apt-get upgrade -y && \
    rm -rf /var/lib/apt/lists/*

# Download, install, and remove FreeSurfer package in one layer
RUN wget https://surfer.nmr.mgh.harvard.edu/pub/dist/freesurfer/${FS_VERSION}/freesurfer_ubuntu22-${FS_VERSION}_amd64.deb && \
    apt-get update && apt-get install -y ./freesurfer_ubuntu22-${FS_VERSION}_amd64.deb && \
    rm -f freesurfer_ubuntu22-${FS_VERSION}_amd64.deb && \
    rm -rf /var/lib/apt/lists/*

# Copy only the requirements and licence files first for better caching
ENV FREESURFER_HOME=/usr/local/freesurfer/${FS_VERSION}
COPY requirements.txt /root/
COPY license.txt $FREESURFER_HOME

# Install Python libraries
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application files
COPY . /root

# Expose the port used by the Flask app
EXPOSE 5001

# Start the application by sourcing FreeSurfer setup and running Flask
CMD ["bash", "-c", "source $FREESURFER_HOME/SetUpFreeSurfer.sh && python3.10 app.py"]
