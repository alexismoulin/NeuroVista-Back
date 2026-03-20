# App layer on top of the cached base
FROM freesurfer_ubuntu24:8.2.0

WORKDIR /app

# Create a virtual environment and use it by default
ENV VIRTUAL_ENV=/app/.venv
ENV PATH="${VIRTUAL_ENV}/bin:${PATH}"

# Install venv support if the base image doesn't already have it
RUN apt-get update && \
    apt-get install -y --no-install-recommends python3-venv && \
    rm -rf /var/lib/apt/lists/*

# Install Python deps first for better caching
COPY requirements.txt .
RUN python3 -m venv ${VIRTUAL_ENV} && \
    pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# App code
COPY . /app

# Put the FreeSurfer license into the FreeSurfer folder
ENV FS_LICENSE=/usr/local/freesurfer/8.2.0/license.txt \
    FREESURFER_HOME=/usr/local/freesurfer/8.2.0

COPY license.txt ${FS_LICENSE}

EXPOSE 5001

CMD ["bash","-lc","source ${FREESURFER_HOME}/SetUpFreeSurfer.sh && python app.py"]