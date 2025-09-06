# App layer on top of the cached base
FROM freesurfer_ubuntu22:7.4.1

WORKDIR /app

# Install Python deps first for better caching
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# App code
COPY . /app

# Put the FreeSurfer license into the FreeSurfer folder
ENV FS_LICENSE=/usr/local/freesurfer/7.4.1/license.txt \
    FREESURFER_HOME=/usr/local/freesurfer/7.4.1
COPY license.txt ${FS_LICENSE}

EXPOSE 5001

CMD ["bash","-lc","source ${FREESURFER_HOME}/SetUpFreeSurfer.sh && python3 app.py"]