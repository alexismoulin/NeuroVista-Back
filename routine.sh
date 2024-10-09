#!/bin/bash
set -e

uploaded_folder_name=$3

export FS_LICENSE=$HOME/license.txt
export FREESURFER_HOME=/usr/local/freesurfer/7.4.1
source $FREESURFER_HOME/SetUpFreeSurfer.sh
recon-all -s "$uploaded_folder_name" -i "NIFTI/$uploaded_folder_name".nii -all -qcache
echo "recon-all completed"

chmod 777 segmenter.sh
./segmenter.sh "$uploaded_folder_name"
echo "subcortical segmentation completed"

python3 jsonifier.py -i="$uploaded_folder_name"
echo "json files created"
