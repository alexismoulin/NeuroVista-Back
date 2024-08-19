#!/bin/bash
set -e

uploaded_folder_name=$1
mkdir -p "DICOM/$uploaded_folder_name"
mv "$uploaded_folder_name/dicom_directory/"* "DICOM/$uploaded_folder_name"
echo "Dicom move completed"
python3 nifti.py -i="$uploaded_folder_name"
echo "nifti creation completed"
export FS_LICENSE=$HOME/license.txt
export FREESURFER_HOME=/usr/local/freesurfer/7.4.1
source $FREESURFER_HOME/SetUpFreeSurfer.sh
recon-all -s "$uploaded_folder_name" -i "NIFTI/$uploaded_folder_name".nii -all -qcache
echo "recon-all completed"
./segmenter.sh "$uploaded_folder_name"
echo "subcortical segmentation completed"
python3 jsonifier.py -i="$uploaded_folder_name"
echo "json files created"
