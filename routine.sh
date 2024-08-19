#!/bin/bash
set -e

uploaded_folder_name=$1
python3 nifti.py -i="$uploaded_folder_name"
echo "nifti creation completed"
recon-all -s "$uploaded_folder_name" -i "$uploaded_folder_name".nii -all -qcache
echo "recon-all completed"
./segmenter.sh "$uploaded_folder_name"
echo "subcortical segmentation completed"
python3 jsonifier.py -i="$uploaded_folder_name"
echo "json files created"
