#!/bin/bash
set -e

subject=$1
study=$2
series=$3
uploaded_folder_name="./SUBJECTS/$subject/$study/$series"
echo $uploaded_folder_name

export FS_LICENSE=$HOME/license.txt
export FREESURFER_HOME=/usr/local/freesurfer/7.4.1
source $FREESURFER_HOME/SetUpFreeSurfer.sh
recon-all -s "$subject" -i "$uploaded_folder_name/NIFTI/nifti".nii -all -qcache
echo "recon-all completed"

chmod 777 segmenter.sh
./segmenter.sh "$uploaded_folder_name"
echo "subcortical segmentation completed"
