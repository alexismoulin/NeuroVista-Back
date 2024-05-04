#!/bin/bash

uploaded_folder_name=$1
python nifti.py -i="$uploaded_folder_name"
recon-all -s "$uploaded_folder_name" -i "$uploaded_folder_name".nii -all -qcache
./segmenter.sh "$uploaded_folder_name"
python jsonifier.py -i="$uploaded_folder_name"
