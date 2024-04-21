#!/bin/bash

uploaded_folder_name=SE01_COR_T1_MPR_GADO_UPLOADED
python nifti.py -i="$uploaded_folder_name"
recon-all -s "$uploaded_folder_name" -i "$uploaded_folder_name".nii -all -qcache
./segmenter.sh $uploaded_folder_name