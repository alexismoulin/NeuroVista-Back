#!/bin/bash
set -e

export FS_LICENSE=$HOME/license.txt
export FREESURFER_HOME=/usr/local/freesurfer/7.4.1
source $FREESURFER_HOME/SetUpFreeSurfer.sh

chmod 777 segmenter.sh
