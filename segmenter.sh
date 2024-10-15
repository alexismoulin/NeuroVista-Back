#!/bin/bash

series=$1

function segment() {
    # Hypothalamus
    echo "Hypothalamic segmentation for $1"
    if [ -f "$series/mri/hypothalamic_subunits_seg.v1.mgz" ]; then
        echo "$series/mri/hypothalamic_subunits_seg.v1.mgz already exist - skipping"
    else
        mri_segment_hypothalamic_subunits --s "$1"
    fi
    # Thalamus
    echo "Thalamus nuclei segmentation for $1"
    if [ -f "$series/mri/ThalamicNuclei.mgz" ]; then
        echo "$series/mri/ThalamicNuclei.mgz already exist - skipping"
    else
        segment_subregions thalamus --cross "$1"
    fi
    # Brain stem
    echo "Brainstem structures segmentation for $1"
    if [ -f "$series/mri/brainstemSsLabels.mgz" ]; then
        echo "$series/mri/brainstemSsLabels.mgz already exist - skipping"
    else
        segment_subregions brainstem --cross "$1"
    fi
    # Hippocampus and Amygdala
    echo "Segmentation of hippocampal subfields and nuclei of the amygdala for $1"
    if [ -f "$series/mri/lh.hippoAmygLabels.mgz" ] && [ -f "$series/mri/rh.hippoAmygLabels.mgz" ]; then
        echo "$series/mri/lh.hippoAmygLabels.mgz and $series/mri/rh.hippoAmygLabels.mgz already exist - skipping"
    else
        segment_subregions hippo-amygdala --cross "$1"
    fi
}

segment "$1"
