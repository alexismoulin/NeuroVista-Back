#!/bin/bash

subjects=$FREESURFER_HOME/subjects

function segment() {
    test_serie=$subjects/$1
    # Hypothalamus
    echo "Hypothalamic segmentation for $1"
    if [ -f "$test_serie/mri/hypothalamic_subunits_seg.v1.mgz" ]; then
        echo "$test_serie/mri/hypothalamic_subunits_seg.v1.mgz already exist - skipping"
    else
        mri_segment_hypothalamic_subunits --s "$1"
    fi
    # Thalamus
    echo "Thalamus nuclei segmentation for $1"
    if [ -f "$test_serie/mri/ThalamicNuclei.mgz" ]; then
        echo "$test_serie/mri/ThalamicNuclei.mgz already exist - skipping"
    else
        segment_subregions thalamus --cross "$1"
    fi
    # Brain stem
    echo "Brainstem structures segmentation for $1"
    if [ -f "$test_serie/mri/brainstemSsLabels.mgz" ]; then
        echo "$test_serie/mri/brainstemSsLabels.mgz already exist - skipping"
    else
        segment_subregions brainstem --cross "$1"
    fi
    # Hippocampus and Amygalia
    echo "Segmentation of hippocampal subfields and nuclei of the amygdala for $1"
    if [ -f "$test_serie/mri/lh.hippoAmygLabels.mgz" ] && [ -f "$test_serie/mri/rh.hippoAmygLabels.mgz" ]; then
        echo "$test_serie/mri/lh.hippoAmygLabels.mgz and $test_serie/mri/rh.hippoAmygLabels.mgz already exist - skipping"
    else
        segment_subregions hippo-amygdala --cross "$1"
    fi
}

segment "$1"
