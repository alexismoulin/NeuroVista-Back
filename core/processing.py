import json
import logging
import os
import time
import queue
from configparser import ConfigParser
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from typing import List, Dict, Optional
from functools import partial

import dicom2nifti
import pydicom
from werkzeug.datastructures import ImmutableMultiDict, FileStorage

from core.jsonifier import run_jsonifier, run_json_average, run_global_json
from core.utils import (
    add_dcm_extension,
    get_folder_names,
    create_folders,
    reconall,
    process_lesions,
    segment_subregions,
    segment_hypothalamus
)
from core.viewer import create_gltf_models

logger = logging.getLogger(__name__)

# Read configuration for base data path
config = ConfigParser()
config.read(filenames="./config.ini")
BASE_DATA_PATH = Path(config.get(section="DATA", option="real_data"))

# Shared constants for the processing pipeline
STEP_COMPLETION_QUEUE = queue.Queue()
processing_event = Event()


def notify_step(step: str) -> None:
    """
    Helper function to notify that a processing step is complete.
    """
    STEP_COMPLETION_QUEUE.put(step)

def notify_failure(step: str) -> None:
    """
    Helper function to notify that a processing step has failed.
    It prefixes the step key with 'failed_'.
    """
    notify_step(f"failed_{step}")

def save_dicoms(request_files: ImmutableMultiDict[str, FileStorage], dicom_directory: Path) -> None:
    """
    Save uploaded DICOM files into subdirectories based on their SeriesDescription.
    Only processes DICOM images and skips DICOMDIR files.
    """
    for dicom_file in request_files.getlist("dicoms"):
        try:
            if "DICOMDIR" in dicom_file.filename.upper():
                logger.info("Skipping DICOMDIR file based on filename: %s", dicom_file.filename)
                continue

            dicom_file.stream.seek(0)
            ds = pydicom.dcmread(dicom_file.stream)
            if str(getattr(ds, "SOPClassUID", "")) == str(pydicom.uid.MediaStorageDirectoryStorage):
                logger.info("Skipping DICOMDIR file based on SOPClassUID: %s", dicom_file.filename)
                continue

            series_description = getattr(ds, "SeriesDescription", "UNKNOWN").replace(" ", "_")
            series_dir = dicom_directory / series_description
            series_dir.mkdir(parents=True, exist_ok=True)
            dicom_file.stream.seek(0)
            dest_file = series_dir / add_dcm_extension(os.path.basename(dicom_file.filename))
            dicom_file.save(dst=str(dest_file))
        except Exception as e:
            logger.exception("Skipping file %s due to error: %s", dicom_file.filename, e)
    logger.info("DICOM files saved successfully")

def convert_to_nifti(dicom_directory: Path, nifti_directory: Path) -> None:
    """
    Convert each DICOM series in the dicom_directory to a single NIfTI file.
    """
    for folder in get_folder_names(dicom_directory):
        input_dir = dicom_directory / folder
        output_file = nifti_directory / f"{folder}.nii.gz"
        try:
            dicom2nifti.dicom_series_to_nifti(
                original_dicom_directory=str(input_dir),
                output_file=str(output_file)
            )
        except Exception as e:
            logger.exception("Error converting folder %s: %s", folder, e)
    logger.info("NIFTI conversion completed")

def run_reconall(base_dir: Path) -> None:
    """
    Execute FreeSurfer recon-all process on the given base directory.
    """
    start_time = time.time()
    try:
        reconall(base_dir=base_dir.resolve())
        elapsed = time.time() - start_time
        logger.info("FreeSurfer recon-all completed in %.2f seconds", elapsed)
    except Exception as e:
        logger.exception("Error during FreeSurfer recon-all: %s", e)
        raise

def process_lesions_for_series(series: str, freesurfer_path: Path, samseg_path: Path) -> None:
    """
    Process lesions for a single series.
    """
    try:
        process_lesions(freesurfer_path, samseg_path, series)
    except Exception as e:
        logger.exception("Error processing lesions for series %s: %s", series, e)
        raise

def process_lesions_for_all(folders: List[str], freesurfer_path: Path, samseg_path: Path) -> None:
    """
    Process lesions for all series in parallel.
    """
    with ThreadPoolExecutor(max_workers=max(1, os.cpu_count())) as executor:
        executor.map(partial(process_lesions_for_series, freesurfer_path=freesurfer_path, samseg_path=samseg_path), folders)
    logger.info("SAMSEG processing completed")

def segment_subregions_for_all(folders: List[str], freesurfer_path: Path) -> None:
    """
    Run subcortical segmentation (thalamus, brainstem, hippo-amygdala) for each series.
    """
    for folder in folders:
        for structure in ["thalamus", "brainstem", "hippo-amygdala"]:
            try:
                segment_subregions(structure=structure, subject_id=folder, subject_dir=freesurfer_path)
            except Exception as e:
                logger.exception("Error segmenting %s for series %s: %s", structure, folder, e)
    logger.info("Subcortical segmentation completed")

def segment_hypothalamus_for_all(folders: List[str], freesurfer_path: Path) -> None:
    """
    Run hypothalamus segmentation for each series.
    """
    for folder in folders:
        try:
            segment_hypothalamus(subject_id=folder, subject_dir=freesurfer_path)
        except Exception as e:
            logger.exception("Error segmenting hypothalamus for series %s: %s", folder, e)
    logger.info("FreeSurfer hypothalamus segmentation completed")


def generate_json_files(folders: List[str],
                        freesurfer_path: Path,
                        samseg_path: Path,
                        json_folder: Path) -> None:
    """
    Generate JSON files for each series and compute averages and global metrics.
    """
    for folder in folders:
        output_dir = json_folder / folder
        output_dir.mkdir(parents=True, exist_ok=True)
        try:
            run_jsonifier(
                freesurfer_path=freesurfer_path / folder,
                samseg_path=samseg_path / folder,
                output_folder=output_dir,
            )
        except Exception as e:
            logger.exception("Error generating JSON for series %s: %s", folder, e)
            raise
    averages_dir = json_folder / "AVERAGES"
    averages_dir.mkdir(parents=True, exist_ok=True)
    try:
        run_json_average(json_path=json_folder, folders=folders, main_type="cortical.json")
        run_json_average(json_path=json_folder, folders=folders, main_type="subcortical.json")
        run_json_average(json_path=json_folder, folders=folders, main_type="general.json")
        run_global_json(json_path=json_folder, folders=folders)
    except Exception as e:
        logger.exception("Error generating average/global JSON files: %s", e)
        raise
    logger.info("JSON files generation completed")


def process_viewer(folders: List[str], viewer_path: Path, freesurfer_path: Path):
    for folder in folders:
        output_dir = viewer_path / folder
        output_dir.mkdir(parents=True, exist_ok=True)
        create_gltf_models(freesurfer_path=freesurfer_path, viewer_path=viewer_path, folder=folder)



def prepare_processing(base_path: Path, request_files: ImmutableMultiDict[str, FileStorage]) -> Optional[Dict[str, Path]]:
    """
    1) Create all the subfolders under base_path (dicom, nifti, freesurfer, json, etc.).
    2) Save uploaded DICOMs into base_path/<patient>/<study>/DICOM/<SeriesDescription>/*

    Returns a dict of all the folder paths
    """
    # Create the entire hierarchy on disk
    folders_dict = create_folders(base_path)
    dicom_dir = folders_dict["dicom"]

    # Save the uploaded DICOMs to disk (may raise if something goes wrong)
    try:
        save_dicoms(request_files=request_files, dicom_directory=dicom_dir)
        notify_step(step="dicom")
        return folders_dict
    except Exception as e:
        logger.exception("Error during DICOM saving: %s", e)
        notify_failure("dicom")
        return None


def run_processing(base_path: Path, folders_dict: Dict[str, Path]) -> None:
    """
    Run the complete processing pipeline.
    If a step fails, notify the failure and stop further processing.
    """
    try:
        dicom_dir = folders_dict["dicom"]
        nifti_dir = folders_dict["nifti"]
        fs_path = folders_dict["freesurfer"]
        samseg_path = folders_dict["samseg"]
        json_folder = folders_dict["json"]
        viewer_folder = folders_dict["viewer"]

        series_folders = get_folder_names(dicom_dir)

        try:
            convert_to_nifti(dicom_directory=dicom_dir, nifti_directory=nifti_dir)
            notify_step("nifti")
        except Exception as e:
            logger.exception("Error during NIFTI conversion: %s", e)
            notify_failure("nifti")
            return

        try:
            run_reconall(base_dir=base_path)
            notify_step("recon")
        except Exception as e:
            logger.exception("Error during brain reconstruction: %s", e)
            notify_failure("recon")
            return

        try:
            process_lesions_for_all(folders=series_folders, freesurfer_path=fs_path, samseg_path=samseg_path)
            notify_step("lesions")
        except Exception as e:
            logger.exception("Error during lesions processing: %s", e)
            notify_failure("lesions")
            return

        try:
            segment_subregions_for_all(folders=series_folders, freesurfer_path=fs_path)
            notify_step("subs")
        except Exception as e:
            logger.exception("Error during subcortical segmentation: %s", e)
            notify_failure("subs")
            return

        try:
            segment_hypothalamus_for_all(folders=series_folders, freesurfer_path=fs_path)
            notify_step("hyp")
        except Exception as e:
            logger.exception("Error during hypothalamus segmentation: %s", e)
            notify_failure("hyp")
            return

        # JSON file generation
        try:
            generate_json_files(
                folders=series_folders,
                freesurfer_path=fs_path,
                samseg_path=samseg_path,
                json_folder=json_folder,
            )
            notify_step("json")
        except Exception as e:
            logger.exception("Error during JSON file generation: %s", e)
            notify_failure("json")
            return

        # GLTF processing
        try:
            process_viewer(folders=series_folders, freesurfer_path=fs_path, viewer_path=viewer_folder)
            notify_step("viewer")
        except Exception as e:
            logger.exception("Error during viewer processing: %s", e)
            notify_failure("viewer")
            return

    finally:
        processing_event.clear()

def read_json_file(json_path: Path) -> Dict:
    """
    Helper function to read a JSON file.
    """
    try:
        with json_path.open("r") as f:
            return json.load(f)
    except FileNotFoundError:
        logger.exception("JSON file not found: %s", json_path)
        return {}
