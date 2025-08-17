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
    Notify listeners that a processing step has completed.

    The step name is pushed into the global ``STEP_COMPLETION_QUEUE`` so
    external consumers (e.g., a UI thread or websocket) can consume progress
    updates.

    Parameters
    ----------
    step : str
        Logical step identifier (e.g., ``"dicom"``, ``"nifti"``, ``"recon"``).

    Returns
    -------
    None
    """
    STEP_COMPLETION_QUEUE.put(step)

def notify_failure(step: str) -> None:
    """
    Notify listeners that a processing step failed.

    Internally prefixes the provided step key with ``"failed_"`` and forwards
    it to :func:`notify_step`.

    Parameters
    ----------
    step : str
        Logical step identifier that failed.

    Returns
    -------
    None
    """
    notify_step(f"failed_{step}")

def save_dicoms(request_files: ImmutableMultiDict[str, FileStorage], dicom_directory: Path) -> None:
    """
        Save uploaded DICOMs to disk, grouped by series.

        For each uploaded file under the ``"dicoms"`` form key, this function:
        1) Skips DICOMDIR files (by filename or SOPClassUID).
        2) Reads the file's ``SeriesDescription`` (fallback ``"UNKNOWN"``),
           replacing spaces with underscores.
        3) Creates ``<dicom_directory>/<SeriesDescription>/`` if needed.
        4) Saves the file with a ``.dcm`` extension (added if missing).

        Parameters
        ----------
        request_files : ImmutableMultiDict[str, FileStorage]
            Incoming files from a Werkzeug/Flask request.
        dicom_directory : Path
            Root directory where per-series folders will be created.

        Returns
        -------
        None

        Notes
        -----
        Errors while handling individual files are logged and skipped; the
        function continues with remaining files.
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
            dest_file = series_dir / add_dcm_extension(filename=os.path.basename(dicom_file.filename))
            dicom_file.save(dst=str(dest_file))
        except Exception as e:
            logger.exception("Skipping file %s due to error: %s", dicom_file.filename, e)
    logger.info("DICOM files saved successfully")

def convert_to_nifti(dicom_directory: Path, nifti_directory: Path) -> None:
    """
      Convert each DICOM series to a single NIfTI file.

      For every direct subfolder in ``dicom_directory`` (assumed to represent a
      DICOM series), writes ``<folder>.nii.gz`` into ``nifti_directory`` using
      :mod:`dicom2nifti`.

      Parameters
      ----------
      dicom_directory : Path
          Directory containing one subfolder per DICOM series.
      nifti_directory : Path
          Destination directory for the generated ``.nii.gz`` files.

      Returns
      -------
      None

      Notes
      -----
      Exceptions per series are logged and skipped; the conversion continues for
      remaining series.
      """
    for folder in get_folder_names(directory=dicom_directory):
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
    Run the FreeSurfer ``recon-all`` pipeline for a subject.

    Parameters
    ----------
    base_dir : Path
        Base directory containing the subject tree expected by FreeSurfer.

    Returns
    -------
    None

    Raises
    ------
    Exception
        Propagates any error raised by the underlying :func:`reconall` helper.

    Notes
    -----
    Logs the elapsed wall-clock time for the reconstruction.
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
        Run lesion processing for a single series.

        Parameters
        ----------
        series : str
            Series identifier (folder name).
        freesurfer_path : Path
            Root path containing FreeSurfer outputs organized by series.
        samseg_path : Path
            Root path for SAMSEG-related inputs/outputs for the series.

        Returns
        -------
        None

        Raises
        ------
        Exception
            Re-raises any error from :func:`process_lesions` after logging.
        """
    try:
        process_lesions(freesurfer_path, samseg_path, series)
    except Exception as e:
        logger.exception("Error processing lesions for series %s: %s", series, e)
        raise

def process_lesions_for_all(folders: List[str], freesurfer_path: Path, samseg_path: Path) -> None:
    """
    Process lesions for all series in parallel.

    Parameters
    ----------
    folders : list of str
        Series identifiers to process.
    freesurfer_path : Path
        Root FreeSurfer directory.
    samseg_path : Path
        Root SAMSEG directory.

    Returns
    -------
    None

    Notes
    -----
    Uses a :class:`concurrent.futures.ThreadPoolExecutor` with
    ``max_workers = max(1, os.cpu_count())`` to parallelize series-level work.
    """
    with ThreadPoolExecutor(max_workers=max(1, os.cpu_count())) as executor:
        executor.map(partial(process_lesions_for_series, freesurfer_path=freesurfer_path, samseg_path=samseg_path), folders)
    logger.info("SAMSEG processing completed")

def segment_subregions_for_all(folders: List[str], freesurfer_path: Path) -> None:
    """
    Run subcortical subregion segmentations for each series.

    Executes segmentation for:
    ``thalamus``, ``brainstem``, and ``hippo-amygdala``.

    Parameters
    ----------
    folders : list of str
        Series identifiers to process.
    freesurfer_path : Path
        Root FreeSurfer directory.

    Returns
    -------
    None

    Notes
    -----
    Errors are logged per series/structure and do not stop iteration of the
    remaining tasks.
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

    Parameters
    ----------
    folders : list of str
        Series identifiers to process.
    freesurfer_path : Path
        Root FreeSurfer directory.

    Returns
    -------
    None

    Notes
    -----
    Errors are logged per series and do not stop the loop.
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
    Generate per-series JSON outputs and aggregated summaries.

    For each series, runs :func:`run_jsonifier` into ``<json_folder>/<series>/``.
    Then computes cohort-level averages for several categories and a global
    summary file.

    Parameters
    ----------
    folders : list of str
        Series identifiers to process.
    freesurfer_path : Path
        Root FreeSurfer directory.
    samseg_path : Path
        Root SAMSEG directory.
    json_folder : Path
        Root directory where JSON outputs will be written.

    Returns
    -------
    None

    Raises
    ------
    Exception
        Propagates failures from per-series JSON generation or aggregation.

    Side Effects
    ------------
    Creates (at minimum) files like:
    ``cortical.json``, ``subcortical.json``, ``general.json`` per series, and
    aggregated outputs in ``<json_folder>/AVERAGES/`` plus a global JSON.
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


def process_viewer(folders: List[str], viewer_path: Path, freesurfer_path: Path) -> None:
    """
    Produce GLTF/GLB models for each series for interactive viewing.

    Uses :func:`core.viewer.create_gltf_models` to export standard sets of
    meshes derived from FreeSurfer outputs into ``<viewer_path>/<series>/``.

    Parameters
    ----------
    folders : list of str
        Series identifiers to process.
    viewer_path : Path
        Destination root for viewer assets.
    freesurfer_path : Path
        Root FreeSurfer directory.

    Returns
    -------
    None
    """
    for folder in folders:
        output_dir = viewer_path / folder
        output_dir.mkdir(parents=True, exist_ok=True)
        create_gltf_models(freesurfer_path=freesurfer_path, viewer_path=viewer_path, folder=folder)



def prepare_processing(base_path: Path, request_files: ImmutableMultiDict[str, FileStorage]) -> Optional[Dict[str, Path]]:
    """
    Prepare on-disk folders and persist uploaded DICOMs.

    This function:
    1) Creates the expected directory hierarchy under ``base_path`` (e.g.,
       ``dicom/``, ``nifti/``, ``freesurfer/``, ``samseg/``, ``json/``, ``viewer/``).
    2) Saves uploaded DICOMs into ``<dicom>/ <SeriesDescription> / *``.
    3) Emits a progress notification for the ``"dicom"`` step upon success.

    Parameters
    ----------
    base_path : Path
        Root path for the processing workspace.
    request_files : ImmutableMultiDict[str, FileStorage]
        Uploaded files from a request object (expects key ``"dicoms"``).

    Returns
    -------
    dict[str, Path] or None
        Mapping of logical folder names to paths if successful; ``None`` on failure.

    Notes
    -----
    On failure, a ``"failed_dicom"`` notification is emitted and the error is
    logged. Attention with Multithreading (e.g. gunicorn, it is not working properly)
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
        Execute the full imaging pipeline end-to-end.

        The pipeline steps are, in order:
        1) DICOM→NIfTI conversion (``"nifti"``)
        2) FreeSurfer ``recon-all`` (``"recon"``)
        3) SAMSEG lesions (``"lesions"``)
        4) Subcortical subregions (``"subs"``)
        5) Hypothalamus segmentation (``"hyp"``)
        6) JSON generation and aggregation (``"json"``)
        7) Viewer GLTF/GLB export (``"viewer"``)

        After each successful step a progress notification is emitted; on failure,
        a ``"failed_<step>"`` notification is emitted and processing stops.

        Parameters
        ----------
        base_path : Path
            Root workspace directory (passed to :func:`run_reconall`).
        folders_dict : dict[str, Path]
            Folder mapping as returned by :func:`prepare_processing`.

        Returns
        -------
        None

        Notes
        -----
        Clears the global :data:`processing_event` in a ``finally`` block.
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
    Read a JSON file from disk.

    Parameters
    ----------
    json_path : Path
        Path to a ``.json`` file.

    Returns
    -------
    dict
        Parsed JSON object on success. Returns an empty dict if the file
        does not exist or cannot be read.

    Notes
    -----
    Missing files are logged at exception level for visibility, but the
    function handles the error by returning ``{}``.
    """
    try:
        with json_path.open("r") as f:
            return json.load(f)
    except FileNotFoundError:
        logger.exception("JSON file not found: %s", json_path)
        return {}
