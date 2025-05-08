import logging
import os
import re
from pathlib import Path
import shutil
from typing import List, Tuple, Dict

import nibabel as nib
from nipype.interfaces.base import CommandLine
from nipype.interfaces.freesurfer import ReconAll
from nipype.pipeline.engine import Workflow, MapNode

logger = logging.getLogger(__name__)


def add_dcm_extension(filename: str) -> str:
    """
    Append '.dcm' to the filename if it does not already end with it.

    This function ensures that the provided filename has the proper DICOM extension,
    checking the extension in a case-insensitive manner.

    Args:
        filename (str): The original filename.

    Returns:
        str: The filename ending with '.dcm'.
    """
    return filename if filename.lower().endswith(".dcm") else f"{filename}.dcm"


def get_folder_names(directory: Path) -> List[str]:
    """
    Retrieve the names of all subdirectories within a given directory.

    Iterates over the contents of the directory and returns a list containing the names
    of items that are directories.

    Args:
        directory (Path): The directory to search within.

    Returns:
        List[str]: A list of folder names.
    """
    return [p.name for p in directory.iterdir() if p.is_dir()]


def sanitize_name(name: str) -> str:
    """
    Sanitize an input string to prevent path traversal and remove unsafe characters.

    The function allows only alphanumeric characters, underscores, and dashes.

    Args:
        name (str): The original name string.

    Returns:
        str: The sanitized name.
    """
    return re.sub(r'[^A-Za-z0-9_-]', '', name)


def create_folders(base_path: Path) -> Dict[str, Path]:
    """
    Create necessary processing folders and return a mapping of folder names to their paths.

    This function creates a set of predefined folders (e.g., DICOM, NIFTI, FREESURFER, etc.)
    under the given base directory. If a folder already exists, it is left intact.

    Args:
        base_path (Path): The root directory where folders will be created.

    Returns:
        Dict[str, Path]: A dictionary mapping folder names to their corresponding Path objects.
    """
    folders = {
        "dicom": base_path / "DICOM",
        "nifti": base_path / "NIFTI",
        "freesurfer": base_path / "FREESURFER",
        "samseg": base_path / "SAMSEG",
        "workflows": base_path / "WORKFLOWS",
        "json": base_path / "JSON",
        "corestats": base_path / "CORESTATS",
    }
    for folder in folders.values():
        folder.mkdir(parents=True, exist_ok=True)
    return folders


def get_nifti_dimensions(file_path: Path) -> Tuple[int, ...]:
    """
    Return the dimensions (shape) of a NIfTI file.

    Uses nibabel to load the file and extract its shape. Raises a FileNotFoundError if the file
    does not exist.

    Args:
        file_path (Path): The path to the NIfTI file.

    Returns:
        Tuple[int, ...]: The dimensions of the NIfTI image.

    Raises:
        FileNotFoundError: If the specified file does not exist.
    """
    if not file_path.exists():
        raise FileNotFoundError(f"NIfTI file not found: {file_path}")
    nifti_image = nib.load(file_path)
    return nifti_image.shape


def remove_double_extension(file: Path) -> str:
    """
    Remove the double extension from a NIfTI file (e.g. '.nii.gz') and return its base name.

    If the filename ends with '.nii.gz', the function strips this extension.
    Otherwise, it returns the stem of the file.

    Args:
        file (Path): The path to the NIfTI file.

    Returns:
        str: The base name of the file without the double extension.
    """
    name = file.name
    if name.endswith(".nii.gz"):
        return name[:-7]
    return file.stem


def list_folder_subfolders(directory_path: Path) -> List[Tuple]:
    """
    List each folder within the directory along with its immediate subfolders.

    For every folder found in the provided directory, the function returns tuples where the first
    element is the folder name and the second element is the name of one of its subfolders.

    Args:
        directory_path (Path): The directory to search within.

    Returns:
        List[Tuple[str, str]]: A list of tuples in the format (folder_name, subfolder_name).
    """
    folder_subfolder_pairs = []

    for folder in sorted(directory_path.iterdir()):
        if folder.is_dir():
            subfolders = [subfolder.name for subfolder in sorted(folder.iterdir()) if subfolder.is_dir()]
            for subfolder in subfolders:
                folder_subfolder_pairs.append((folder.name, subfolder))

    return folder_subfolder_pairs


def reconall(base_dir: Path) -> None:
    """
    Run FreeSurfer's recon-all processing on NIfTI files within the base directory.

    This function looks for NIfTI files in the 'NIFTI' subfolder of the base directory and
    prepares subject IDs by removing double extensions. It checks whether each subject has been
    processed based on the presence of key output files. Subjects needing processing are then
    submitted to a MapNode running the ReconAll interface in a workflow.

    Args:
        base_dir (Path): The root directory containing the NIFTI and FREESURFER folders.

    Returns:
        None

    Raises:
        Exception: Propagates any exceptions raised during the workflow execution.
    """
    data_dir = base_dir / "NIFTI"
    fs_folder = base_dir / "FREESURFER"

    if not data_dir.exists():
        logger.error(f"Data directory {data_dir} does not exist.")
        return

    nifti_files = sorted(data_dir.glob("*.nii.gz"))
    if not nifti_files:
        logger.error(f"No .nii.gz files found in {data_dir}.")
        return

    subject_ids = [remove_double_extension(f) for f in nifti_files]
    logger.info(f"Found NIFTI files: {nifti_files}")
    logger.info(f"Subject IDs: {subject_ids}")

    subjects_to_process: List[str] = []
    nifti_files_to_process: List[str] = []

    for subj_id, nifti_file in zip(subject_ids, nifti_files):
        subj_dir = fs_folder / subj_id
        if subj_dir.exists():
            key_files = [
                subj_dir / "surf" / "lh.white",
                subj_dir / "surf" / "rh.white",
                subj_dir / "stats" / "lh.aparc.stats",
                subj_dir / "stats" / "rh.aparc.stats",
                subj_dir / "mri" / "aparc+aseg.mgz"
            ]
            if all(f.exists() for f in key_files):
                logger.info(f"Subject {subj_id} already processed. Skipping.")
                continue
            else:
                logger.info(f"Subject {subj_id} directory exists but processing incomplete. Re-processing.")
        else:
            logger.info(f"Subject {subj_id} has not been processed. Processing will begin.")

        subjects_to_process.append(subj_id)
        nifti_files_to_process.append(str(nifti_file))

    if not subjects_to_process:
        logger.info("All subjects have been processed. Nothing to do.")
        return

    reconall_node = MapNode(
        interface=ReconAll(),
        name='reconall',
        iterfield=['subject_id', 'T1_files']
    )
    reconall_node.inputs.subject_id = subjects_to_process
    reconall_node.inputs.directive = 'all'
    reconall_node.inputs.subjects_dir = str(fs_folder)
    reconall_node.inputs.T1_files = nifti_files_to_process
    reconall_node.inputs.flags = "-qcache"

    wf = Workflow(
        name='reconall_workflow',
        base_dir=str(base_dir / "WORKFLOWS" / "workingdir_reconflow")
    )
    wf.add_nodes([reconall_node])
    wf.config['execution'] = {'stop_on_first_crash': False}

    try:
        wf.run('MultiProc', plugin_args={'n_procs': os.cpu_count()})
        logger.info("Recon-all completed for all subjects.")
    except Exception as e:
        logger.error(f"Error in FreeSurfer recon-all: {e}")
        raise

    logger.info(f"Subjects processed: {subjects_to_process}")


def process_lesions(freesurfer_path: Path, samseg_path: Path, series: str) -> None:
    """
    Process lesion data using SAMSEG if the output does not already exist.

    Checks if the expected SAMSEG output files exist for the given series. If not, it constructs a
    command line call to run SAMSEG with lesion processing enabled.

    Args:
        freesurfer_path (Path): The path to the FreeSurfer processed data.
        samseg_path (Path): The path where SAMSEG outputs should be stored.
        series (str): The series identifier for which lesions should be processed.

    Returns:
        None

    Raises:
        Exception: Propagates any exceptions encountered when running the SAMSEG command.

    """
    output_file = samseg_path / series / "samseg.stats"
    # output_file2 = samseg_path / series / "samseg.fs.stats"
    if output_file.is_file():
        logger.info("samseg.stats file already exists - skipping")
        return

    cmd_args = f"--input {freesurfer_path / series / 'mri' / 'brain.mgz'} --output {samseg_path / series} --lesion"
    samseg_cmd = CommandLine(command="run_samseg", args=cmd_args)
    try:
        samseg_cmd.run()
        logger.info(f"Created {samseg_path / series}")
    except Exception as e:
        logger.error(f"Error running SAMSEG for series {series}: {e}")
        raise


def segment_subregions(structure: str, subject_id: str, subject_dir: Path) -> None:
    """
    Segment subregions for a given structure if the required output files are missing.

    Based on the specified structure, the function checks for the presence of expected output files.
    If any are missing, it runs the segmentation command via nipype's CommandLine interface.

    Args:
        structure (str): The brain structure to segment (e.g., "thalamus", "brainstem", "hippo-amygdala").
        subject_id (str): The identifier for the subject.
        subject_dir (Path): The directory containing subject data.

    Returns:
        None

    Raises:
        Exception: Propagates any exceptions raised during the segmentation process.
    """
    subject_path = subject_dir / subject_id
    output_files = {
        "thalamus": [
            subject_path / "mri" / "ThalamicNuclei.mgz",
            subject_path / "mri" / "ThalamicNuclei.volumes.txt",
        ],
        "brainstem": [
            subject_path / "mri" / "brainstemSsLabels.mgz",
            subject_path / "mri" / "brainstemSsLabels.volumes.txt",
        ],
        "hippo-amygdala": [
            subject_path / "mri" / "rh.amygNucVolumes.txt",
            subject_path / "mri" / "rh.hippoSfVolumes.txt",
            subject_path / "mri" / "lh.amygNucVolumes.txt",
            subject_path / "mri" / "lh.hippoSfVolumes.txt",
            subject_path / "mri" / "lh.hippoAmygLabels.mgz",
            subject_path / "mri" / "rh.hippoAmygLabels.mgz",
        ],
    }
    missing_files = [f for f in output_files.get(structure, []) if not f.exists()]
    if not missing_files:
        logger.info(f"Skipping {structure} segmentation as all output files already exist")
        return

    logger.info(f"Missing output files for {structure}: {missing_files}. Running segmentation.")
    cmd = f"{structure} --cross {subject_id} --sd {subject_dir}"
    command = CommandLine(command="segment_subregions", args=cmd)
    try:
        command.run()
        logger.info(f"{structure} segmentation completed")
    except Exception as e:
        logger.error(f"Error during {structure} segmentation: {e}")
        raise


def segment_hypothalamus(subject_id: str, subject_dir: Path) -> None:
    """
    Run segmentation of the hypothalamus for a given subject.

    Checks if the hypothalamus segmentation output file exists; if not, it executes the segmentation
    command using the nipype CommandLine interface.

    Args:
        subject_id (str): The subject identifier.
        subject_dir (Path): The directory containing subject data.

    Returns:
        None

    Raises:
        Exception: Propagates any exceptions raised during the segmentation process.
    """
    output_file = subject_dir / subject_id / "mri" / "hypothalamic_subunits_volumes.v1.csv"
    if output_file.is_file():
        logger.info(f"{output_file} already exists - skipping")
        return

    cmd = f"--s {subject_id} --sd {subject_dir} --threads {os.cpu_count()}"
    command = CommandLine(command="mri_segment_hypothalamic_subunits", args=cmd)
    logger.info(f"Executing command: {command.cmdline}")
    try:
        command.run()
        logger.info("Hypothalamus segmentation completed")
    except Exception as e:
        logger.error(f"Error during hypothalamus segmentation: {e}")
        raise


def process_corestats(fs_path: Path, corestats_folder: Path) -> None:
    """
    Process core statistics by copying and renaming stats files from FreeSurfer.

    The function copies all '.stats' files from the FreeSurfer 'stats' subfolder to the provided
    corestats folder. It then renames these files to have a '.txt' extension. Additionally, it copies
    any '.txt' files found in the 'mri' subfolder.

    Args:
        fs_path (Path): The FreeSurfer directory containing the 'stats' and 'mri' subfolders.
        corestats_folder (Path): The destination folder for the core statistics files.

    Returns:
        None

    Raises:
        FileNotFoundError: If the FreeSurfer directory does not exist.
        Exception: Propagates any exceptions raised during file renaming or copying.
    """
    if not fs_path.exists():
        raise FileNotFoundError(f"FreeSurfer directory not found: {fs_path}")

    stats_dir = fs_path / "stats"
    mri_dir = fs_path / "mri"
    corestats_folder.mkdir(parents=True, exist_ok=True)

    # Copy .stats files from the stats subfolder
    if stats_dir.exists():
        for stats_file in stats_dir.glob("*.stats"):
            shutil.copy2(stats_file, corestats_folder)
    else:
        logger.warning(f"No stats directory found in FreeSurfer path: {fs_path}")

    # Rename stats files to txt files
    for stats_file in corestats_folder.glob("*.stats"):
        txt_file = stats_file.with_suffix(".txt")
        try:
            stats_file.rename(txt_file)
            logger.info(f"Renamed {stats_file} to {txt_file}")
        except Exception as e:
            logger.error(f"Error renaming file {stats_file}: {e}")
            raise

    # Copy .txt files from the mri subfolder
    if mri_dir.exists():
        for mri_file in mri_dir.glob("*.txt"):
            shutil.copy2(mri_file, corestats_folder)

    logger.info(f"Core statistics processed and saved to {corestats_folder}")
