import logging
import os
from pathlib import Path
import shutil
from typing import List, Tuple, Dict

import nibabel as nib
from nipype.interfaces.base import (
    CommandLine,
    CommandLineInputSpec,
    TraitedSpec,
    File,
    Directory,
    traits,
)
from nipype.interfaces.freesurfer import ReconAll
from nipype.pipeline.engine import Workflow, Node, MapNode

logger = logging.getLogger(__name__)
USE_FASTSURFER = False


def add_dcm_extension(filename: str) -> str:
    """
    Append '.dcm' to the filename if it doesn't already end with it.
    """
    return filename if filename.lower().endswith(".dcm") else f"{filename}.dcm"


def get_folder_names(directory: Path) -> List[str]:
    """
    Return a list of folder names within the given directory.
    """
    return [p.name for p in directory.iterdir() if p.is_dir()]


def create_folders(base_path: Path) -> Dict[str, Path]:
    """
    Create necessary folders for processing and return a dictionary mapping folder names to their paths.
    """
    folders = {
        "dicom": base_path / "DICOM",
        "nifti": base_path / "NIFTI",
        "freesurfer": base_path / "FREESURFER",
        "samseg": base_path / "SAMSEG",
        "fastsurfer": base_path / "FASTSURFER",
        "workflows": base_path / "WORKFLOWS",
        "json": base_path / "JSON",
        "corestats": base_path / "CORESTATS",
    }
    for folder in folders.values():
        folder.mkdir(parents=True, exist_ok=True)
    return folders


def get_nifti_dimensions(file_path: Path) -> Tuple[int, ...]:
    """
    Return the dimensions of a NIfTI file.
    """
    if not file_path.exists():
        raise FileNotFoundError(f"NIfTI file not found: {file_path}")
    nifti_image = nib.load(file_path)
    return nifti_image.shape


def remove_double_extension(file: Path) -> str:
    """
    Remove the double extension from a NIfTI file (e.g. '.nii.gz') and return the base name.
    """
    name = file.name
    if name.endswith(".nii.gz"):
        return name[:-7]
    return file.stem


def reconall(base_dir: Path) -> None:
    """
    Run FreeSurfer's recon-all processing on NIfTI files within the specified base directory.
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
    Process lesions using SAMSEG if the output does not already exist.
    """
    output_file = samseg_path / series / "samseg.stats"
    if output_file.is_file():
        logger.info(f"{output_file} already exists - skipping")
        return

    cmd = (
        f"--input {freesurfer_path / series / 'mri' / 'brain.mgz'} "
        f"--output {samseg_path / series} --lesion"
    )
    samseg_cmd = CommandLine(command="run_samseg", args=cmd)
    try:
        samseg_cmd.run()
        logger.info(f"Created {samseg_path / series}")
    except Exception as e:
        logger.error(f"Error running SAMSEG for series {series}: {e}")
        raise


def segment_subregions(structure: str, subject_id: str, subject_dir: Path) -> None:
    """
    Segment subregions for the specified structure if output files are missing.
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
    Run segmentation of the hypothalamus.
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


class RunFastSurferInputSpec(CommandLineInputSpec):
    t1 = File(argstr="--t1 %s", exists=True, mandatory=True, desc="Path to T1-weighted NIfTI image")
    sid = traits.Str(argstr="--sid %s", mandatory=True, desc="Subject ID")
    sd = Directory(argstr="--sd %s", mandatory=True, desc="Directory for FastSurfer output")
    no_asegdkt = traits.Bool(argstr="--no_asegdkt", usedefault=True, desc="Omit ASEG and DKT segmentations")
    parallel = traits.Bool(argstr="--parallel", usedefault=True, desc="Use parallel processing")
    threads = traits.Int(argstr="--threads %d", usedefault=True, default_value=4, desc="Number of threads")


class RunFastSurfer(CommandLine):
    _cmd = "./run_fastsurfer.sh"
    input_spec = RunFastSurferInputSpec
    output_spec = TraitedSpec


def run_fastsurfer(fs_dir: Path, t1: Path, sid: str, sd: Path, wf_dir: Path, threads: int) -> None:
    """
    Run FastSurfer segmentation workflow if the expected output files do not exist.
    """
    output_files = [
        sd / sid / "mri" / "cerebellum.CerebNet.nii.gz",
        sd / sid / "mri" / "hypothalamus.HypVINN.nii.gz",
        sd / sid / "mri" / "hypothalamus_mask.HypVINN.nii.gz",
        sd / sid / "stats" / "cerebellum.CerebNet.stats",
        sd / sid / "stats" / "hypothalamus.HypVINN.stats",
    ]
    if all(f.exists() for f in output_files):
        logger.info("Skipping Hypothalamus and Cerebellum segmentations as all output files already exist")
        return

    fastsurfer_instance = RunFastSurfer()
    fastsurfer_instance._cmd = str(fs_dir / "run_fastsurfer.sh")
    fastsurfer_node = Node(fastsurfer_instance, name="run_fastsurfer")
    fastsurfer_node.inputs.t1 = str(t1.resolve())
    fastsurfer_node.inputs.sid = sid
    fastsurfer_node.inputs.sd = str(sd.resolve())
    fastsurfer_node.inputs.threads = threads

    wf = Workflow(name="fastsurfer_workflow", base_dir=str(wf_dir))
    wf.add_nodes([fastsurfer_node])

    try:
        wf.run()
        logger.info("FastSurfer workflow completed")
    except Exception as e:
        logger.error(f"Error during FastSurfer workflow: {e}")
        raise


def process_corestats(fs_path: Path, corestats_folder: Path) -> None:
    """
    Process core statistics by copying stats files from FreeSurfer,
    then renaming them from '.stats' to '.txt'.
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
