import os
from pathlib import Path
from nipype.interfaces.base import CommandLine
from nipype.interfaces.freesurfer import ReconAll
from nipype.pipeline.engine import Workflow, MapNode
from core.utils import remove_double_extension, logger
from typing import List


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
        logger.exception(f"Error in FreeSurfer recon-all: {e}")
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
        logger.exception(f"Error running SAMSEG for series {series}: {e}")
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
        logger.exception(f"Error during {structure} segmentation: {e}")
        raise


def segment_hypothalamus(subject_id: str, subject_dir: Path) -> None:
    """
    Run FreeSurfer hypothalamic subunit segmentation for a subject using Nipype's CommandLine,
    with strong error detection and helpful logging.

    Args:
        subject_id: FreeSurfer subject ID.
        subject_dir: Path to SUBJECTS_DIR (directory containing the subject folder).

    Raises:
        RuntimeError: if the command fails (non-zero return code) or the expected output is missing.
    """
    output_file = subject_dir / subject_id / "mri" / "hypothalamic_subunits_volumes.v1.csv"
    if output_file.is_file():
        logger.info("%s already exists - skipping", output_file)
        return

    # Build the CLI
    threads = os.cpu_count() or 1
    args = f"--s {subject_id} --sd {subject_dir} --threads {threads}"

    # Capture all output so we can surface errors even if the tool returns 0.
    cli = CommandLine(
        command="mri_segment_hypothalamic_subunits",
        args=args,
        terminal_output="allatonce"  # capture stdout/stderr in result.runtime
    )

    logger.info("Executing command: %s", cli.cmdline)

    try:
        result = cli.run()  # does not always raise even if the tool failed internally
    except Exception as e:
        logger.exception("Nipype raised while running hypothalamus segmentation")
        raise

    # Pull captured output
    rc = getattr(result.runtime, "returncode", None)
    stdout = getattr(result.runtime, "stdout", "") or ""
    stderr = getattr(result.runtime, "stderr", "") or ""

    # Log a brief tail to keep logs readable; write full text at debug level
    def _tail(txt: str, n: int = 60_000) -> str:  # ~60k chars tail for context
        return txt[-n:] if len(txt) > n else txt

    logger.info("===== mri_segment_hypothalamic_subunits STDOUT =====\n%s", stdout)
    logger.info("===== mri_segment_hypothalamic_subunits STDERR =====\n%s", stderr)

    # Treat non-zero return codes as failure
    if rc not in (0, None):  # Some Nipype versions may not set rc; we handle that below with file existence.
        snippet = (_tail(stderr) or _tail(stdout) or "").strip()
        raise RuntimeError(
            f"mri_segment_hypothalamic_subunits exited with return code {rc}.\n"
            f"--- Tool output (tail) ---\n{snippet}"
        )

    # Even with rc==0, assert the expected artifact exists
    if not output_file.is_file():
        snippet = (_tail(stderr) or _tail(stdout) or "").strip()
        raise RuntimeError(
            "Hypothalamus segmentation did not produce the expected output file:\n"
            f"  {output_file}\n"
            "The command reported success but likely failed internally.\n"
            "Troubleshooting hints:\n" + "\n" +
            "\n--- Tool output (tail) ---\n" + snippet
        )

    logger.info("Hypothalamus segmentation completed and output verified: %s", output_file)
