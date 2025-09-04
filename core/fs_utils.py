import os
from pathlib import Path
from nipype.interfaces.base import CommandLine
from nipype.interfaces.freesurfer import ReconAll
from nipype.pipeline.engine import Workflow, MapNode
from nipype.interfaces.base.support import InterfaceResult
from core.utils import remove_double_extension, logger
from typing import List, Dict


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


def create_env_fullspeed(subject_dir: Path) -> Dict[str, str]:
    """
    Environment for the first (full-speed) attempt.

    Sets SUBJECTS_DIR but does NOT cap thread pools, allowing libraries (OpenMP,
    MKL/OpenBLAS, etc.) to use as many threads as they want.
    """
    env = os.environ.copy()
    env["SUBJECTS_DIR"] = str(subject_dir)
    return env


def create_env_safemode(subject_dir: Path) -> Dict[str, str]:
    """
    Environment for the fallback (low-memory) attempt.

    Caps all common thread pools to 1 to minimize memory usage.
    """
    env = os.environ.copy()
    env["SUBJECTS_DIR"] = str(subject_dir)
    env["OMP_NUM_THREADS"] = "1"
    env["ITK_GLOBAL_DEFAULT_NUMBER_OF_THREADS"] = "1"
    env["FREESURFER_NUM_THREADS"] = "1"
    env["OPENBLAS_NUM_THREADS"] = "1"
    env["MKL_NUM_THREADS"] = "1"
    env["VECLIB_MAXIMUM_THREADS"] = "1"
    env["NUMEXPR_NUM_THREADS"] = "1"
    return env


def run_hypo(subject_id: str, subject_dir: Path, threads: int, env: Dict[str, str]) -> InterfaceResult:
    """
    Run the `mri_segment_hypothalamic_subunits` command via Nipype.

    Args:
        subject_id (str): FreeSurfer subject ID.
        subject_dir (Path): Path to SUBJECTS_DIR.
        threads (int): Number of threads to pass via `--threads`.
        env (dict): Environment variables to use for the subprocess.

    Returns:
        nipype.interfaces.base.support.InterfaceResult: Execution result object
        with runtime info (stdout, stderr, return code, etc.).
    """
    args = f"--s {subject_id} --sd {subject_dir} --threads {threads}"
    cli = CommandLine(
        command="mri_segment_hypothalamic_subunits",
        args=args,
        terminal_output="allatonce",  # capture stdout+stderr
        environ=env,
    )
    logger.info("Executing command: %s", cli.cmdline)
    return cli.run()


def segment_hypothalamus(subject_id: str, subject_dir: Path) -> None:
    """
    Run FreeSurfer hypothalamic subunit segmentation with OOM-aware retry.

    Strategy:
        1) First run with all available CPUs (os.cpu_count()) and no env thread caps.
        2) If OOM symptoms are detected (exit code 137 or 'Killed' in output),
           retry once with threads=1 and strict env caps.
        3) For other non-zero exit codes, fail immediately (no retry).
        4) Always verify the expected CSV output exists before reporting success.

    Args:
        subject_id (str): FreeSurfer subject ID.
        subject_dir (Path): Path to FreeSurfer SUBJECTS_DIR.

    Raises:
        RuntimeError: If the command fails for non-OOM reasons, is OOM-killed
            even on 1 thread, or does not produce the expected output CSV.
    """
    output_file = subject_dir / subject_id / "mri" / "hypothalamic_subunits_volumes.v1.csv"
    if output_file.is_file():
        logger.info("%s already exists - skipping", output_file)
        return

    # ---- First attempt: full speed (max threads), no caps ----
    max_threads = os.cpu_count() or 1
    env_full = create_env_fullspeed(subject_dir)

    result = run_hypo(subject_id, subject_dir, threads=max_threads, env=env_full)
    rc = getattr(result.runtime, "returncode", None)
    stdout = (getattr(result.runtime, "stdout", "") or "").strip()
    stderr = (getattr(result.runtime, "stderr", "") or "").strip()

    oomish = (rc == 137) or ("Killed" in stderr) or ("Killed" in stdout)

    if not oomish:
        if rc not in (0, None):
            # Non-OOM failure → fail fast (don’t mask real errors)
            raise RuntimeError(
                f"mri_segment_hypothalamic_subunits failed with return code {rc}\n"
                f"--- stderr (tail) ---\n{stderr[-60000:]}"
            )
        # Exit code ok; verify output exists
        if output_file.is_file():
            logger.info("Hypothalamus segmentation completed: %s", output_file)
            return
        raise RuntimeError(
            "Segmentation exited cleanly but output file is missing:\n"
            f"  {output_file}\n--- stderr (tail) ---\n{stderr[-60000:]}\n"
            "--- stdout (tail) ---\n" + stdout[-60000:]
        )

    logger.warning(
        "Likely OOM at %d threads (rc=%s). Retrying with 1 thread and capped env.",
        max_threads, rc
    )

    # ---- Fallback: 1 thread, strict env caps ----
    env_safe = create_env_safemode(subject_dir)
    result = run_hypo(subject_id, subject_dir, threads=1, env=env_safe)
    rc = getattr(result.runtime, "returncode", None)
    stdout = (getattr(result.runtime, "stdout", "") or "").strip()
    stderr = (getattr(result.runtime, "stderr", "") or "").strip()

    if rc not in (0, None):
        raise RuntimeError(
            f"mri_segment_hypothalamic_subunits failed with return code {rc} (1 thread)\n"
            f"--- stderr (tail) ---\n{stderr[-60000:]}"
        )
    if not output_file.is_file():
        raise RuntimeError(
            "Segmentation with 1 thread did not produce expected output:\n"
            f"  {output_file}\n--- stderr (tail) ---\n{stderr[-60000:]}\n"
            "--- stdout (tail) ---\n" + stdout[-60000:]
        )

    logger.info("Hypothalamus segmentation completed with 1 thread: %s", output_file)
