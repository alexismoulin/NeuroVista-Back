import os
import platform
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import logging
from nipype.interfaces.base import (
    CommandLine,
    CommandLineInputSpec,
    TraitedSpec,
    File,
    Directory,
    traits,
)
from nipype.pipeline.engine import Workflow, Node
from functools import partial
from typing import List, Any

logger = logging.getLogger(__name__)

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

def run_fastsurfer_for_series(series: str, freesurfer_path: Path, fastsurfer_path: Path, workflows_path: Path) -> None:
    """
    Run FastSurfer for a single series.
    """
    try:
        run_fastsurfer(
            fs_dir=Path.home() / "FastSurfer",
            t1=freesurfer_path / series / "mri" / "T1.mgz",
            sid=series,
            sd=fastsurfer_path,
            wf_dir=workflows_path,
            threads=max(1, os.cpu_count()),
        )
    except Exception as e:
        logger.exception("Error in FastSurfer processing for series %s: %s", series, e)
        raise

def run_fastsurfer_for_all(folders: List[str],
                           freesurfer_path: Path,
                           fastsurfer_path: Path,
                           workflows_path: Path) -> None:
    """
    Run FastSurfer segmentation in parallel for all series.
    """
    if platform == "darwin":
        os.environ["PYTORCH_ENABLE_MPS_FALLBACK"] = "1"

    try:
        with ThreadPoolExecutor(max_workers=max(1, os.cpu_count())) as executor:
            list(executor.map(
                partial(run_fastsurfer_for_series, freesurfer_path=freesurfer_path, fastsurfer_path=fastsurfer_path,
                        workflows_path=workflows_path),
                folders,
            ))
        logger.info("Extra subcortical segmentation completed")
    except Exception as e:
        logger.exception("Error with FastSurfer: %s", e)
        raise

def test_run_fastsurfer(temp_dir, mocker):
    """
    Test that run_fastsurfer sets up and calls Workflow.run.
    """
    # Patch Workflow.run.
    workflow_run_mock = mocker.patch("nipype.pipeline.engine.Workflow.run")

    # Create a dummy T1 file.
    t1_file = temp_dir / "t1.nii.gz"
    t1_file.touch()

    run_fastsurfer(
        fs_dir=temp_dir,
        t1=t1_file,
        sid="series1",
        sd=temp_dir / "FASTSURFER",
        wf_dir=temp_dir / "WORKFLOWS",
        threads=4,
    )
    workflow_run_mock.assert_called()


def test_run_fastsurfer_for_all(temp_dir, mocker):
    """
    Test that run_fastsurfer_for_all calls run_fastsurfer for each series.
    """
    # Patch the run_fastsurfer function in the app module.
    run_fastsurfer_mock = mocker.patch("app.run_fastsurfer")
    freesurfer_path = temp_dir / "FREESURFER"
    fastsurfer_path = temp_dir / "FASTSURFER"
    workflows_path = temp_dir / "WORKFLOWS"

    # Ensure directories exist.
    freesurfer_path.mkdir(parents=True, exist_ok=True)
    fastsurfer_path.mkdir(parents=True, exist_ok=True)
    workflows_path.mkdir(parents=True, exist_ok=True)

    run_fastsurfer_for_all(["series1"], freesurfer_path, fastsurfer_path, workflows_path)
    run_fastsurfer_mock.assert_called_once_with(
        fs_dir=Any(),  # We don't enforce the exact fs_dir here.
        t1=freesurfer_path / "series1" / "mri" / "T1.mgz",
        sid="series1",
        sd=fastsurfer_path,
        wf_dir=workflows_path,
        parallel=True,
        threads=Any(),  # Not enforcing exact thread count.
    )
    # Note: For flexible argument checking, one could use custom matchers (like pytest-clarity's Any) or inspect call args.


# def process_hypothalamus_v2(fastsurfer_path: Path) -> List[Dict[str, Union[str, float]]]:
#     """
#     Process FastSurfer hypothalamus MRI data from a stats file.
#     """
#
#     lines = read_volume_file_skip(fastsurfer_path, skip=55)
#     volumes = []
#     for idx, row in enumerate(lines, start=1):
#         if len(row) < 5:
#             logger.warning(f"Hypothalamus row {idx} skipped: insufficient columns.")
#             continue
#         try:
#             volume = float(row[3])
#             name = row[4]
#             if name.startswith("L-"):
#                 name = "Left" + name[2:]
#             elif name.startswith("R-"):
#                 name = "Right" + name[2:]
#             volumes.append({"Structure": name, "Volume (mm3)": round(volume, 2)})
#         except ValueError as e:
#             logger.warning(f"Hypothalamus row {idx} error with row {row}: {e}")
#     return volumes


# def process_cerebellum(file_path: pathlib.Path) -> List[Dict[str, Union[str, float]]]:
#     """
#     Process cerebellum volumes from a stats file.
#     """
#     lines = read_volume_file_skip(file_path, skip=55)
#     volumes = []
#     for idx, row in enumerate(lines, start=1):
#         if len(row) < 5:
#             logger.warning(f"Cerebellum row {idx} skipped: insufficient columns.")
#             continue
#         try:
#             volume = float(row[3])
#             name = row[4]
#             volumes.append({"Structure": name, "Volume (mm3)": round(volume, 2)})
#         except ValueError as e:
#             logger.warning(f"Cerebellum row {idx} error with row {row}: {e}")
#     return volumes
