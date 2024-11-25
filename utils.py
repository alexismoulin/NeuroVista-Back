import os
from os.path import join as opj
import pathlib

from nipype.interfaces.freesurfer import ReconAll
from nipype.pipeline.engine import Workflow, Node, MapNode
from nipype.interfaces.base import CommandLine, CommandLineInputSpec, TraitedSpec, File, Directory, traits
import logging

logging.basicConfig(level=logging.INFO)


def add_dcm_extension(filename: str) -> str:
    return filename if filename.lower().endswith('.dcm') else filename + '.dcm'


def get_folder_names(directory):
    # List all items in the directory and filter to include only directories
    folder_names = [name for name in os.listdir(directory) if os.path.isdir(os.path.join(directory, name))]
    return folder_names


def create_folders(base_path: pathlib.Path) -> tuple:
    dicom_directory = base_path / "DICOM"
    dicom_directory.mkdir(parents=True, exist_ok=True)
    nifti_directory = base_path / "NIFTI"
    nifti_directory.mkdir(parents=True, exist_ok=True)
    freesurfer_path = base_path / "FREESURFER"
    freesurfer_path.mkdir(parents=True, exist_ok=True)
    samseg_path = base_path / "SAMSEG"
    samseg_path.mkdir(parents=True, exist_ok=True)
    fastsurfer_path = base_path / "FASTSURFER"
    fastsurfer_path.mkdir(parents=True, exist_ok=True)
    workflows_path = base_path / "WORKFLOWS"
    workflows_path.mkdir(parents=True, exist_ok=True)
    json_folder = base_path / "JSON"
    json_folder.mkdir(parents=True, exist_ok=True)
    return dicom_directory, nifti_directory, freesurfer_path, samseg_path, fastsurfer_path, workflows_path, json_folder


def reconall(base_dir: str):
    data_dir = opj(base_dir, 'NIFTI')
    fs_folder = opj(base_dir, 'FREESURFER')

    nifti_files = [opj(data_dir, f) for f in os.listdir(data_dir) if f.endswith('.nii.gz')]

    if not os.path.exists(data_dir):
        logging.error(f"Data directory {data_dir} does not exist.")
        return

    if not nifti_files:
        logging.error(f"No .nii.gz files found in {data_dir}.")
        return

    subject_ids = [os.path.splitext(os.path.splitext(os.path.basename(f))[0])[0] for f in nifti_files]
    print("NIFTI: ", nifti_files)
    print("SUBJECTS: ", subject_ids)

    # Initialize lists for subjects to process
    subjects_to_process = []
    nifti_files_to_process = []

    for subj_id, nifti_file in zip(subject_ids, nifti_files):
        subj_dir = opj(fs_folder, subj_id)
        if os.path.exists(subj_dir):
            # Check for expected FreeSurfer key output files
            lh_white = opj(subj_dir, 'surf', 'lh.white')
            rh_white = opj(subj_dir, 'surf', 'rh.white')
            aparc_stats = opj(subj_dir, 'stats', 'aparc.stats')
            aparc_aseg = opj(subj_dir, "mri", "aparc + aseg.mgz")
            # Check if the key output files exist
            if all(os.path.exists(f) for f in [lh_white, rh_white, aparc_stats, aparc_aseg]):
                logging.info(f"Subject {subj_id} already processed. Skipping.")
                continue
            else:
                logging.info(f"Subject {subj_id} directory exists but processing incomplete. Re-processing.")
        else:
            logging.info(f"Subject {subj_id} has not been processed. Processing will begin.")

        # If subject directory doesn't exist or processing is incomplete, add to processing list
        subjects_to_process.append(subj_id)
        nifti_files_to_process.append(nifti_file)

    if not subjects_to_process:
        logging.info("All subjects have been processed. Nothing to do.")
        return

    reconall = MapNode(
        interface=ReconAll(),
        name='reconall',
        iterfield=['subject_id', 'T1_files']
    )

    reconall.inputs.subject_id = subjects_to_process
    reconall.inputs.directive = 'all'
    reconall.inputs.subjects_dir = fs_folder
    reconall.inputs.T1_files = nifti_files_to_process
    reconall.inputs.flags = "-qcache"

    # Create a workflow and add the ReconAll MapNode
    wf = Workflow(name='reconall_workflow')
    wf.base_dir = opj(base_dir, "WORKFLOWS", "workingdir_reconflow")
    wf.add_nodes([reconall])

    # Configure the workflow to continue even if one subject fails
    wf.config['execution'] = {'stop_on_first_crash': False}

    # Run the workflow using the MultiProc plugin to parallelize execution
    try:
        wf.run('MultiProc', plugin_args={'n_procs': os.cpu_count()})
        logging.info("Recon-all completed for all subjects.")
    except Exception as e:
        logging.error(f"Error in FreeSurfer recon-all: {e}")


def process_lesions(freesurfer_path: pathlib.Path, samseg_path: pathlib.Path, series: str):
    if (samseg_path/series/"samseg.stats").is_file():
        logging.info(msg=f"{samseg_path/series/'samseg.stats'} already exists - skipping ")
        return
    else:
        # Define the SAMSEG command
        samseg_cmd = CommandLine(
            command='run_samseg',
            args=f"--input {freesurfer_path/series}/mri/brain.mgz --output {samseg_path/series} --lesion"
        )

        # Execute the command
        samseg_cmd.run()
        logging.info(msg=f"{samseg_path / series} created ")


def segment_subregions(structure: str, subject_id: str, subject_dir: pathlib.Path):
    # Define output files based on the structure
    output_files = {
        "thalamus": [
            subject_dir / subject_id / "mri" / "ThalamicNuclei.mgz",
            subject_dir / subject_id / "mri" / "ThalamicNuclei.volumes.txt"
        ],
        "brainstem": [
            subject_dir / subject_id / "mri" / "brainstemSsLabels.mgz",
            subject_dir / subject_id / "mri" / "brainstemSsLabels.volumes.txt"
        ],
        "hippo-amygdala": [
            subject_dir / subject_id / "mri" / "rh.amygNucVolumes.txt",
            subject_dir / subject_id / "mri" / "rh.hippoSfVolumes.txt",
            subject_dir / subject_id / "mri" / "lh.amygNucVolumes.txt",
            subject_dir / subject_id / "mri" / "lh.hippoSfVolumes.txt",
            subject_dir / subject_id / "mri" / "lh.hippoAmygLabels.mgz",
            subject_dir / subject_id / "mri" / "rh.hippoAmygLabels.mgz"
        ]
    }

    # Check for missing output files
    missing_files = [file for file in output_files[structure] if not file.exists()]

    # Log and skip if all files are present
    if not missing_files:
        logging.info(f"Skipping {structure} segmentation as all output files already exist")
        return

    # Log missing files and run the segmentation command
    logging.info(f"Missing output files for {structure}: {missing_files}. Running segmentation.")
    command = CommandLine(command="segment_subregions", args=f"{structure} --cross {subject_id} --sd {subject_dir}")
    try:
        command.run()
        logging.info(f"{structure} segmentation completed")
    except Exception as e:
        logging.error(f"Error during {structure} segmentation: {e}")


def segment_hypothalamus(subject_id: str, subject_dir: str):
    command = CommandLine(
        command="mri_segment_hypothalamic_subunits", 
        args=f"--s {subject_id} --sd {subject_dir} --threads {os.cpu_count()}"
    )
    logging.info(command.cmdline)
    try:
        command.run()
        logging.info("Hypothalamus segmentation completed")
    except Exception as e:
        logging.error(f"Error during hypothalamus segmentation: {e}")

# Define a custom interface for run_fastsurfer.sh
class RunFastSurfer(CommandLine):
    _cmd = './run_fastsurfer.sh'  # Command to run FastSurfer
    input_spec = CommandLineInputSpec
    output_spec = TraitedSpec

    # Define the input fields for the command
    class input_spec(CommandLineInputSpec):
        t1 = File(argstr="--t1 %s", exists=True, mandatory=True, desc="Path to T1-weighted NIfTI image")
        sid = traits.Str(argstr="--sid %s", mandatory=True, desc="Subject ID")
        sd = Directory(argstr="--sd %s", mandatory=True, desc="Directory for FastSurfer output")
        no_asegdkt = traits.Bool(argstr="--no_asegdkt", usedefault=True, desc="Omit ASEG and DKT segmentations")
        parallel = traits.Bool(argstr="--parallel", usedefault=True, desc="Use parallel processing")
        threads = traits.Int(argstr="--threads %d", usedefault=True, default_value=4, desc="Number of threads")


def run_fastsurfer(fs_dir: pathlib.Path,
                   t1: pathlib.Path,
                   sid: str,
                   sd: pathlib.Path,
                   wf_dir: pathlib.Path,
                   parallel: bool,
                   threads: int):

    # Check if files already exist
    output_files = [
        sd / sid / "mri" / "cerebellum.CerebNet.nii.gz",
        sd / sid / "mri" / "hypothalamus.HypVINN.nii.gz",
        sd / sid / "mri" / "hypothalamus_mask.HypVINN.nii.gz",
        sd / sid / "stats" / "cerebellum.CerebNet.stats",
        sd / sid / "stats" / "hypothalamus.HypVINN.stats"
    ]

    missing_files = [file for file in output_files if not file.exists()]

    # Log and skip if all files are present
    if not missing_files:
        logging.info(f"Skipping Hypothalamus and Cerebellum segmentations as all output files already exist")
        return

    # Set up the FastSurfer node with inputs
    fastsurfer_instance = RunFastSurfer()
    fastsurfer_instance._cmd = f"{fs_dir}/run_fastsurfer.sh"
    fastsurfer_node = Node(fastsurfer_instance, name="run_fastsurfer")

    # Specify the inputs
    fastsurfer_node.inputs.t1 = t1
    fastsurfer_node.inputs.sid = sid
    fastsurfer_node.inputs.sd = sd
    fastsurfer_node.inputs.parallel = parallel
    fastsurfer_node.inputs.threads = threads

    # Create a workflow
    wf = Workflow(name="fastsurfer_workflow", base_dir=wf_dir)

    # Add the node to the workflow
    wf.add_nodes([fastsurfer_node])

    # Run the workflow
    try:
        wf.run()
        logging.info("FastSurfer workflow completed")
    except Exception as e:
        logging.error(f"Error during FastSurfer: {e}")

