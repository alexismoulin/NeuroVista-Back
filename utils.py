import os
from os.path import join as opj
import pathlib
from nipype.interfaces.freesurfer import ReconAll
from nipype.interfaces.utility import IdentityInterface
from nipype.pipeline.engine import Workflow, Node
from nipype.interfaces.base import CommandLine

def add_dcm_extension(filename):
    if not filename.lower().endswith('.dcm'):
        filename += '.dcm'
    return filename

def freesurfer(experiment_dir: str, series: str):
    # Specify important variables
    experiment_dir = experiment_dir # location of experiment folder
    data_dir = opj(experiment_dir, 'NIFTI')  # location of data folder
    fs_folder = opj(experiment_dir, 'FREESURFER')  # location of freesurfer folder
    subject_list = [series]  # subject identifier
    t1_identifier = 'struct.nii.gz'  # Name of T1-weighted image

    # Create the output folder - FreeSurfer can only run if this folder exists
    pathlib.Path(fs_folder).mkdir(parents=True, exist_ok=True)

    # Create the pipeline that runs the recon-all command
    reconflow = Workflow(name="reconflow")
    reconflow.base_dir = opj(experiment_dir, 'workingdir_reconflow')

    # Some magical stuff happens here (not important for now)
    infosource = Node(IdentityInterface(fields=['subject_id']), name="infosource")
    infosource.iterables = ('subject_id', subject_list)

    # This node represents the actual recon-all command
    reconall = Node(ReconAll(directive='all', subjects_dir=fs_folder), name="reconall")

    # This function returns for each subject the path to struct.nii.gz
    def pathfinder(subject, foldername, filename):
        from os.path import join as opj
        struct_path = opj(foldername, subject, filename)
        return struct_path

    # This section connects all the nodes of the pipeline to each other
    reconflow.connect([
        (infosource, reconall, [('subject_id', 'subject_id')]),
        (infosource, reconall, [(('subject_id', pathfinder, data_dir, t1_identifier), 'T1_files')]),
    ])

    # This command runs the recon-all pipeline in parallel (using cpu_count cores)
    reconflow.run('MultiProc', plugin_args={'n_procs': os.cpu_count()})


def segment_subregions(structure: str, subject_dir: str):
    # Configure the FreeSurfer command with the required arguments
    thalamus_segmentation = CommandLine(
        command="segment_subregions",
        args=f"{structure} --cross --sd {subject_path}"
    )
    # Execute the command
    thalamus_segmentation.run()

def segment_hypothalamus(subject_id: str, subject_dir: str):
# Set up the FreeSurfer command with Nipype
    hypothalamus_segmentation = CommandLine(
        command="mri_segment_hypothalamic_subunits",
        args=f"--s {subject_id} --sd {subjects_dir}"
    )
    # Execute the command
    hypothalamus_segmentation.run()