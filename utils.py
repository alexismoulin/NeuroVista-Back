import os
from os.path import join as opj
import pathlib
from nipype.interfaces.freesurfer import ReconAll
from nipype.interfaces.utility import IdentityInterface
from nipype.pipeline.engine import Workflow, Node
from nipype.interfaces.base import CommandLine
import logging

logging.basicConfig(level=logging.INFO)

def add_dcm_extension(filename: str) -> str:
    return filename if filename.lower().endswith('.dcm') else filename + '.dcm'

def freesurfer(experiment_dir: str, series: str):
    data_dir = opj(experiment_dir, 'NIFTI')
    fs_folder = opj(experiment_dir, 'FREESURFER')
    t1_identifier = 'struct.nii.gz'
    subject_list = [series]

    pathlib.Path(fs_folder).mkdir(parents=True, exist_ok=True)
    reconflow = Workflow(name="reconflow")
    reconflow.base_dir = opj(experiment_dir, 'workingdir_reconflow')

    infosource = Node(IdentityInterface(fields=['subject_id']), name="infosource")
    infosource.iterables = ('subject_id', subject_list)
    reconall = Node(ReconAll(directive='all', subjects_dir=fs_folder), name="reconall")

    def pathfinder(subject, foldername, filename):
        from os.path import join as opj
        return opj(foldername, subject, filename)

    reconflow.connect([
        (infosource, reconall, [('subject_id', 'subject_id')]),
        (infosource, reconall, [(('subject_id', pathfinder, data_dir, t1_identifier), 'T1_files')]),
    ])

    try:
        reconflow.run('MultiProc', plugin_args={'n_procs': os.cpu_count()})
        logging.info(f"Recon-all completed for series {series}")
    except Exception as e:
        logging.error(f"Error in FreeSurfer recon-all: {e}")

def segment_subregions(structure: str, subject_id):
    command = CommandLine(command="segment_subregions", args=f"{structure} --cross {subject_id}")
    try:
        command.run()
        logging.info(f"{structure} segmentation completed")
    except Exception as e:
        logging.error(f"Error during {structure} segmentation: {e}")

def segment_hypothalamus(subject_id: str, subject_dir: pathlib.Path):
    command = CommandLine(command="mri_segment_hypothalamic_subunits", args=f"--s {subject_id} --sd {subject_dir}")
    try:
        command.run()
        logging.info("Hypothalamus segmentation completed")
    except Exception as e:
        logging.error(f"Error during hypothalamus segmentation: {e}")