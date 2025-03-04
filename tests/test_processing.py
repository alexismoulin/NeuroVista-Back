import pytest
from unittest.mock import MagicMock

from utils import (
    reconall,
    process_lesions,
    segment_subregions,
    segment_hypothalamus,
    run_fastsurfer,
    process_corestats,
)
from app import (
    save_dicoms,
    convert_to_nifti,
    run_reconall,
    process_lesions_for_all,
    run_fastsurfer_for_all,
    generate_json_files
)
from werkzeug.datastructures import FileStorage, ImmutableMultiDict


# --- Fixtures ---

@pytest.fixture
def temp_dir(tmp_path):
    """
    Returns a temporary directory as a pathlib.Path object.
    """
    return tmp_path


@pytest.fixture
def mock_pydicom_dataset():
    ds = MagicMock()
    ds.SeriesDescription = "Series1"
    return ds


# --- Tests ---

def test_save_dicoms(temp_dir, mocker, mock_pydicom_dataset):
    """
    Test that save_dicoms creates the proper directory and calls FileStorage.save with the expected destination.
    """
    # Patch pydicom.dcmread in the app namespace.
    mocker.patch("app.pydicom.dcmread", return_value=mock_pydicom_dataset)
    # Patch add_dcm_extension so it appends '.dcm' if not present.
    mocker.patch("app.add_dcm_extension", side_effect=lambda filename: filename + ".dcm" if not filename.lower().endswith(".dcm") else filename)
    # Patch logger and notify_step to prevent side effects during testing.
    mocker.patch("app.logger")
    mocker.patch("app.notify_step")
    
    # Create a fake FileStorage object.
    mock_file = MagicMock(spec=FileStorage)
    mock_file.filename = "test.dcm"
    mock_file.stream = MagicMock()
    mock_file.save = MagicMock()
    
    # Create an ImmutableMultiDict mimicking request.files.
    files = ImmutableMultiDict([("dicoms", mock_file)])
    
    # Call the function.
    save_dicoms(files, temp_dir)
    
    # The expected destination path.
    expected_path = temp_dir / "Series1" / "test.dcm"
    
    # Verify that the directory was created.
    assert (temp_dir / "Series1").exists()
    # Verify that the file save method was called with the correct destination.
    mock_file.save.assert_called_once_with(dst=str(expected_path))


def test_convert_to_nifti(temp_dir, mocker):
    """
    Test that convert_to_nifti calls dicom2nifti.dicom_series_to_nifti for each DICOM series folder.
    """
    # Create a dedicated DICOM directory.
    dicom_dir = temp_dir / "DICOM"
    dicom_dir.mkdir()
    
    # Create a dummy series folder inside the DICOM directory.
    series_folder = dicom_dir / "Series1"
    series_folder.mkdir()

    # Patch the dicom2nifti function.
    dicom2nifti_mock = mocker.patch("dicom2nifti.dicom_series_to_nifti")

    # Create a separate output NIFTI directory.
    nifti_dir = temp_dir / "NIFTI"
    nifti_dir.mkdir()

    # Call conversion.
    convert_to_nifti(dicom_dir, nifti_dir)

    # Assert that conversion was triggered once.
    dicom2nifti_mock.assert_called_once()
    _, kwargs = dicom2nifti_mock.call_args

    # Check that the original_dicom_directory corresponds to the Series1 folder.
    assert str(series_folder) in kwargs["original_dicom_directory"]
    # And that the output file is in the nifti_dir and has the expected filename.
    expected_output = str(nifti_dir / "Series1.nii.gz")
    assert kwargs["output_file"] == expected_output


def test_reconall_setup(temp_dir, mocker):
    """
    Test the reconall function sets up the workflow when a NIFTI file is present.
    """
    # Create NIFTI directory and a dummy NIFTI file.
    nifti_dir = temp_dir / "NIFTI"
    nifti_dir.mkdir(parents=True)
    nifti_file = nifti_dir / "test.nii.gz"
    nifti_file.touch()

    # Create FREESURFER directory.
    fs_dir = temp_dir / "FREESURFER"
    fs_dir.mkdir(parents=True, exist_ok=True)

    # Patch Workflow.run so that it does not actually execute.
    workflow_run_mock = mocker.patch("nipype.pipeline.engine.Workflow.run")

    # Call reconall.
    reconall(temp_dir)

    # Check that Workflow.run was called.
    workflow_run_mock.assert_called()


def test_process_lesions(temp_dir, mocker):
    """
    Test that process_lesions calls CommandLine.run when the output does not exist,
    and skips calling it when the output already exists.
    """
    # Ensure the SAMSEG output file does not exist.
    freesurfer_path = temp_dir / "FREESURFER"
    samseg_path = temp_dir / "SAMSEG"
    series = "series1"
    # Make sure directories exist.
    freesurfer_path.mkdir(parents=True, exist_ok=True)
    samseg_path.mkdir(parents=True, exist_ok=True)

    # Patch CommandLine.run.
    cmd_run_mock = mocker.patch("nipype.interfaces.base.CommandLine.run")

    # First call – file does not exist, so run should be called.
    process_lesions(freesurfer_path, samseg_path, series)
    assert cmd_run_mock.call_count == 1

    cmd_run_mock.reset_mock()
    # Create the output file so that processing is skipped.
    output_file = samseg_path / series / "samseg.stats"
    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_file.touch()

    process_lesions(freesurfer_path, samseg_path, series)
    # Since the file exists, CommandLine.run should not be called again.
    cmd_run_mock.assert_not_called()


def test_process_lesions_for_all(temp_dir, mocker):
    """
    Test that process_lesions_for_all calls process_lesions for each series.
    """
    # Patch the process_lesions function in the app module.
    process_lesions_mock = mocker.patch("app.process_lesions")
    freesurfer_path = temp_dir / "FREESURFER"
    samseg_path = temp_dir / "SAMSEG"
    folders = ["series1"]

    process_lesions_for_all(folders, freesurfer_path, samseg_path)

    process_lesions_mock.assert_called_once_with(freesurfer_path, samseg_path, "series1")


def test_segment_subregions(temp_dir, mocker):
    """
    Test that segment_subregions calls CommandLine.run if output files are missing,
    and skips when all expected output files exist.
    """
    # Use a temporary subject directory.
    subject_dir = temp_dir
    subject_id = "series1"

    # Patch CommandLine.run.
    cmd_run_mock = mocker.patch("nipype.interfaces.base.CommandLine.run")

    # First, call with no existing output files.
    segment_subregions("thalamus", subject_id, subject_dir)
    # CommandLine.run should be called once.
    assert cmd_run_mock.call_count == 1

    cmd_run_mock.reset_mock()
    # Create the expected output files.
    mri_dir = subject_dir / subject_id / "mri"
    mri_dir.mkdir(parents=True, exist_ok=True)
    for filename in ["ThalamicNuclei.mgz", "ThalamicNuclei.volumes.txt"]:
        (mri_dir / filename).touch()

    segment_subregions("thalamus", subject_id, subject_dir)
    # Should skip segmentation because outputs exist.
    cmd_run_mock.assert_not_called()


def test_segment_hypothalamus(temp_dir, mocker):
    """
    Test that segment_hypothalamus calls CommandLine.run.
    """
    # Patch CommandLine.run.
    cmd_run_mock = mocker.patch("nipype.interfaces.base.CommandLine.run")

    segment_hypothalamus("series1", temp_dir)
    cmd_run_mock.assert_called_once()


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
        parallel=True,
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


def test_generate_json_files(temp_dir, mocker):
    # Patch the functions in the module where generate_json_files is defined.
    jsonifier_mock = mocker.patch("app.run_jsonifier")
    json_average_mock = mocker.patch("app.run_json_average")
    global_json_mock = mocker.patch("app.run_global_json")

    freesurfer_path = temp_dir / "FREESURFER"
    fastsurfer_path = temp_dir / "FASTSURFER"
    samseg_path = temp_dir / "SAMSEG"
    json_folder = temp_dir / "JSON"

    # Ensure dummy directories exist.
    for d in [freesurfer_path, fastsurfer_path, samseg_path]:
        d.mkdir(parents=True, exist_ok=True)

    generate_json_files(["series1"], freesurfer_path, fastsurfer_path, samseg_path, json_folder)

    # Check that run_jsonifier was called with expected directories.
    jsonifier_mock.assert_called_once()
    # Check that average JSON functions were called.
    assert json_average_mock.call_count == 3
    global_json_mock.assert_called_once()


def test_process_corestats(temp_dir):
    """
    Test that process_corestats copies and renames stats files from FreeSurfer and FastSurfer.
    """
    fs_stats_dir = temp_dir / "FREESURFER" / "stats"
    fastsurfer_stats_dir = temp_dir / "FASTSURFER" / "stats"
    corestats_folder = temp_dir / "CORESTATS"

    fs_stats_dir.mkdir(parents=True, exist_ok=True)
    fastsurfer_stats_dir.mkdir(parents=True, exist_ok=True)

    # Create dummy .stats files.
    (fs_stats_dir / "test.stats").touch()
    (fastsurfer_stats_dir / "test2.stats").touch()

    process_corestats(temp_dir / "FREESURFER", temp_dir / "FASTSURFER", corestats_folder)

    # After processing, the .stats files should have been renamed to .txt.
    assert (corestats_folder / "test.txt").exists()
    assert (corestats_folder / "test2.txt").exists()


# --- Helper matcher for flexible argument checking ---

class Any:
    """
    A simple matcher that can be used in assertions to indicate that any value is acceptable.
    """

    def __eq__(self, other):
        return True