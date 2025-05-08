import pytest
from pathlib import Path
import nibabel as nib
import numpy as np
from numpy.typing import ArrayLike
from core.utils import (
    add_dcm_extension,
    get_folder_names,
    create_folders,
    get_nifti_dimensions,
    remove_double_extension,
)

# Parameterized tests for add_dcm_extension with additional edge cases.
@pytest.mark.parametrize("input_filename,expected", [
    ("file", "file.dcm"),
    ("file.dcm", "file.dcm"),
    ("file.DCM", "file.DCM"),
    ("file.txt", "file.txt.dcm"),
    ("", ".dcm"),  # Edge case: empty string input
])
def test_add_dcm_extension(input_filename, expected):
    result = add_dcm_extension(input_filename)
    assert result == expected, f"Expected '{expected}', got '{result}'"

# Parameterized tests for remove_double_extension including a case-sensitivity check.
@pytest.mark.parametrize("input_path,expected", [
    (Path("file.nii.gz"), "file"),
    (Path("file.nii"), "file"),
    (Path("file.dcm"), "file"),
    (Path("file"), "file"),
])
def test_remove_double_extension(input_path, expected):
    result = remove_double_extension(input_path)
    assert result == expected, f"Expected '{expected}', got '{result}'"

# Use tmp_path fixture to ensure isolated temporary directories.
def test_get_folder_names(tmp_path):
    # Create two subdirectories and one file
    (tmp_path / "folder1").mkdir()
    (tmp_path / "folder2").mkdir()
    (tmp_path / "file.txt").touch()
    folders = get_folder_names(tmp_path)
    assert set(folders) == {"folder1", "folder2"}, f"Expected folders 'folder1' and 'folder2', got {folders}"

def test_create_folders(tmp_path):
    folders = create_folders(tmp_path)
    expected_keys = {"dicom", "nifti", "freesurfer", "samseg", "workflows", "json", "corestats"}
    assert set(folders.keys()) == expected_keys, f"Expected folder keys {expected_keys}, got {folders.keys()}"
    for folder_path in folders.values():
        assert folder_path.exists() and folder_path.is_dir(), f"Folder {folder_path} does not exist or is not a directory"

def test_get_nifti_dimensions(tmp_path):
    # Create a simple 3D NIfTI image
    nifti_file = tmp_path / "test.nii.gz"
    data: ArrayLike = np.zeros((5, 10, 15))
    img = nib.Nifti1Image(data, affine=np.eye(4))
    nib.save(img, nifti_file)
    dims = get_nifti_dimensions(nifti_file)
    assert dims == (5, 10, 15), f"Expected dimensions (5, 10, 15), got {dims}"

def test_get_nifti_dimensions_nonexistent(tmp_path):
    # Test that a FileNotFoundError is raised for a missing file.
    non_existent_file = tmp_path / "nonexistent.nii.gz"
    with pytest.raises(FileNotFoundError) as excinfo:
        get_nifti_dimensions(non_existent_file)
    assert "NIfTI file not found" in str(excinfo.value)

def test_get_nifti_dimensions_invalid(tmp_path):
    # Create an invalid NIfTI file to test error handling.
    invalid_file = tmp_path / "invalid.nii.gz"
    invalid_file.write_text("Not a valid NIfTI file")
    with pytest.raises(Exception):
        get_nifti_dimensions(invalid_file)