import pytest
import json
import numpy as np
import nibabel as nib
from core.jsonifier import get_volume, read_volume_file, process_paired_volumes, record_nifti_dimensions


@pytest.fixture
def sample_nuclei():
    """Fixture providing sample nuclei data."""
    return [{"Thalamus": 100.0}, {"Hippocampus": 200.0}]


def test_get_volume(sample_nuclei):
    """Test retrieval of volumes from a nuclei list."""
    assert get_volume("Thalamus", sample_nuclei) == 100.0
    assert get_volume("Hippocampus", sample_nuclei) == 200.0
    assert get_volume("Amygdala", sample_nuclei) is None  # Not found returns None


@pytest.mark.parametrize(
    "file_content, expected_output",
    [
        ("Structure1 100.0\nStructure2 200.0\n", [["Structure1", "100.0"], ["Structure2", "200.0"]]),
        # Test with extra spaces and an empty line that should be skipped.
        ("  Structure1   100.0  \n\nStructure2   200.0 \n", [["Structure1", "100.0"], ["Structure2", "200.0"]])
    ]
)
def test_read_volume_file(tmp_path, file_content, expected_output):
    """Test that the file is read correctly and split into expected fields."""
    file_path = tmp_path / "volumes.txt"
    file_path.write_text(file_content)
    data = read_volume_file(file_path)
    assert data == expected_output


def test_read_volume_file_not_found(tmp_path):
    """Test that reading a non-existent file raises FileNotFoundError."""
    non_existent_file = tmp_path / "missing.txt"
    with pytest.raises(FileNotFoundError, match=f"File not found: {non_existent_file}"):
        read_volume_file(non_existent_file)


def test_process_paired_volumes(tmp_path):
    """Test that paired volume files are processed correctly."""
    left_file = tmp_path / "left.txt"
    right_file = tmp_path / "right.txt"
    left_file.write_text("StructureA 100.0\nStructureB 150.0\n")
    right_file.write_text("StructureA 110.0\nStructureB 160.0\n")

    volumes = process_paired_volumes(left_file, right_file)
    expected = [
        {"Structure": "StructureA", "LHS Volume (mm3)": 100.0, "RHS Volume (mm3)": 110.0},
        {"Structure": "StructureB", "LHS Volume (mm3)": 150.0, "RHS Volume (mm3)": 160.0},
    ]
    assert volumes == expected


def test_process_paired_volumes_invalid_data(tmp_path, caplog):
    """Test that invalid data rows are skipped and appropriate warnings are logged."""
    left_file = tmp_path / "left.txt"
    right_file = tmp_path / "right.txt"
    left_file.write_text("StructureA invalid\n")  # Invalid float for volume
    right_file.write_text("StructureA 110.0\n")

    caplog.set_level("WARNING")
    volumes = process_paired_volumes(left_file, right_file)
    # Expect no valid rows to be processed.
    assert volumes == []
    assert "Skipping row due to error" in caplog.text


def test_record_nifti_dimensions(tmp_path):
    study = tmp_path / 'study'
    dicom = study / 'DICOM'
    nifti = study / 'NIFTI'
    json_dir = study / 'JSON'
    # Create directories
    dicom.mkdir(parents=True)
    nifti.mkdir(parents=True)
    json_dir.mkdir(parents=True)
    # Create two series directories in DICOM
    (dicom / 's1').mkdir()
    (dicom / 's2').mkdir()
    # Create minimal NIfTI files with known dimensions
    data_shape = (4, 5, 6)
    arr: np.ndarray = np.zeros(data_shape)
    affine: np.ndarray = np.eye(4)
    img1 = nib.Nifti1Image(arr, affine)  # type: ignore
    img2 = nib.Nifti1Image(arr, affine)  # type: ignore
    nib.save(img1, nifti / 's1.nii.gz')
    nib.save(img2, nifti / 's2.nii.gz')
    # Run the function
    record_nifti_dimensions(study)
    out_file = json_dir / 'niftiDimensions.json'
    assert out_file.exists()
    data = json.loads(out_file.read_text())
    # Each series should map to the correct shape
    assert data == {'s1': list(data_shape), 's2': list(data_shape)}
