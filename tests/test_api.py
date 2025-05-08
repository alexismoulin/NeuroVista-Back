from io import BytesIO
from werkzeug.datastructures import FileStorage

# Helper function to create JSON files in the expected directory structure.
def create_json_file(temp_dir, patient, study, file_name, content):
    json_dir = temp_dir / "DATA" / patient / study / "JSON"
    json_dir.mkdir(parents=True, exist_ok=True)
    (json_dir / file_name).write_text(content)

def test_home(client):
    """Test that the home endpoint returns the expected response."""
    response = client.get("/")
    assert response.data == b"Home"
    assert response.status_code == 200

def test_run_script_processing_not_in_progress(client, mocker):
    """Test /run_script when no processing is active."""
    mocker.patch("app.run_processing")
    mocker.patch("app.processing_event.is_set", return_value=False)
    file_data = BytesIO(b"dummy data")
    file_storage = FileStorage(
        stream=file_data, filename="test.dcm", content_type="application/dicom"
    )
    data = {
        "study": "Study1",
        "patient": "Patient1",
        "dicoms": file_storage
    }
    response = client.post("/run_script", data=data, content_type="multipart/form-data")
    assert response.status_code == 202
    assert response.json["message"] == "Processing started"

def test_run_script_processing_in_progress(client, mocker):
    """Test /run_script when processing is already in progress."""
    mocker.patch("app.processing_event.is_set", return_value=True)
    file_data = BytesIO(b"dummy data")
    file_storage = FileStorage(
        stream=file_data, filename="test.dcm", content_type="application/dicom"
    )
    data = {
        "study": "Study1",
        "patient": "Patient1",
        "dicoms": file_storage
    }
    response = client.post("/run_script", data=data, content_type="multipart/form-data")
    assert response.status_code == 400

def test_cortical(client, temp_dir, monkeypatch):
    """Test that the /cortical endpoint returns the expected JSON data."""
    # Create the expected JSON file
    create_json_file(temp_dir, "patient1", "study1", "cortical.json", '{"data": "test"}')
    # Override the base data path to point to our temporary directory
    monkeypatch.setattr("app.BASE_DATA_PATH", temp_dir / "DATA")
    response = client.get("/cortical/patient1/study1")
    assert response.status_code == 200, f"Unexpected status code: {response.status_code}"
    assert response.json == {"data": "test"}

def test_subcortical(client, temp_dir, monkeypatch):
    """Test that the /subcortical endpoint returns the expected JSON data."""
    create_json_file(temp_dir, "patient1", "study1", "subcortical.json", '{"data": "test"}')
    monkeypatch.setattr("app.BASE_DATA_PATH", temp_dir / "DATA")
    response = client.get("/subcortical/patient1/study1")
    assert response.status_code == 200, f"Unexpected status code: {response.status_code}"
    assert response.json == {"data": "test"}

def test_general(client, temp_dir, monkeypatch):
    """Test that the /general endpoint returns the expected JSON data."""
    create_json_file(temp_dir, "patient1", "study1", "general.json", '{"data": "test"}')
    monkeypatch.setattr("app.BASE_DATA_PATH", temp_dir / "DATA")
    response = client.get("/general/patient1/study1")
    assert response.status_code == 200, f"Unexpected status code: {response.status_code}"
    assert response.json == {"data": "test"}

def test_series(client, temp_dir, monkeypatch, mocker):
    """Test that the /series endpoint processes series data correctly."""
    # Set up the directory structure:
    # DATA/patient1/study1/DICOM/series1
    # DATA/patient1/study1/NIFTI with a file series1.nii.gz
    data_dir = temp_dir / "DATA" / "patient1" / "study1"
    dicom_dir = data_dir / "DICOM"
    nifti_dir = data_dir / "NIFTI"
    dicom_dir.mkdir(parents=True, exist_ok=True)
    (dicom_dir / "series1").mkdir(exist_ok=True)
    nifti_dir.mkdir(parents=True, exist_ok=True)
    (nifti_dir / "series1.nii.gz").touch()

    mocker.patch("app.get_nifti_dimensions", return_value=(10, 10, 10))
    monkeypatch.setattr("app.BASE_DATA_PATH", temp_dir / "DATA")
    response = client.get("/series/patient1/study1")
    assert response.status_code == 200, f"Unexpected status code: {response.status_code}"
    assert response.json == {"series1": [10, 10, 10]}