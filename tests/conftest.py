import pytest
from pathlib import Path
import tempfile
import shutil
from app import app as flask_app

@pytest.fixture
def app():
    """Provide the Flask app for testing."""
    flask_app.config.update({
        "TESTING": True,
    })
    yield flask_app

@pytest.fixture
def client(app):
    """Provide a test client for the Flask app."""
    return app.test_client()

@pytest.fixture
def temp_dir():
    """Create a temporary directory for tests."""
    temp = Path(tempfile.mkdtemp())
    yield temp
    shutil.rmtree(temp, ignore_errors=True)