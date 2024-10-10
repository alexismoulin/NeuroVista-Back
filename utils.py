import pydicom
from pydicom.errors import InvalidDicomError


def add_dcm_extension_if_pixel_array(filename):
    try:
        # Load the DICOM file
        dicom_file = pydicom.dcmread(filename)

        # Check if it contains a pixel array
        if hasattr(dicom_file, 'pixel_array'):
            # Add .dcm extension if not present
            if not filename.lower().endswith('.dcm'):
                filename += '.dcm'
            return filename
        else:
            # Return None if no pixel array is present
            return None
    except (TypeError, InvalidDicomError):
        # Return None if the file is not a valid DICOM file
        return None