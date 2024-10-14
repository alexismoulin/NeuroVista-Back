def add_dcm_extension(filename):
    if not filename.lower().endswith('.dcm'):
        filename += '.dcm'
    return filename