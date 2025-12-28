from pathlib import Path
from configparser import ConfigParser

class Config:
    parser = ConfigParser()
    parser.read(filenames="./config.ini")
    # Base path for storing data; falls back to ./DATA
    BASE_DATA_PATH = Path(parser.get(section="DATA", option="real_data", fallback="./DATA"))
    # Logging configuration
    LOG_LEVEL = "INFO"
    # CORS origins (comma-separated) for production; restrict to your frontend domain(s)
    CORS_ORIGINS = "*"
