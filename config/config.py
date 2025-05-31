from pathlib import Path

class Config:
    # Base path for storing data; set via environment variable
    BASE_DATA_PATH = Path("./DATA")
    # Logging configuration
    LOG_LEVEL = "INFO"
    # CORS origins (comma-separated) for production; restrict to your frontend domain(s)
    CORS_ORIGINS = "*"
