import logging
from flask import Flask
from flask_cors import CORS
from config.config import Config


def create_app() -> Flask:
    """
    Application factory for creating Flask app with production settings.
    """
    app = Flask(__name__, instance_relative_config=False)
    app.config.from_object(obj=Config)

    # Configure logging
    handler = logging.StreamHandler()
    handler.setLevel(app.config["LOG_LEVEL"])
    formatter = logging.Formatter(fmt="[%(asctime)s] %(levelname)s in %(module)s: %(message)s")
    handler.setFormatter(fmt=formatter)
    app.logger.setLevel(app.config["LOG_LEVEL"])
    app.logger.addHandler(hdlr=handler)

    # Enable CORS
    CORS(app, origins=app.config["CORS_ORIGINS"], supports_credentials=True)

    # Register Blueprints
    from .processing_routes import processing_bp
    from .data_routes import data_bp
    app.register_blueprint(blueprint=processing_bp)
    app.register_blueprint(blueprint=data_bp)

    return app
