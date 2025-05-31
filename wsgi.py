from app import create_app

# Expose the WSGI callable for Gunicorn/UWSGI
app = create_app()
