import os

SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "mfs-superset-secret-change-in-prod")

# Disable CSRF for local dev (re-enable in production)
WTF_CSRF_ENABLED = False
