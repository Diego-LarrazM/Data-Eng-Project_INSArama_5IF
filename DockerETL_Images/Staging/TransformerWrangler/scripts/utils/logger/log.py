import logging
import sys

LOG = logging.getLogger("TF-W")
LOG.setLevel(logging.INFO)

# Only add handler if none exists (prevents double logging)
if not LOG.handlers:
    handler = logging.StreamHandler(sys.stdout)  # Airflow captures stdout
    LOG.addHandler(handler)
