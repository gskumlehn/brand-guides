import os
def load_config():
    return {
        "FLASK_ENV": os.getenv("FLASK_ENV", "production"),
        "PORT": int(os.getenv("PORT", "8080")),
        "GCP_PROJECT": os.getenv("GCP_PROJECT", ""),
        "BQ_PROJECT": os.getenv("BQ_PROJECT", os.getenv("GCP_PROJECT", "")),
        "BQ_DATASET": os.getenv("BQ_DATASET", "brand_guides"),
        "GCS_BUCKET": os.getenv("GCS_BUCKET", ""),
        "USE_GCS_SIGNED_URLS": os.getenv("USE_GCS_SIGNED_URLS", "false").lower() == "true",
        "GCS_SIGNED_URL_EXPIRY_SECONDS": int(os.getenv("GCS_SIGNED_URL_EXPIRY_SECONDS", "3600")),
        "SERVICE_ACCOUNT_PATH": os.getenv("SERVICE_ACCOUNT_PATH", "/secrets/service-acocunt.json"),
    }
