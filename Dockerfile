# Friendlier for wheels and fewer resolver headaches
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONOPTIMIZE=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    DEBIAN_FRONTEND=noninteractive

WORKDIR /usr/src/app

# System deps you were using
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg ca-certificates tzdata \
 && rm -rf /var/lib/apt/lists/*

ENV FFMPEG_BIN=/usr/bin/ffmpeg

# --- Key: stabilize packaging toolchain BEFORE installing your deps ---
RUN python -m pip install --upgrade "pip<24" "setuptools<70" "wheel<0.44" "packaging<24"

# Install Python deps (requirements.txt should NOT pin packaging==25.0)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# App code
COPY . .

# Run as non-root (good for Cloud Run)
RUN addgroup --system app && adduser --system --ingroup app app \
 && chown -R app:app /usr/src/app
USER app

# Gunicorn entrypoint (Cloud Run provides $PORT)
CMD exec gunicorn --bind :$PORT --workers 1 --worker-class gthread --threads 8 --timeout 0 app:app
