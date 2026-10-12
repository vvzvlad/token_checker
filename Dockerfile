# slim is enough: every requirement installs from a wheel. It lacks `curl`, which
# the HEALTHCHECK needs, so curl is installed explicitly below; without it the
# probe fails forever and auto-heal/auto-update act on that. ci/smoke.py checks it.
FROM python:3.13-slim

WORKDIR /app

# os._exit() in the watchdog skips CPython's buffer flush and docker's stdout is a
# pipe, so without this the message explaining a restart could be lost.
ENV PYTHONUNBUFFERED=1

# gosu is used by the entrypoint to drop privileges from root to the app user.
# curl is used by the HEALTHCHECK; the full python image shipped it, -slim does not.
RUN apt-get update \
    && apt-get install -y --no-install-recommends gosu curl \
    && rm -rf /var/lib/apt/lists/*

# Fixed uid keeps volume ownership stable across image rebuilds.
RUN useradd -m -u 1000 app

# Dependencies as a separate layer: change less often than code → cached better
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# The service keeps no state, but the entrypoint chowns data/ on every start under
# `set -e`, so it has to exist.
RUN mkdir -p data && chown app:app data

# Code only. Deliberately NOT `COPY . .`: tests, CI and the dev requirements stay
# out of the image (see .dockerignore).
COPY src/ src/
COPY main.py ./
# --chmod pins the executable bit: exec-form ENTRYPOINT fails with "permission
# denied" if the bit is lost in the build context (Windows checkout, tar copy).
COPY --chmod=0755 entrypoint.sh /entrypoint.sh

# The probe the deployment acts on: auto-heal restarts an `unhealthy` container,
# and auto-update waits for `healthy` within max(120s, start_period + 15s) before
# accepting a new image. 10s start-period plus 3 retries at 10s settles about 40s
# in; do not lengthen the interval without a matching start-period.
# The port follows HEALTH_PORT like src/settings.py; production does not pass it,
# so the `:-8080` default is what runs.
HEALTHCHECK --interval=10s --timeout=5s --start-period=10s --retries=3 \
  CMD curl -f "http://localhost:${HEALTH_PORT:-8080}/health" || exit 1

# No USER directive on purpose: the entrypoint starts as root, heals /app/data
# ownership (migration from older root-based images) and drops to app via gosu.
# A compose `user:` override is respected (the entrypoint then just execs).
ENTRYPOINT ["/entrypoint.sh"]

# Direct invocation with no shell restart loop, so a startup failure kills the
# container visibly; `restart: unless-stopped` restarts it.
CMD ["python", "main.py"]
