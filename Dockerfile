# python:3.13-slim. The full image put this at 1.14 GB, of which the service is
# 28 MB: the rest is base layers, and a single 656 MB one is the build toolchain
# (gcc, make, *-dev) that the official image needs once to compile CPython and
# that is dead weight at runtime. Nothing here compiles — every requirement
# installs from a wheel — so -slim costs nothing in build time.
#
# What -slim does cost is `curl`, which the HEALTHCHECK below runs against the
# /health endpoint. It is therefore installed explicitly in the apt layer next to
# gosu. Do NOT drop that install: without curl the endpoint keeps answering
# perfectly while docker's probe fails forever, and a failing probe is not a
# cosmetic problem here — Portainer's auto-heal restarts on it and its
# auto-update rolls a new image back over it. ci/smoke.py checks both paths.
FROM python:3.13-slim

WORKDIR /app

# os._exit() in the watchdog and a container-killed process both skip CPython's
# buffer flush, and docker gives the container a pipe, which CPython
# block-buffers. Without this the last thing the process says — the watchdog's
# reason for killing it, or the startup guard naming a missing variable — can die
# in a buffer and never reach `docker logs`, i.e. exactly the message that
# explains the restart.
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

# Runtime state directory. This service keeps no state of its own today, but the
# directory has to exist and be owned by app: the entrypoint chowns it on every
# start and `set -e` would kill the container if it were missing.
RUN mkdir -p data && chown app:app data

# Code only. Deliberately NOT `COPY . .`: tests, CI and the dev requirements stay
# out of the image (see .dockerignore).
COPY src/ src/
COPY main.py ./
# --chmod pins the executable bit: exec-form ENTRYPOINT fails with "permission
# denied" if the bit is lost in the build context (Windows checkout, tar copy).
COPY --chmod=0755 entrypoint.sh /entrypoint.sh

# The probe the deployment acts on, in both directions: our Portainer build's
# auto-heal restarts a container docker reports `unhealthy`, and its auto-update
# waits for `healthy` after recreating the container — within
# max(120s, start_period + 15s) — before it accepts the new image instead of
# rolling it back. So these timings are part of the deploy mechanism: 10s
# start-period plus 3 retries at a 10s interval settles the verdict about 40s in,
# comfortably inside that window. Do not lengthen the interval without a
# start-period to match, and do not remove the line: without a HEALTHCHECK the
# rollback gate is not stricter, it is simply gone.
# The port follows HEALTH_PORT so it cannot drift from src/settings.py, which
# reads the same variable; the `:-8080` default is the value production runs on,
# because the crypt-common stack does not pass HEALTH_PORT at all.
HEALTHCHECK --interval=10s --timeout=5s --start-period=10s --retries=3 \
  CMD curl -f "http://localhost:${HEALTH_PORT:-8080}/health" || exit 1

# No USER directive on purpose: the entrypoint starts as root, heals /app/data
# ownership (migration from older root-based images) and drops to app via gosu.
# A compose `user:` override is respected (the entrypoint then just execs).
ENTRYPOINT ["/entrypoint.sh"]

# A DIRECT invocation, not the old `while true; do python token_checker.py;
# sleep 10; done`. That wrapper made every startup failure INVISIBLE: a container
# with broken configuration looked alive, because the shell survived the program
# and restarted it every ten seconds forever, so `docker ps` and any liveness
# check based on it reported a healthy service that had never once worked. The
# process is meant to fail loudly instead — production runs this with
# `restart: unless-stopped`, which is what does the restarting, and with a
# healthcheck, which is what notices.
CMD ["python", "main.py"]
