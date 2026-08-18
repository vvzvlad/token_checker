"""The /health endpoint — the liveness signal the deployment acts on.

The Dockerfile's HEALTHCHECK curls this endpoint, and our Portainer build reads
docker's verdict twice over: auto-heal restarts a container docker reports
`unhealthy`, and the auto-update rollback gate waits for `healthy` after it
recreates the container. So what this handler answers is not a debug convenience
— a 503 held for long enough is a restart, and a container that never reaches 200
is an image that gets rolled back.

The three answers are a contract and are pinned by `tests/test_health.py`:
    GET /health, healthy    -> 200 {"status": "healthy"}
    GET /health, unhealthy  -> 503 {"status": "unhealthy", "reason": ...}
    anything else           -> 404
"""

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

# The one reason the service ever reports itself unhealthy: a step of the main
# loop (a balance lookup) is still in flight, or the watchdog has run its timeout
# down far enough that the loop is presumed stuck.
UNHEALTHY_REASON = "Request in progress taking too long"


class HealthCheckHandler(BaseHTTPRequestHandler):
    # CLASS-level, not instance-level, and that is load-bearing: HTTPServer builds
    # a fresh handler instance for every single request, so an instance attribute
    # would be reset before it could ever be read. The consequence for tests is
    # that this flag is process-wide mutable state shared by every test in the
    # suite — tests/conftest.py asserts it is back to True before AND after each
    # test for exactly that reason.
    is_healthy = True

    @classmethod
    def set_health(cls, status: bool):
        cls.is_healthy = status

    def do_GET(self):
        if self.path == '/health':
            if self.is_healthy:
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({"status": "healthy"}).encode())
            else:
                self.send_response(503)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps(
                    {"status": "unhealthy", "reason": UNHEALTHY_REASON}).encode())
        else:
            self.send_response(404)
            self.end_headers()


def run_health_server(port):
    """Serve /health forever. Blocks; call it on a thread."""
    server = HTTPServer(('0.0.0.0', port), HealthCheckHandler)
    server.serve_forever()


def start_health_server(port, logger):
    """Start the health server on a daemon thread and return it.

    A daemon thread on purpose: it must never keep the process alive on its own.
    The whole design of this service is that a fatal problem KILLS the container
    and `restart: unless-stopped` restarts it, and a non-daemon HTTP thread would
    turn that into a process that answers "healthy" while nothing else is left
    running.
    """
    thread = threading.Thread(target=run_health_server, args=(port,), daemon=True)
    thread.start()
    logger.info(f"Health check server started on port {port}")
    return thread
