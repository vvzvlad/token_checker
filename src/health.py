"""The /health endpoint — the liveness signal the deployment acts on.

The Dockerfile's HEALTHCHECK curls this endpoint. Auto-heal restarts a container
docker reports `unhealthy`, and the auto-update rollback gate waits for `healthy`:
a 503 held long enough is a restart, a container that never reaches 200 is rolled
back.

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
    # Class-level: HTTPServer builds a new handler instance per request. It is
    # process-wide state, so tests/conftest.py asserts it is True around each test.
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

    A daemon thread so it never keeps the process alive on its own: a fatal
    problem must kill the container, not leave a process answering "healthy".
    """
    thread = threading.Thread(target=run_health_server, args=(port,), daemon=True)
    thread.start()
    logger.info(f"Health check server started on port {port}")
    return thread
