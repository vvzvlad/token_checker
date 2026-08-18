"""The /health endpoint's three answers.

This is the contract our Portainer build acts on twice over: auto-heal restarts a
container docker calls `unhealthy`, and auto-update waits for `healthy` after it
recreates one before it accepts the new image instead of rolling it back. So a
change to a status code here is a change to how the deployment behaves, not a
cosmetic one.

The handler is exercised without a socket: `BaseHTTPRequestHandler.__init__`
would read a real connection, so a subclass bypasses it and records what the
handler tried to send.
"""

import io
import json

import pytest

from src.health import UNHEALTHY_REASON, HealthCheckHandler


class RecordingHandler(HealthCheckHandler):
    """A HealthCheckHandler whose responses go into memory instead of a socket."""

    def __init__(self, path):
        # BaseHTTPRequestHandler.__init__ parses a request off a real socket, so
        # it is deliberately NOT called. Everything do_GET touches is set here.
        self.path = path
        self.status = None
        self.headers_sent = []
        self.ended = False
        self.wfile = io.BytesIO()

    def send_response(self, code, message=None):
        self.status = code

    def send_header(self, name, value):
        self.headers_sent.append((name, value))

    def end_headers(self):
        self.ended = True

    def body(self):
        return self.wfile.getvalue()


def test_healthy_answers_200_with_the_documented_body():
    handler = RecordingHandler("/health")
    handler.do_GET()
    assert handler.status == 200
    assert json.loads(handler.body()) == {"status": "healthy"}
    assert ("Content-type", "application/json") in handler.headers_sent
    assert handler.ended


def test_unhealthy_answers_503_with_a_reason(health_flag):
    health_flag.set_health(False)
    handler = RecordingHandler("/health")
    handler.do_GET()
    # 503 rather than 500: this is "not ready right now", which is what docker's
    # retry count is for, and it is the code the whole deployment is tuned to.
    assert handler.status == 503
    assert json.loads(handler.body()) == {"status": "unhealthy", "reason": UNHEALTHY_REASON}


@pytest.mark.parametrize("path", ["/", "/healthz", "/health/", "/metrics", ""])
def test_any_other_path_is_404_and_has_no_body(path):
    # Only the exact path counts. `curl -f http://localhost:8080/health` in the
    # Dockerfile fails on a 404, so a handler that answered 200 to everything
    # would be indistinguishable from a healthy one — and a typo in the
    # HEALTHCHECK line would then never be noticed.
    handler = RecordingHandler(path)
    handler.do_GET()
    assert handler.status == 404
    assert handler.body() == b""


def test_a_404_is_returned_whatever_the_health_flag_says(health_flag):
    health_flag.set_health(False)
    handler = RecordingHandler("/other")
    handler.do_GET()
    assert handler.status == 404


def test_the_flag_lives_on_the_class_so_every_request_sees_it(health_flag):
    # HTTPServer builds a NEW handler per request; an instance attribute would be
    # reset before it could ever be read, and the endpoint would answer "healthy"
    # forever. This is what makes the flag shared state — see tests/conftest.py.
    health_flag.set_health(False)
    first = RecordingHandler("/health")
    second = RecordingHandler("/health")
    first.do_GET()
    second.do_GET()
    assert first.status == 503
    assert second.status == 503
    health_flag.set_health(True)
    third = RecordingHandler("/health")
    third.do_GET()
    assert third.status == 200
