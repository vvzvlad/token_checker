import os

import pytest

# Provide the required credentials BEFORE any test module imports src.settings
# (Settings() is instantiated at import time and would otherwise exit(1)). In CI
# the same variables are injected via the workflow's test job, so the suite does
# not silently depend on this file keeping its defaults.
#
# These are obviously-fake placeholders and nothing in the suite ever performs a
# real request with them: every HTTP call is mocked at the `requests` boundary
# and the Grist client is replaced by a recording double.
os.environ.setdefault("GRIST_SERVER", "http://grist.invalid")
os.environ.setdefault("GRIST_DOC_ID", "test-doc")
os.environ.setdefault("GRIST_API_KEY", "test-key")
os.environ.setdefault("ETHERSCAN_API_KEY", "test-etherscan-key")

# Imported only after the environment above exists, for the same reason.
from src.health import HealthCheckHandler  # noqa: E402


@pytest.fixture(autouse=True)
def health_flag_is_clean():
    """Guard the ONE piece of process-wide mutable state this suite has.

    `HealthCheckHandler.is_healthy` lives on the CLASS (HTTPServer builds a fresh
    handler per request, so it has to), which means every test in the run shares
    it. A test that flips it to False and does not put it back leaves the next
    test asserting against somebody else's state — and that next test fails, or
    worse passes for the wrong reason, in a way that depends on collection order
    and disappears when it is run alone.

    Checked BOTH before and after each test, and the "after" is the half that
    actually catches anything: "before" only tells the victim that something
    earlier was dirty, while "after" names the test that dirtied it. Tests that
    need the flag flipped use the `health_flag` fixture below, whose teardown
    runs first because it is set up later than this autouse one.
    """
    assert HealthCheckHandler.is_healthy is True, (
        "HealthCheckHandler.is_healthy was already False when this test started — "
        "an earlier test left it dirty")
    yield
    assert HealthCheckHandler.is_healthy is True, (
        "this test left HealthCheckHandler.is_healthy False; it is class-level "
        "state shared with every other test, so put it back (see the "
        "`health_flag` fixture)")


@pytest.fixture
def health_flag():
    """Let a test flip the class-level health flag and restore it afterwards."""
    yield HealthCheckHandler
    HealthCheckHandler.set_health(True)


class NullLogger:
    """A logger that records nothing but answers every call the code makes."""

    def info(self, *args, **kwargs):
        pass

    def warning(self, *args, **kwargs):
        pass

    def error(self, *args, **kwargs):
        pass


class RecordingLogger:
    """Keeps every message, so a test can assert on what was said and at what level."""

    def __init__(self):
        self.info_messages = []
        self.warning_messages = []
        self.error_messages = []
        self.handlers = []

    def info(self, message, *args, **kwargs):
        self.info_messages.append(str(message))

    def warning(self, message, *args, **kwargs):
        self.warning_messages.append(str(message))

    def error(self, message, *args, **kwargs):
        self.error_messages.append(str(message))


@pytest.fixture
def logger():
    return RecordingLogger()
