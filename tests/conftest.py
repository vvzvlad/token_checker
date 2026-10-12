import os

import pytest

# `src.redact` holds no configuration of its own and imports nothing, so it is
# safe to read here — unlike `src.settings`, which builds Settings() at import
# time and is exactly what the environment below exists to satisfy.
from src.redact import MIN_SECRET_LENGTH


def long_enough(value, pad="x"):
    """`value`, padded until it clears the credential floor in src/settings.py.

    Every credential stand-in in the suite goes through this, so raising
    `MIN_SECRET_LENGTH` moves them all. Only the tests about the boundary go red:
    `test_several_secrets_are_removed_in_one_pass` (eight-character literals) and
    tests/test_ci_placeholders.py (literals outside the suite).

    Values that already clear the floor are left alone by `str.ljust`, so a
    stand-in keeps the exact text a failing assertion prints. `pad` exists for the
    values whose SHAPE a test depends on — a chat id has to stay numeric.
    """
    return value.ljust(MIN_SECRET_LENGTH, pad)


# Provide the required credentials BEFORE any test module imports src.settings
# (Settings() is instantiated at import time and would otherwise exit(1)). In CI
# the same variables are injected via the workflow's test job, so the suite does
# not silently depend on this file keeping its defaults.
#
# The workflow literals and `SMOKE_ENV` in `ci/smoke.py` take precedence over
# these and are not derived; tests/test_ci_placeholders.py measures them against
# the constant. All values are fake: HTTP is mocked and Grist is a recording
# double. The two API keys carry `Secret`, so they go through `long_enough`;
# `GRIST_DOC_ID` has no floor.
os.environ.setdefault("GRIST_SERVER", "http://grist.invalid")
os.environ.setdefault("GRIST_DOC_ID", "test-doc")
os.environ.setdefault("GRIST_API_KEY", long_enough("test-grist-api-key"))
os.environ.setdefault("ETHERSCAN_API_KEY", long_enough("test-etherscan-api-key"))

# Imported only after the environment above exists, for the same reason.
from src.health import HealthCheckHandler  # noqa: E402


@pytest.fixture(autouse=True)
def health_flag_is_clean():
    """Guard the ONE piece of process-wide mutable state this suite has.

    `HealthCheckHandler.is_healthy` lives on the class, so every test shares it.
    Checked before and after each test; "after" names the test that dirtied it.
    Tests that need it flipped use the `health_flag` fixture below, whose
    teardown runs first because it is set up later than this autouse one.
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
