import os

import pytest

# `src.redact` holds no configuration of its own and imports nothing, so it is
# safe to read here — unlike `src.settings`, which builds Settings() at import
# time and is exactly what the environment below exists to satisfy.
from src.redact import MIN_SECRET_LENGTH


def long_enough(value, pad="x"):
    """`value`, padded until it clears the credential floor in src/settings.py.

    DERIVED from the constant, not merely chosen above it, and that is the whole
    point of the helper. Every credential stand-in in this suite goes through it —
    here, in test_settings, in test_balances, in test_redact, in test_watchdog —
    so raising `MIN_SECRET_LENGTH` moves them all with it and the suite stays
    green apart from the tests that are deliberately about the raise:
    `test_several_secrets_are_removed_in_one_pass` in tests/test_redact.py pins
    the boundary with eight-character literals precisely so that a raise goes red
    there, and tests/test_ci_placeholders.py goes red for the literals OUTSIDE
    this suite that no derivation can reach (see the note below). That is the
    promise, and it is the whole of it.

    Hard-coded stand-ins could not keep it, in two different ways, both of which
    hide the one failure that matters. The values in this file make the suite
    importable at all, so one falling under a raised floor turns the change into a
    collection-time exit(1) — no test named, no assertion pointing at the cause.
    The ones in the test modules turn it into a wall of failures across tests with
    nothing to do with the floor.

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
# That injection is one of THREE places the derivation above does not reach. The
# values in `.gitea/workflows/tests.yml` and in `image-check-publish.yml` are
# literals in a YAML file, they take precedence over the `setdefault`s here, and no
# python runs on them; `SMOKE_ENV` in `ci/smoke.py` carries a third copy, for the
# long-lived container the image gate starts. All three have to be lengthened in
# the same commit as `MIN_SECRET_LENGTH`.
#
# Said here for the reader, but NOT relied on: this note is what used to be the
# only thing standing between raising the constant and a run that is green locally
# and red in CI, and it had already gone stale by one whole source — it named the
# two workflows and not the gate. tests/test_ci_placeholders.py now measures all
# three against the constant, so the failure arrives in `make test`, on the
# workstation, naming the file and the variable.
#
# These are obviously-fake placeholders and nothing in the suite ever performs a
# real request with them: every HTTP call is mocked at the `requests` boundary
# and the Grist client is replaced by a recording double.
#
# The two API KEYS are the fields carrying `Secret` in src/settings.py, i.e. a
# minimum length taken from `MIN_SECRET_LENGTH`, so they go through the helper
# above. `GRIST_DOC_ID` deliberately does not: it is an address, it carries
# `Required`, and padding it would imply a floor that does not exist there.
os.environ.setdefault("GRIST_SERVER", "http://grist.invalid")
os.environ.setdefault("GRIST_DOC_ID", "test-doc")
os.environ.setdefault("GRIST_API_KEY", long_enough("test-grist-api-key"))
os.environ.setdefault("ETHERSCAN_API_KEY", long_enough("test-etherscan-api-key"))

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
