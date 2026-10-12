"""The placeholder credentials CI hands to this program, measured against the floor.

`MIN_SECRET_LENGTH` is not a number this suite may drift away from: `src/settings.py`
imports it as the `min_length` of its `Secret` alias, so a credential below it fails
validation at startup. Everything the SUITE feeds itself is derived from the constant
through `long_enough` in tests/conftest.py and therefore moves with it — but three
places outside python hand the same program the same variables, and they hold
LITERALS:

* `.gitea/workflows/tests.yml`, where the test job passes them to the container it
  runs pytest in;
* `.gitea/workflows/image-check-publish.yml`, which does the same in its own test job;
* `SMOKE_ENV` in `ci/smoke.py`, the configuration for the long-lived container the
  image gate starts and then `docker exec`s into.

None of them goes through the helper and all three take precedence over the
`setdefault`s in tests/conftest.py, so raising `MIN_SECRET_LENGTH` past their length
would otherwise fail only in CI. This file asserts of each placeholder credential what
`Secret` asserts of it: length, and nothing else.
"""

import re
from pathlib import Path

import pytest

from ci.smoke import SMOKE_ENV
from src.redact import MIN_SECRET_LENGTH

REPO_ROOT = Path(__file__).resolve().parents[1]

# Both workflow files, because they carry their own copy of the block: the two jobs
# run in different workflows and neither reads the other, which is exactly how one
# gets updated and the other does not.
WORKFLOWS = (
    ".gitea/workflows/tests.yml",
    ".gitea/workflows/image-check-publish.yml",
)

# The variables carrying `Secret` / `OptionalSecret` in src/settings.py. The Grist
# address and doc id have no floor. `TELEGRAM_BOT_TOKEN` is listed although no source
# sets one: optional means absent, not short.
CREDENTIAL_VARS = ("GRIST_API_KEY", "ETHERSCAN_API_KEY", "TELEGRAM_BOT_TOKEN")

# What every source has to be found supplying, so a regex that stops matching (a
# reformatted `docker run` block) cannot pass by finding nothing.
REQUIRED_EVERYWHERE = ("GRIST_API_KEY", "ETHERSCAN_API_KEY")

# `docker run -e NAME=value` as the workflows spell it. The value stops at whitespace,
# and the trailing `\` of the continued shell line is stripped separately so a block
# written without the space before it is read the same way.
DOCKER_ENV_FLAG = re.compile(r"-e\s+([A-Z][A-Z0-9_]*)=(\S+)")


def _from_workflow(relative_path):
    """Every `-e NAME=value` in a workflow file, as a mapping."""
    text = (REPO_ROOT / relative_path).read_text(encoding="utf-8")
    return {name: value.rstrip("\\") for name, value in DOCKER_ENV_FLAG.findall(text)}


def _from_smoke_env():
    """`SMOKE_ENV` — a list of `NAME=value` strings — as the same mapping.

    IMPORTED rather than parsed out of the file, unlike the workflows: `ci/smoke.py`
    is python, so the list is reachable as a value and a rename of the constant
    arrives here as an ImportError instead of as a pattern that quietly matches
    nothing. It imports only the standard library and does its work under
    `if __name__ == "__main__"`, so reading it costs this suite nothing.
    """
    return dict(entry.split("=", 1) for entry in SMOKE_ENV if "=" in entry)


def _assert_placeholders_clear_the_floor(source, assignments):
    missing = [name for name in REQUIRED_EVERYWHERE if name not in assignments]
    assert not missing, (
        "{}: found no assignment for {} — this file located the placeholders by "
        "pattern, and a pattern that stops matching would otherwise pass every "
        "check below by having nothing to check".format(source, missing))
    for name in CREDENTIAL_VARS:
        if name not in assignments:
            continue
        value = assignments[name]
        assert len(value) >= MIN_SECRET_LENGTH, (
            "{}: the placeholder for {} is {} characters, below the "
            "MIN_SECRET_LENGTH of {} that src/settings.py enforces on it. The "
            "container CI starts with it will exit(1) at startup — lengthen the "
            "placeholder in the same commit as the constant.".format(
                source, name, len(value), MIN_SECRET_LENGTH))


@pytest.mark.parametrize("relative_path", WORKFLOWS)
def test_the_workflow_placeholders_clear_the_redaction_floor(relative_path):
    _assert_placeholders_clear_the_floor(relative_path, _from_workflow(relative_path))


def test_the_smoke_gate_placeholders_clear_the_redaction_floor():
    # A placeholder under the floor would make the gate's long-lived container exit
    # at once and fail every probe without naming a length.
    _assert_placeholders_clear_the_floor("ci/smoke.py SMOKE_ENV", _from_smoke_env())
