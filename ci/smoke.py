"""Smoke gate for the token_checker image, run on the CI runner against a built image.

It sits BETWEEN `docker build` and `docker push` and is the last point at which a broken
image can still be stopped: the `tokenchecker` container in the `crypt-common` stack on
nebula carries `io.portainer.update.enable`, so what lands on `:latest` is what production
ends up running, and nobody presses a button in between.

It is a plain `python3 ci/smoke.py` on the runner that drives `docker` against the tag it is
given in $SMOKE_IMAGE. Nothing here is imported from the application and nothing here needs
the application's dependencies — the whole gate is `docker run`, `docker exec`,
`docker inspect` and `docker logs`.

What this gate is for, and what it is NOT for
----------------------------------------------
The pytest suite runs in its own job before the image is built (`build` needs `test`), and
it answers "does the code work" against a checkout with every collaborator mocked. What it
cannot answer is whether the ARTEFACT carries that code, comes up on its own, and is
packaged the way this repo says it is. Every check below is one of those:

* (a) every python file the image ships READS and COMPILES under the image's own
  interpreter. The suite runs on whatever python the test job has; this is the line that
  goes red the day the code stops parsing on the version production actually runs;
* (b) every THIRD-PARTY module those files import resolves inside the image, one at a time
  and named, so a dropped requirement names itself instead of arriving as a stack trace out
  of a program that got half-way through its first Grist call. The list is also compared
  against what the image's own files import, so it cannot quietly stop covering a new one;
* (c) every FIRST-PARTY module imports inside the image. `py_compile` cannot see a package
  that was not COPYed, an `__init__.py` left behind, or an import that only resolves from
  the repository root — importing can, and after a layout refactor that is exactly the
  breakage worth fearing;
* (d) PID 1 really is `python main.py`, running as a non-root uid. Since the CMD stopped
  being a `while true` wrapper, the first half is also the liveness statement: a container
  that is up IS the checker, not a shell that outlives it. The second half is the only
  evidence that entrypoint.sh still drops privileges — nothing else in the pipeline would
  notice an image that quietly went back to running as root;
* (e) `.dockerignore` did its job: no tests, no ci, no `.env`, no `.venv` in the image;
* (f) the program FAILS LOUDLY on missing configuration — a non-zero exit AND a message
  naming what is missing. That is the specific defect this repository was fixed for, and
  the old `while true` CMD is exactly what used to hide it;
* (g) the /health endpoint answers `healthy` from inside the container;
* (h) DOCKER's own health verdict reaches `healthy`. This is a DIFFERENT check from (g) and
  the difference is the whole point: (g) speaks python to a socket, while the Dockerfile's
  HEALTHCHECK shells out to `curl`. Drop `curl` from the image — a move to a `-slim` base,
  a Debian change — and the endpoint answers perfectly while the probe fails forever. Our
  Portainer build's auto-heal then restarts a healthy container every ~40 s, and auto-update
  rolls back every good image for never reaching `healthy` in its window, with every other
  check in this file still green.

Constraints of this runner, which shaped every choice below
------------------------------------------------------------
**This job and the docker daemon are not in the same network namespace.** Gitea's act_runner
executes the job inside its own job container while the `docker` CLI it provides drives a
daemon that lives outside it. So NO PORT IS PUBLISHED by anything in this file and nothing
here talks to 127.0.0.1: a published port would land in the HOST daemon's namespace, not in
this job's. Everything that has to be observed from inside a container is observed with
`docker exec` — including check (g), whose HTTP request is made by a python one-liner
running INSIDE the container, against that container's own loopback.

Two properties matter and are easy to lose, so they are stated where they can be checked:

* Failures leave through SystemExit, never `assert`. Asserts vanish under PYTHONOPTIMIZE=1,
  which would silently turn this gate permanently green.
* Every check runs before anything is reported, so one run shows the full extent of the
  breakage instead of only the first broken thing. A check that CANNOT run reports itself as
  FAILED; it is never quietly skipped, which is the classic way a gate keeps reporting
  success while proving less and less.
"""

import json
import os
import subprocess
import sys
import time
import traceback

# The tag to test. Required rather than defaulted: a default would let a mistyped `env:`
# block in a workflow silently gate some other image that happens to be on the daemon — and
# a gate that grades the wrong artefact is worse than no gate, because it is green.
IMAGE_ENV = "SMOKE_IMAGE"
# Base name for every container this gate starts. Required for the same reason plus one
# more: the runner has a single docker daemon shared by every repository, so two concurrent
# runs must not collide on a container name. The workflows put the run id in it.
NAME_ENV = "SMOKE_NAME"

# The two containers this gate starts, by suffix on $SMOKE_NAME:
#   ""      the long-lived one, started with the image's REAL command and placeholder
#           configuration. Checks (a) through (e), (g) and (h) all look at it.
#   -guard  the short-lived one started with NO environment at all, for check (f).
# Both are NAMED rather than left to docker's random name generator, and the reason is the
# one case that matters: `subprocess` hitting its timeout kills the docker CLIENT on the
# runner, not the container on the daemon. With no name nobody could ever remove the
# survivor — not the `finally` here, not the workflow's `if: always()` step — and it would
# go on pinning the image, so the `docker rmi` at the end of the job would fail too.
# Kept in step with the suffix list in both workflows' cleanup steps.
GUARD_SUFFIX = "-guard"

# WORKDIR in the Dockerfile, and the only place this gate looks. Hardcoded rather than read
# back out of the image on purpose: /app is part of the contract between this repo and its
# image, so a Dockerfile that quietly moves WORKDIR has to go red here and be looked at, not
# be politely followed.
APP_DIR = "/app"

# Every python file the image is expected to ship, relative to /app. LISTED rather than
# globbed: a file that stops being COPYed has to FAIL here, and a glob would simply find one
# file fewer and report nothing at all. Keep in step with the COPY lines in the Dockerfile.
ENTRY_SCRIPT = "main.py"
SHIPPED_FILES = (
    ENTRY_SCRIPT,
    "src/__init__.py",
    "src/settings.py",
    "src/config_errors.py",
    "src/redact.py",
    "src/http_timeout.py",
    "src/grist.py",
    "src/health.py",
    "src/watchdog.py",
    "src/balances.py",
    "src/checker.py",
)

# The importable form of the same set. `main` is included on purpose: it is the module the
# CMD executes, and importing it proves its `from src.checker import run` resolves from
# /app. `src.__init__` is not here — it is imported as part of every `src.*` below.
FIRST_PARTY_MODULES = (
    "main",
    "src.settings",
    "src.config_errors",
    "src.redact",
    "src.http_timeout",
    "src.grist",
    "src.health",
    "src.watchdog",
    "src.balances",
    "src.checker",
)

# The distributions those files import. Declared here rather than derived, so the row count
# is a property of this file — but NOT trusted: the sweep inside the image derives the same
# list from the shipped sources' own syntax trees and one row compares the two. A new
# third-party import that nobody added here therefore fails the gate instead of going
# ungated, which is the failure mode a hand-maintained list normally has.
# Deliberately derived from the CODE and not from requirements.txt: that file pins the
# transitive closure too (certifi, idna, urllib3 ...), and gating on pins would check
# packages this program never loads while missing the one it does.
THIRD_PARTY_MODULES = (
    "colorama",
    "grist_api",
    "pydantic",
    "pydantic_settings",
    "requests",
)

# The uid the entrypoint is supposed to drop to (`useradd -m -u 1000 app` in the Dockerfile).
# Fixed on purpose so volume ownership does not drift between rebuilds.
APP_UID = 1000

# Paths that must NOT be inside the image, all four excluded by .dockerignore. `.env` is the
# one that matters most and it is not about size: a `.env` baked into an image that gets
# PUSHED to the registry is a credential leak that leaves no trace whatsoever — the
# container behaves identically and the only way anybody finds out is by unpacking the
# published layers by hand. The Dockerfile also copies its files one by one today, which is
# exactly why this check is worth keeping: widening that to `COPY . .` is a one-line change
# that looks tidier in review, and .dockerignore is then the only thing left.
EXCLUDED_PATHS = ["/app/tests", "/app/ci", "/app/.env", "/app/.venv"]

# The four variables the crypt-common stack supplies, and the ones check (f) strips. Named
# here so that check can require the failure message to name them: the exit code alone is
# worth very little, since an ImportError exits non-zero too.
CONFIG_VARS = ("GRIST_SERVER", "GRIST_DOC_ID", "GRIST_API_KEY", "ETHERSCAN_API_KEY")

# src/config_errors.py's OWN wording. Matching on it is safe because this repository writes
# it, unlike a pydantic message which pydantic is free to reword.
GUARD_FRAGMENT = "Missing required variable(s)"

# PLACEHOLDER configuration for the long-lived container. Obviously fake and deliberately
# unroutable: `.invalid` is reserved by RFC 2606 and can never resolve, so the loop's first
# Grist call is refused locally and Etherscan is never reached because no wallet list ever
# arrives. A gate that reddens when somebody else's service has an outage is a gate everybody
# learns to ignore, and a gate that needs real keys cannot run on a pull request at all.
# It has to be passed at all because the CMD is a direct `python main.py`: with a bare
# environment the container exits within a second, which is the documented behaviour and
# precisely what makes `docker exec` impossible. Check (f) builds the bare environment itself.
SMOKE_ENV = [
    "GRIST_SERVER=http://grist.smoke.invalid",
    "GRIST_DOC_ID=smoke-not-a-real-doc-id",
    "GRIST_API_KEY=smoke-not-a-real-api-key",
    "ETHERSCAN_API_KEY=smoke-not-a-real-api-key",
]

# HEALTH_PORT is deliberately NOT passed above, so this gate exercises the 8080 default in
# src/settings.py — which is what production runs on, because the stack does not pass it
# either, and it is the port the Dockerfile's HEALTHCHECK curls.
HEALTH_URL = "http://127.0.0.1:8080/health"
HEALTH_STATUS = "healthy"

# How many verdicts each probe below is REQUIRED to return, compared against what it
# actually returned before anything is reported. Every probe returns exactly one row per
# target on every path it can take — including the paths where the container could not be
# started at all, which report every target as failed rather than returning nothing. So
# these numbers are a property of the SOURCE, not of a particular run, and any run that
# disagrees with them is a run in which a check went missing.
#
# What this defends against is this gate's own worst failure mode, and it is the one thing
# no other check in the pipeline can catch: a probe that quietly stops probing. A check
# dropped in a refactor, an early return that forgets to emit its rows, an `if ...: return []`
# left behind after debugging — none of those print anything, none contribute a failure, and
# the run still ends on `smoke ok: N/N targets`, because N is counted from the rows that
# happened to arrive and therefore agrees with itself no matter how few there are.
#
# THE ONE WAY TO MISUSE THIS: when a mismatch fires, do NOT edit the number to match what
# the run produced. The number is the claim; the run is the evidence that the claim has
# become false. "Fixing" it that way turns the tripwire into a rubber stamp forever. The fix
# is to work out WHICH probe stopped returning a verdict and why.
#
# MAINTENANCE CONTRACT: adding or removing a check means editing this constant IN THE SAME
# COMMIT as the probe. Counts are derived from the source wherever a derivation exists, so
# growing SHIPPED_FILES or EXCLUDED_PATHS cannot make them stale.
EXPECTED_TARGETS = (
    ("(a) shipped files compile", len(SHIPPED_FILES)),
    ("(b) third-party imports", 1 + len(THIRD_PARTY_MODULES)),
    ("(c) first-party imports", len(FIRST_PARTY_MODULES)),
    ("(d) PID 1", 2),
    ("(e) excluded paths", len(EXCLUDED_PATHS)),
    ("(f) missing-configuration guard", 3),
    ("(g) health endpoint", 1),
    ("(h) docker HEALTHCHECK verdict", 1),
)

# Bounds. Every docker call gets one, because a gate that hangs is worse than a gate that
# fails: it holds a slot on a runner the whole fleet queues for until the step timeout kills
# it, and a killed step never runs its own cleanup. Their worst-case SUM is what the smoke
# step's `timeout-minutes` in both workflows has to exceed, in the order the calls happen:
#     30 (rm guard)   +  90 (guard run, no environment)
#   + 30 (rm probe)   +  60 (probe run -d, the image's own command)
#   + 60 (the compile/import sweep, one exec over every shipped file)
#   + 5 * 30 (one import exec per third-party module)
#   + 10 * 30 (one import exec per first-party module)
#   + 30 (exec: /proc/1/cmdline)  + 30 (exec: /proc/1/status)
#   + 30 (exec: excluded-path sweep)
#   + 60 (exec: the health probe, which retries inside the container)
#   + 120 (docker health: a 60 s poll, a straggling inspect that overruns it, and the
#          one final inspect that fetches the probe's own log for the failure message)
#   + 30 (docker logs: the probe container's own output, taken before it is removed)
#   + 30 (rm probe, finally) + 30 (rm guard, finally)
#   = 1080 s, eighteen minutes. Both workflows allow 20.
# Two of these `rm`s are PRE-run cleanups: each container is removed by name before it is
# started, so a re-run from the Gitea UI — which keeps the same run id, hence the same
# $SMOKE_NAME — cannot die on "name already in use".
INSPECT_TIMEOUT = 30
REMOVE_TIMEOUT = 30
LOGS_TIMEOUT = 30
GUARD_TIMEOUT = 90
START_TIMEOUT = 60
EXEC_TIMEOUT = 30
SWEEP_TIMEOUT = 60
HEALTH_EXEC_TIMEOUT = 60

# The health probe's own budget INSIDE the container, and the docker-health poll's budget on
# the runner. The first is generous next to reality — the health thread is listening a few
# hundred milliseconds into run(), because building the Grist client performs no network I/O.
# The second comes from the healthcheck's own parameters rather than from a taste in round
# numbers: `--start-period=10s` plus `--retries=3` at `--interval=10s` means a container that
# does come up can take 10 + 3*10 = 40 s before its status settles, so 60 s is half again as
# much slack, which a shared runner under load needs.
HEALTH_BUDGET = 30
DOCKER_HEALTH_BUDGET = 60
DOCKER_HEALTH_PAUSE = 2

# How much of an unexpected output reaches the log. Container logs can run to thousands of
# lines when something loops, and an unbounded dump would bury the verdict.
EXCERPT_CHARS = 4000

# Exit codes. A broken check and a gate that could not start are different problems for
# whoever reads the run, so they are different codes.
EXIT_CHECKS_FAILED = 1
EXIT_MISCONFIGURED = 2
# A third code, for the EXPECTED_TARGETS self-check finding that a probe returned fewer (or
# more) verdicts than it declares — and for an unhandled exception, which is the same
# statement: a gate that crashed did not grade the image either. Deliberately NOT folded
# into EXIT_CHECKS_FAILED, because the two demand opposite reactions. A 1 means "this image
# is broken, do not ship it"; a 3 means "this gate has been lying, find out since when", and
# that question is about every image that already went out while a check was not running.
EXIT_SELF_CHECK = 3

# Fed to the image's own python, with the shipped paths as positional arguments. It answers
# three questions in ONE exec — can each file be read, does it compile under THIS
# interpreter, and what third-party names do they all import between them — because they
# share the same file reads and splitting them would triple the docker round-trips for
# nothing. Output is one line per verdict, parsed on the runner.
# It is pure stdlib on purpose: the only third-party imports this gate ever performs are the
# ones it is GATING, one at a time and by name further down. Anything imported here would be
# a library that fails at the gate instead of at the thing being gated.
SWEEP_SCRIPT = r'''
import ast, os, py_compile, sys, sysconfig, tempfile

APP_DIR = "/app"

def stdlib_names():
    # Three sources, because none is sufficient alone: builtin_module_names covers what is
    # compiled into the interpreter and appears as no file anywhere; stdlib_module_names is
    # authoritative but only exists from 3.10; listing the stdlib directory is the fallback
    # that carries an older base. Third-party packages live in site-packages, which shows up
    # as a single harmless entry.
    names = set(sys.builtin_module_names)
    names |= set(getattr(sys, "stdlib_module_names", ()))
    stdlib_dir = sysconfig.get_paths().get("stdlib")
    if stdlib_dir:
        for directory in (stdlib_dir, os.path.join(stdlib_dir, "lib-dynload")):
            try:
                entries = os.listdir(directory)
            except OSError:
                continue
            for entry in entries:
                name = entry.split(".")[0]
                if name:
                    names.add(name)
    return names

paths = sys.argv[1:]
first_party = set(path.split("/")[0].replace(".py", "") for path in paths)
imported = set()
print("interpreter " + ".".join(str(part) for part in sys.version_info[:3]))
for rel in paths:
    path = os.path.join(APP_DIR, rel)
    try:
        with open(path, "rb") as handle:
            raw = handle.read()
    except OSError as error:
        print("file %s FAIL cannot read %s: %s -- either the image never received it (a "
              "COPY naming a path that no longer exists) or it keeps it somewhere other "
              "than %s" % (rel, path, error, APP_DIR))
        continue
    # An empty __init__.py is normal and is what makes the package a package; an empty
    # anything-else means a truncated copy.
    if not raw and not rel.endswith("__init__.py"):
        print("file %s FAIL %s is 0 bytes" % (rel, path))
        continue
    try:
        source = raw.decode("utf-8")
    except UnicodeDecodeError as error:
        print("file %s FAIL %s is not valid UTF-8: %s" % (rel, path, error))
        continue
    try:
        # The .pyc goes to a temp directory instead of __pycache__ next to the source, so
        # this leaves the image's filesystem exactly as it found it. Without `doraise`
        # py_compile prints the error and returns None, and this check would pass on a file
        # that does not parse -- the exact silent-green failure this gate exists to avoid.
        with tempfile.TemporaryDirectory() as workdir:
            py_compile.compile(path, cfile=os.path.join(workdir, "smoke.pyc"), doraise=True)
    except Exception as error:
        print("file %s FAIL does not compile under python %s: %s: %s" % (
            rel, ".".join(str(p) for p in sys.version_info[:3]),
            type(error).__name__, str(error).replace("\n", " ")))
        continue
    try:
        tree = ast.parse(source, filename=path)
    except SyntaxError as error:
        print("file %s FAIL does not parse: %s" % (rel, str(error).replace("\n", " ")))
        continue
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                # `import a.b.c` -> `a`: the top-level name is the one that has to be
                # installed, and importing it is what proves the distribution is there.
                imported.add(alias.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom):
            # `from . import x` is same-project by definition and cannot be a missing
            # requirement.
            if node.level:
                continue
            if node.module:
                imported.add(node.module.split(".")[0])
    print("file %s OK" % rel)
known = stdlib_names() | first_party
for name in sorted(n for n in imported if n not in known):
    print("thirdparty " + name)
'''

# Fed to `sh -c` inside the container to answer "which of these paths exist". Written as a
# script plus positional arguments rather than as an interpolated string: subprocess passes
# argv straight to exec with no shell on the runner side, so `sh -c SCRIPT sh path1 path2`
# puts the paths in "$@" untouched — nothing here can be broken by a path containing a
# space, a quote or a `$`.
PRESENCE_SCRIPT = (
    'for p in "$@"; do '
    'if [ -e "$p" ]; then echo "present $p"; else echo "absent $p"; fi; '
    'done'
)

# Fed to the image's own python INSIDE the container: the runner cannot reach the
# container's loopback (see the module docstring), and this is the endpoint production
# watches. It retries in-process rather than making the runner re-exec, because a docker
# round-trip costs more than the whole wait normally does.
HEALTH_SCRIPT = r'''
import json, sys, time, urllib.error, urllib.request

URL = "%s"
WANT = "%s"
DEADLINE = time.time() + %d

reason = "never attempted"
while True:
    try:
        with urllib.request.urlopen(URL, timeout=2) as response:
            status = response.status
            body = response.read()
        if status != 200:
            reason = "HTTP %%s (expected 200), body %%r" %% (status, body[:200])
        else:
            try:
                payload = json.loads(body.decode("utf-8"))
            except Exception as error:
                reason = "200 but the body is not JSON (%%s): %%r" %% (error, body[:200])
            else:
                if not isinstance(payload, dict):
                    reason = "200 but the body is JSON and not an object: %%r" %% (payload,)
                elif payload.get("status") != WANT:
                    # A 503 with status=unhealthy is an ANSWER, not a transport problem, and
                    # it cannot legitimately happen in this container: nothing gets as far as
                    # a balance check with an unroutable Grist, and the watchdog is reset on
                    # every turn of the loop. So it is a real finding and is reported as one.
                    reason = "200 but status is %%r, expected %%r" %% (
                        payload.get("status"), WANT)
                else:
                    print("health ok")
                    sys.exit(0)
    except urllib.error.HTTPError as error:
        reason = "HTTP %%s (%%r)" %% (error.code, error.read()[:200])
    except Exception as error:
        # Connection refused, timeout, a half-closed socket. Early on this is simply "the
        # server has not started yet", which is why this retries instead of failing here.
        reason = "%%s: %%s" %% (type(error).__name__, error)
    if time.time() >= DEADLINE:
        break
    time.sleep(0.5)
# The LAST reason, not the first: the first is almost always "connection refused" from the
# moment before the server was up, which describes nothing.
print("health fail " + reason)
sys.exit(1)
''' % (HEALTH_URL, HEALTH_STATUS, HEALTH_BUDGET)


def excerpt(text):
    """Bound what reaches the log, and say so when something was cut."""
    if text is None:
        return ""
    if isinstance(text, bytes):
        text = text.decode("utf-8", "replace")
    if len(text) <= EXCERPT_CHARS:
        return text
    return text[:EXCERPT_CHARS] + "\n[... truncated at {} characters]".format(EXCERPT_CHARS)


def describe(error):
    """Type and message of an exception, for a report row."""
    return "{}: {}".format(type(error).__name__, error)


def docker(args, timeout):
    """Run a docker command.

    Returns (status, output) with stderr folded into stdout, because everything here is read
    by a human out of a CI log where the interleaving is the useful part.

    A status of None means the command produced NO EXIT CODE AT ALL — it ran out its
    timeout, or docker is not on PATH — and `output` then explains which. That is
    deliberately not the same thing as a non-zero exit and is never reported with the same
    wording: "the container exited 1" is a finding about the image, while "the docker client
    never came back" is a finding about the runner, and folding them together is how a broken
    runner starts looking like a broken application.
    """
    argv = ["docker"] + args
    try:
        completed = subprocess.run(
            argv,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=timeout,
            text=True)
    except FileNotFoundError:
        return None, (
            "`docker` is not on PATH. This gate drives the daemon from the runner, so it "
            "cannot run anywhere the docker CLI is missing")
    except subprocess.TimeoutExpired as error:
        return None, "`{}` did not finish within {} s. Output so far:\n{}".format(
            " ".join(argv), timeout, excerpt(error.output))
    return completed.returncode, completed.stdout or ""


def remove_container(name):
    """Remove one of this gate's containers by name, together with its anonymous volumes.

    Best effort: the result is deliberately not inspected and this is never the reason a
    check fails. A container that is already gone, or a docker that is momentarily unhappy,
    must not turn into a red verdict about the image.

    `-v` is here even though this Dockerfile declares no `VOLUME` yet, and this is the
    removal path where it matters MOST rather than a copy of the flag the workflows carry:
    on every normal run it is THIS function that removes the long-lived container, so the
    workflow's `if: always()` step finds nothing left and its own `-v` applies to nothing.
    That step exists for the run where this process was killed by the smoke step's
    `timeout-minutes` and never reached its `finally`. Both paths carry the flag; change one
    and change the other. It cannot take anything else with it: `docker rm -v` removes only
    the anonymous volumes belonging to that one container, never a named volume such as the
    ones docker-compose creates.
    """
    docker(["rm", "-f", "-v", name], REMOVE_TIMEOUT)


def environment_flags():
    """SMOKE_ENV as `-e VAR=value` pairs for a `docker run` argument list."""
    flags = []
    for pair in SMOKE_ENV:
        flags.extend(["-e", pair])
    return flags


def parse_sweep(output):
    """Turn SWEEP_SCRIPT's lines into (interpreter, {path: reason-or-None}, [third-party])."""
    interpreter = "?"
    files = {}
    third_party = []
    for line in output.splitlines():
        fields = line.split(None, 2)
        if not fields:
            continue
        if fields[0] == "interpreter" and len(fields) >= 2:
            interpreter = fields[1]
        elif fields[0] == "file" and len(fields) >= 2:
            if len(fields) >= 3 and fields[2] == "OK":
                files[fields[1]] = None
            elif len(fields) >= 3 and fields[2].startswith("FAIL "):
                files[fields[1]] = fields[2][len("FAIL "):]
            else:
                files[fields[1]] = "the sweep reported {!r}, which is neither OK nor a " \
                                   "FAIL reason".format(line)
        elif fields[0] == "thirdparty" and len(fields) >= 2:
            third_party.append(fields[1])
    return interpreter, files, third_party


def check_shipped_files(name, blocked=None):
    """(a) Every file the image ships reads and COMPILES under the image's interpreter.

    One `docker exec` answers all of them, so a failure to run it fails every row rather
    than silently covering fewer files than it claims. The sweep also derives the
    third-party import list that check (b) is graded against, which is why this function
    returns it alongside the rows.
    """
    targets = ["read and py_compile {}/{} under the image's own python".format(APP_DIR, rel)
               for rel in SHIPPED_FILES]

    if blocked is not None:
        return [(target, blocked) for target in targets], None

    status, output = docker(
        ["exec", name, "python", "-c", SWEEP_SCRIPT] + list(SHIPPED_FILES), SWEEP_TIMEOUT)
    if status is None:
        return [(target, "not attempted: " + output) for target in targets], None
    if status != 0 and "interpreter " not in output:
        # A non-zero exit with no output of its own means the sweep never ran (no python at
        # /usr/local/bin/python, an exec refused). A non-zero exit WITH output is normal —
        # nothing in the script exits non-zero, but a stray warning on stderr must not throw
        # away verdicts that were printed.
        reason = "the sweep could not be run (docker exec exited {}):\n{}".format(
            status, excerpt(output))
        return [(target, reason) for target in targets], None

    interpreter, files, third_party = parse_sweep(output)
    rows = []
    for rel, target in zip(SHIPPED_FILES, targets):
        labelled = "read and py_compile {}/{} under python {}".format(APP_DIR, rel, interpreter)
        if rel not in files:
            # The sweep ran but said nothing about this file, which is NOT the same as "it is
            # fine" and must not be reported as a pass.
            rows.append((labelled, (
                "the sweep returned no verdict for this file. Full output:\n{}".format(
                    excerpt(output)))))
        else:
            rows.append((labelled, files[rel]))
    return rows, third_party


def check_third_party_imports(name, declared_by_image, blocked=None):
    """(b) Every third-party module resolves inside the image, one row each.

    The first row is the list itself: THIRD_PARTY_MODULES against what the image's own files
    import. Without it this whole check would only ever cover the names somebody remembered
    to add here, and a new dependency would be gated by nothing at all while the gate stayed
    green.
    """
    list_target = "THIRD_PARTY_MODULES matches what the shipped files import"
    import_targets = ["import {} inside the image".format(module)
                      for module in THIRD_PARTY_MODULES]

    if blocked is not None:
        return [(target, blocked) for target in [list_target] + import_targets]

    rows = []
    if declared_by_image is None:
        rows.append((list_target, (
            "not attempted: the sweep in check (a) could not produce the list (see above)")))
    elif not declared_by_image:
        # The one outcome that must never pass quietly: an empty list means every import got
        # classified as standard library or as one of our own files — a bug in the
        # extraction, or a repo that was gutted — and it would turn this check into zero
        # checks while still printing a cheerful `ok` line.
        rows.append((list_target, (
            "the image's files were found to import NO third-party module at all. They "
            "import grist_api, requests, colorama, pydantic and pydantic_settings at the "
            "very least, so this is a broken extraction, not a lean program — and a gate "
            "that silently degrades to no checks is worse than no gate")))
    elif sorted(declared_by_image) != sorted(THIRD_PARTY_MODULES):
        missing = sorted(set(declared_by_image) - set(THIRD_PARTY_MODULES))
        extra = sorted(set(THIRD_PARTY_MODULES) - set(declared_by_image))
        rows.append((list_target, (
            "they disagree. The image's files import {} but this gate declares {}. "
            "Imported and NOT gated: {}. Gated and no longer imported: {}. The first list "
            "is the dangerous one — those modules are being resolved by nothing".format(
                sorted(declared_by_image), sorted(THIRD_PARTY_MODULES),
                missing or "none", extra or "none"))))
    else:
        rows.append((list_target, None))

    for module, target in zip(THIRD_PARTY_MODULES, import_targets):
        rows.append((target, import_reason(name, module)))
    return rows


def import_reason(name, module):
    """None when `module` imports cleanly inside the container, else the reason.

    One `docker exec` per module, deliberately: the log then names the missing requirement
    instead of making somebody read a traceback to find out which import died.
    """
    status, output = docker(
        ["exec", name, "python", "-c", "import {}".format(module)], EXEC_TIMEOUT)
    if status is None:
        return "not attempted: " + output
    if status != 0:
        return "it does not import (exit {}):\n{}".format(status, excerpt(output))
    return None


def check_first_party_imports(name, blocked=None):
    """(c) Every module the image ships IMPORTS from /app, the way the program does.

    Compiling proves the syntax; importing proves the packaging. `py_compile` cannot see an
    `__init__.py` that was left behind, a package that was never COPYed, or an import that
    only resolves from the repository root — and after a layout refactor those are exactly
    the breakages worth fearing.
    """
    targets = ["import {} inside the image".format(module) for module in FIRST_PARTY_MODULES]
    if blocked is not None:
        return [(target, blocked) for target in targets]
    return [(target, import_reason(name, module))
            for module, target in zip(FIRST_PARTY_MODULES, targets)]


def parse_pid1_uid(status_text):
    """Real uid of PID 1, out of the `Uid:` line of /proc/1/status.

    The kernel writes that file, so it is present whatever userland the image ships —
    unlike `ps`, which a slim image does not carry.
    Returns (uid, None) or (None, reason).
    """
    for line in status_text.splitlines():
        if not line.startswith("Uid:"):
            continue
        fields = line.split()
        # `Uid:  <real> <effective> <saved> <fs>`
        if len(fields) < 2:
            return None, "the Uid line of /proc/1/status is {!r}".format(line)
        try:
            return int(fields[1]), None
        except ValueError as error:
            return None, "the Uid line of /proc/1/status does not parse ({}): {!r}".format(
                describe(error), line)
    return None, "/proc/1/status has no Uid line at all:\n{}".format(excerpt(status_text))


def check_pid1(name, blocked=None):
    """(d) PID 1 is `python main.py`, and it is not root.

    Both halves are about the same process and neither can be answered any other way.

    The uid is read from /proc/1/status and NOT from `docker exec ... id -u`, and that is
    the whole trick: `docker exec` does not go through the ENTRYPOINT and does not inherit
    the gosu drop — it runs as the image's configured user, which here is root, because the
    Dockerfile deliberately declares no USER. So an `id -u` in the exec'd shell prints 0 in
    a perfectly healthy container and 0 in a broken one, and is worth nothing. The question
    is about the process the entrypoint exec'd into, which is PID 1.
    """
    cmd_target = "the container's PID 1 is `python {}`".format(ENTRY_SCRIPT)
    uid_target = "PID 1 does not run as root (the entrypoint dropped to uid {})".format(APP_UID)
    targets = (cmd_target, uid_target)

    if blocked is not None:
        return [(target, blocked) for target in targets]

    rows = []

    status, output = docker(["exec", name, "cat", "/proc/1/cmdline"], EXEC_TIMEOUT)
    if status is None:
        rows.append((cmd_target, "not attempted: " + output))
    elif status != 0:
        rows.append((cmd_target, "/proc/1/cmdline could not be read (docker exec exited "
                                 "{}):\n{}".format(status, excerpt(output))))
    else:
        # /proc/1/cmdline is NUL-separated, which is what keeps an argument containing a
        # space from being mistaken for two.
        argv = [part for part in output.split("\0") if part]
        cmdline = " ".join(argv)
        if not argv:
            rows.append((cmd_target, "/proc/1/cmdline is empty"))
        elif "python" not in os.path.basename(argv[0]):
            # Since the CMD stopped being a `while true` shell wrapper, PID 1 is the
            # interpreter itself. A shell here means the wrapper came back — and with it the
            # failure-hiding restart loop it was removed for — or that an --entrypoint was
            # passed.
            rows.append((cmd_target, (
                "PID 1 is {!r}, whose argv[0] is not a python interpreter — the CMD was "
                "overridden, or the failure-hiding `while true` wrapper is back".format(
                    cmdline))))
        elif not any(arg.endswith(ENTRY_SCRIPT) for arg in argv[1:]):
            rows.append((cmd_target, "PID 1 is {!r}, which never names {}".format(
                cmdline, ENTRY_SCRIPT)))
        else:
            rows.append(("{} ({!r})".format(cmd_target, cmdline), None))

    status, output = docker(["exec", name, "cat", "/proc/1/status"], EXEC_TIMEOUT)
    if status is None:
        rows.append((uid_target, "not attempted: " + output))
    elif status != 0:
        rows.append((uid_target, "/proc/1/status could not be read (docker exec exited "
                                 "{}):\n{}".format(status, excerpt(output))))
    else:
        uid, reason = parse_pid1_uid(output)
        if uid is None:
            rows.append((uid_target, reason))
        elif uid == 0:
            rows.append((uid_target, (
                "it runs as uid 0. The entrypoint is no longer dropping privileges, so this "
                "image runs its whole service as root — and nothing else about it looks any "
                "different, which is why this can go unnoticed for years")))
        elif uid != APP_UID:
            rows.append((uid_target, (
                "it runs as uid {}, not {}. The uid is pinned in the Dockerfile because a "
                "named volume keeps the numeric owner across rebuilds: a drifted uid leaves "
                "an existing volume owned by somebody else".format(uid, APP_UID))))
        else:
            rows.append(("{} -> uid {}".format(uid_target, uid), None))

    return rows


def check_excluded_paths(name, blocked=None):
    """(e) `.dockerignore` did its job: the build context left the wrong things behind.

    One `docker exec` answers all of them, so a failure to run it fails every row rather
    than silently covering fewer paths than it claims.
    """
    targets = ["{} is not in the image".format(path) for path in EXCLUDED_PATHS]

    if blocked is not None:
        return [(target, blocked) for target in targets]

    status, output = docker(
        ["exec", name, "sh", "-c", PRESENCE_SCRIPT, "sh"] + EXCLUDED_PATHS, EXEC_TIMEOUT)
    if status is None:
        return [(target, "not attempted: " + output) for target in targets]
    if status != 0:
        reason = "the path sweep could not be run (docker exec exited {}):\n{}".format(
            status, excerpt(output))
        return [(target, reason) for target in targets]

    seen = {}
    for line in output.splitlines():
        fields = line.split(None, 1)
        if len(fields) == 2 and fields[0] in ("present", "absent"):
            seen[fields[1]] = fields[0]

    rows = []
    for path, target in zip(EXCLUDED_PATHS, targets):
        state = seen.get(path)
        if state == "absent":
            rows.append((target, None))
        elif state == "present":
            rows.append((target, (
                "it IS in the image. .dockerignore lists it, so either that file changed or "
                "the Dockerfile's COPY lines were widened — and in the case of .env the "
                "credentials are now inside an artefact that gets pushed to the registry, "
                "where nothing about the running container would ever reveal it")))
        else:
            rows.append((target, (
                "the sweep returned no verdict for this path. Full output:\n{}".format(
                    excerpt(output)))))
    return rows


def check_missing_configuration_guard(image, name):
    """(f) The image refuses to start without its configuration, AND SAYS SO.

    Started with NO environment at all — not one `-e` — which is the shape of the real
    accident: a stack redeployed after somebody dropped a variable out of the compose file.

    Three rows, because the exit code ALONE is worth very little. A typo in an import, a
    wheel that failed to install, a syntax error in a module: every one of those also exits
    non-zero with no environment set, and every one would let this check report success while
    the guard it claims to be testing had quietly stopped existing. So the exit code is one
    row and the two things the message has to contain are two more — the CLASS of the problem
    and the NAMES of the variables.

    With `restart: unless-stopped` in production the container is restarted either way, so
    that message is the ONLY signal separating "somebody dropped a variable" from an image
    that is simply broken.
    """
    exit_target = "the image exits non-zero when started with no environment at all"
    class_target = "...and its output says {!r}".format(GUARD_FRAGMENT)
    variables_target = "...and its output names {}".format("/".join(CONFIG_VARS))
    targets = (exit_target, class_target, variables_target)

    # Pre-run removal: a re-run from the UI keeps the same run id and therefore the same
    # container name, and `--rm` does not help when a previous attempt was killed mid-flight.
    remove_container(name)

    status, output = docker(["run", "--rm", "--name", name, image], GUARD_TIMEOUT)
    if status is None:
        return [(target, "not attempted: " + output) for target in targets]

    rows = []
    if status == 0:
        rows.append((exit_target, (
            "it exited 0. The image came up with no configuration at all, which means the "
            "guard in src/config_errors.py no longer fires — a variable dropped from the "
            "stack would now produce a container that looks alive and checks nothing. "
            "Output:\n{}".format(excerpt(output)))))
    else:
        rows.append(("{} (exit {})".format(exit_target, status), None))

    if GUARD_FRAGMENT in output:
        rows.append((class_target, None))
    else:
        rows.append((class_target, (
            "it does not. The container exited {} without the configuration guard's own "
            "wording, so this failure is indistinguishable from an import error or a "
            "missing wheel — and the exit code above therefore proves nothing about the "
            "guard. Output:\n{}".format(status, excerpt(output)))))

    unnamed = [variable for variable in CONFIG_VARS if variable not in output]
    if not unnamed:
        rows.append((variables_target, None))
    else:
        rows.append((variables_target, (
            "it never names {}. Whoever redeploys the stack is told that something is "
            "missing but not what, which turns a five-second fix into reading the source of "
            "a container that will not start. Output:\n{}".format(
                ", ".join(unnamed), excerpt(output)))))

    return rows


def check_health_endpoint(name, blocked=None):
    """(g) The /health endpoint answers, and answers healthy.

    Asked from INSIDE the container, because the runner cannot reach its loopback and
    nothing here publishes a port. This is the endpoint itself, over a python socket —
    check (h) is the other half, and the two are not interchangeable.
    """
    target = "GET {} inside the container answers 200 + status={!r} (bounded at {} s)".format(
        HEALTH_URL, HEALTH_STATUS, HEALTH_BUDGET)

    if blocked is not None:
        return [(target, blocked)]

    status, output = docker(
        ["exec", name, "python", "-c", HEALTH_SCRIPT], HEALTH_EXEC_TIMEOUT)
    if status is None:
        return [(target, "not attempted: " + output)]
    if status != 0:
        return [(target, (
            "it does not (exit {}). The container is up, so its main loop reached the point "
            "where it starts the health thread — or it did not, which is the same finding. "
            "Probe output:\n{}".format(status, excerpt(output))))]
    return [("{} -> {}".format(target, output.strip()), None)]


def check_docker_health(name, blocked=None):
    """(h) DOCKER's own verdict on the container reaches `healthy`.

    A DIFFERENT check from (g), and the difference is the point: (g) speaks python to a
    socket while the Dockerfile's HEALTHCHECK shells out to `curl`. Lose `curl` — a `-slim`
    base, a Debian change — and the endpoint answers perfectly while the probe fails
    forever. This is the verdict our Portainer build acts on in both directions: auto-heal
    restarts a container docker calls `unhealthy`, and auto-update waits for `healthy` after
    it recreates one before accepting the new image instead of rolling it back.
    """
    target = "docker reports the container healthy within {} s".format(DOCKER_HEALTH_BUDGET)

    if blocked is not None:
        return [(target, blocked)]

    deadline = time.time() + DOCKER_HEALTH_BUDGET
    health = None
    problem = None
    while True:
        status, output = docker(
            ["inspect", "--format", "{{json .State.Health}}", name], INSPECT_TIMEOUT)
        if status is None:
            problem = output
            break
        if status != 0:
            problem = "`docker inspect {}` exited {}:\n{}".format(
                name, status, excerpt(output))
        else:
            problem = None
            raw = output.strip()
            if raw == "null" or raw == "":
                # NOT "unhealthy" — it is "there is no healthcheck at all", i.e. the
                # HEALTHCHECK line has been removed from the Dockerfile. Nothing else in
                # this gate can see that happen, so it gets its own wording, and it is
                # decided immediately rather than after the full budget: a missing probe
                # will not appear later.
                return [(target, (
                    "docker reports NO health state for this container, which means the "
                    "image declares no HEALTHCHECK — the line is gone from the Dockerfile. "
                    "That probe is what our Portainer build's auto-heal and auto-update act "
                    "on: with it gone a wedged tokenchecker would never be restarted again, "
                    "and nothing else here would notice, because check (g) shows the "
                    "endpoint answering perfectly well on its own"))]
            try:
                state = json.loads(raw)
            except ValueError as error:
                problem = "docker inspect returned something that is not JSON ({}): {}".format(
                    describe(error), excerpt(output))
            else:
                health = (state or {}).get("Status")
                if health == "healthy":
                    return [("{} -> {!r}".format(target, health), None)]
        if time.time() >= deadline:
            break
        time.sleep(DOCKER_HEALTH_PAUSE)

    if problem is not None:
        return [(target, "not attempted: " + problem)]
    # The probe's OWN log, which is where a `curl: not found` shows up. In the scenario this
    # check exists for the application is perfectly healthy, so its container log says
    # nothing at all and without this the gate goes red without ever saying why.
    _, probe_log = docker(
        ["inspect", "--format", "{{json .State.Health}}", name], INSPECT_TIMEOUT)
    return [(target, (
        "its verdict was still {!r} after {} s. The endpoint itself answered in check (g), "
        "so the application is up — what is broken is the Dockerfile's HEALTHCHECK, most "
        "likely a `curl` that is no longer in the image. Our Portainer build's auto-heal "
        "would restart this perfectly healthy container roughly every 40 s, and auto-update "
        "would roll every new image back. Health state:\n{}".format(
            health, DOCKER_HEALTH_BUDGET, excerpt(probe_log))))]


def main():
    image = os.environ.get(IMAGE_ENV)
    name = os.environ.get(NAME_ENV)
    missing = [variable for variable, value in ((IMAGE_ENV, image), (NAME_ENV, name))
               if not value]
    if missing:
        # Refused rather than defaulted, and refused BEFORE anything is started. A default
        # tag would gate whatever image happened to be on this shared daemon and report
        # green; a default container name would collide with a concurrent run and report red
        # at random. Both are worse than a step that will not start.
        print("smoke cannot run: {} not set. The workflow has to provide {} (the exact tag "
              "that was just built) and {} (a base container name unique to this run).".format(
                  ", ".join(missing), IMAGE_ENV, NAME_ENV))
        raise SystemExit(EXIT_MISCONFIGURED)

    probe_name = name
    guard_name = name + GUARD_SUFFIX

    # Each probe's rows are kept in a local of their own rather than poured straight into one
    # shared list, purely so the self-check below can still tell them apart: once they are
    # concatenated there is no way to know which probe contributed how many, which is exactly
    # the information needed to name the probe that went quiet.
    guard_rows = check_missing_configuration_guard(image, guard_name)

    # The probe container's own output, fetched below and printed with the failures. Bound
    # here so the `finally` cannot leave it undefined on a path that never got that far.
    probe_log = None

    try:
        # ONE long-lived container, started with the image's REAL command, serves every
        # remaining check. Not an idle `sleep` override: PID 1 has to be what production runs
        # for check (d) to mean anything, and the health server only exists while the real
        # program does.
        remove_container(probe_name)
        start_status, start_output = docker(
            ["run", "-d", "--name", probe_name] + environment_flags() + [image],
            START_TIMEOUT)
        if start_status is None:
            blocked = "not attempted: " + start_output
        elif start_status != 0:
            blocked = (
                "not attempted: the probe container could not be started (docker run exited "
                "{}):\n{}".format(start_status, excerpt(start_output)))
        else:
            blocked = None
        # `blocked` is passed rather than skipping these: a container that never started
        # still has to produce its full set of rows, all of them failed. That is what keeps
        # the counts below meaningful in exactly the case where a gate is most tempted to
        # fall silent.
        shipped_rows, third_party_seen = check_shipped_files(probe_name, blocked=blocked)
        third_party_rows = check_third_party_imports(
            probe_name, third_party_seen, blocked=blocked)
        first_party_rows = check_first_party_imports(probe_name, blocked=blocked)
        pid1_rows = check_pid1(probe_name, blocked=blocked)
        excluded_rows = check_excluded_paths(probe_name, blocked=blocked)
        health_rows = check_health_endpoint(probe_name, blocked=blocked)
        docker_health_rows = check_docker_health(probe_name, blocked=blocked)

        # The APPLICATION's own output, and the fourth of the four docker commands this
        # gate is built from. Taken HERE, before the `finally` below removes the container:
        # `docker logs` against a container that has been removed has nothing left to
        # return, so a few lines later is too late.
        #
        # The case it exists for is a long-lived container that started and then DIED. Every
        # exec-based probe above then reports "docker exec failed", which is true, correctly
        # red, and completely silent about WHY — the traceback that killed the program only
        # ever existed in the container's log, and without this the gate sends whoever reads
        # the run to reproduce it by hand. Skipped when the container never started at all:
        # `docker run` already returned its reason and it is on every row.
        #
        # It contributes NO verdict. It is diagnostic output about rows that already failed,
        # and an "is there a log" check would go red on a perfectly good image that simply
        # has nothing to say.
        if blocked is None:
            log_status, probe_log = docker(["logs", probe_name], LOGS_TIMEOUT)
            if log_status is None:
                probe_log = "the container log could not be fetched: " + probe_log
    finally:
        # Removed whatever happened above. The workflow removes them again under
        # `if: always()` for the case where this process itself was killed by the step
        # timeout and never reached this line.
        remove_container(probe_name)
        remove_container(guard_name)

    # SAME ORDER AS EXPECTED_TARGETS, and that is a requirement rather than a convention: the
    # pairing below is positional, so a group moved here without moving its declaration is
    # compared against somebody else's count. Nothing in this file can detect a swap between
    # two groups that return the same number of verdicts; keeping the two tuples in step by
    # eye is what prevents it, which is why the letters are on the labels.
    produced = (shipped_rows, third_party_rows, first_party_rows, pid1_rows, excluded_rows,
                guard_rows, health_rows, docker_health_rows)

    # Three self-checks, and they are three because each catches a break the others cannot
    # see. They are collected in two lists rather than one because they are REPORTED
    # differently: `wiring` holds free-text findings about this file's own consistency, while
    # `miscounted` holds one entry per disagreeing probe.
    wiring = []

    # (1) SAME NUMBER OF PROBE GROUPS. Its own check, because the per-probe comparison below
    # is structurally incapable of making it: `zip` stops at the shorter of its arguments and
    # says nothing about the surplus. A refactor that DROPS a probe from `produced` — rather
    # than leaving it in place returning [] — pairs the survivors against the first
    # declarations, finds every one consistent, and reports `miscounted == []`.
    # (`zip(..., strict=True)` would say this in one word and is deliberately not used: the
    # runner's python3 is whatever the job image ships and is not pinned anywhere, and
    # `strict` needs 3.10 — on an older interpreter this file would die with a TypeError
    # before it graded anything.)
    arity_agrees = len(produced) == len(EXPECTED_TARGETS)
    if not arity_agrees:
        wiring.append(
            "this gate's own wiring is inconsistent: EXPECTED_TARGETS declares {} probe "
            "group(s) and `produced` in main() carries {}. A probe was added or removed "
            "without updating the constant in the same commit, so the surviving groups are "
            "no longer even paired with the declarations they are compared against".format(
                len(EXPECTED_TARGETS), len(produced)))

    # (2) PER PROBE: which specific probe returned the wrong number of verdicts. Only
    # attempted when the arity agrees — pairing two lists of different lengths positionally
    # would attribute counts to the wrong labels and send the reader after the wrong probe.
    if arity_agrees:
        miscounted = [
            (label, expected, len(actual))
            for (label, expected), actual in zip(EXPECTED_TARGETS, produced)
            if len(actual) != expected
        ]
    else:
        miscounted = []

    rows = []
    for group in produced:
        rows.extend(group)

    # (3) THE TOTAL, against the sum of the declarations. Belt and braces over (1) and (2),
    # and it earns its place on a case neither can see: a group poured into `rows` a SECOND
    # time — a duplicated `rows.extend(...)` while adding a probe — leaves the arity right
    # and every per-probe count right, because both of those inspect `produced` and this
    # mistake happens after it.
    declared_total = sum(count for _, count in EXPECTED_TARGETS)
    if len(rows) != declared_total:
        wiring.append(
            "the verdicts actually collected do not add up to what is declared: "
            "EXPECTED_TARGETS sums to {} and this run concatenated {}. If no probe is named "
            "below, the arithmetic broke between the probes and `rows` — a group extended "
            "into `rows` twice, or one left out of the loop entirely".format(
                declared_total, len(rows)))

    failures = []
    for target, reason in rows:
        if reason is None:
            print("ok   {}".format(target))
        else:
            print("FAIL {} -> {}".format(target, reason))
            failures.append(target)

    # Reported first, and in full even when the self-check below is also going to fire: "the
    # image is broken" and "the gate lost a check" are two independent facts, and a run that
    # shows only one of them sends whoever reads it after half of the problem.
    if failures:
        print("")
        print("smoke FAILED: {}/{} targets broken".format(len(failures), len(rows)))
        for target in failures:
            print("  - {}".format(target))
        # Printed only alongside failures, deliberately. On a green run this log is a page
        # of expected noise — the placeholder configuration points the loop at
        # grist.smoke.invalid, which by RFC 2606 can never resolve — and printing it every
        # time is how it stops being read on the run where it matters.
        if probe_log is not None:
            print("")
            print("Log of the probe container ({}). The image's own words about the run "
                  "above; empty is normal for a container that stayed up and only failed "
                  "checks about its packaging:".format(probe_name))
            print(excerpt(probe_log).rstrip() or "(no output at all)")

    # The self-check comes BEFORE the success line, so `smoke ok` can never be printed by a
    # run that returned fewer verdicts than it promised — which is the whole scenario this
    # exists for, since a shrinking gate reports success by construction.
    if wiring or miscounted:
        print("")
        print("smoke SELF-CHECK FAILED: this gate did not return the verdicts it declares.")
        for problem in wiring:
            print("  - {}".format(problem))
        for label, expected, actual in miscounted:
            print("  - {}: declared {} verdict(s), returned {}".format(label, expected, actual))
        print("")
        print("This is a finding about THIS SCRIPT, not about the image: either a probe "
              "stopped returning one of its verdicts, or a probe was added to (or removed "
              "from) this file without EXPECTED_TARGETS being updated in the same commit. "
              "Either way a check was not performed and nothing above reports on it in "
              "either direction. Work out which check went missing and when — do NOT "
              "reconcile EXPECTED_TARGETS with the number this run produced, which would "
              "make the gate agree with itself forever.")
        raise SystemExit(EXIT_SELF_CHECK)

    if failures:
        raise SystemExit(EXIT_CHECKS_FAILED)

    print("")
    print("smoke ok: {}/{} targets".format(len(rows), len(rows)))


if __name__ == "__main__":
    # Every verdict this gate reaches leaves through SystemExit, and every one of those exit
    # codes still means exactly what it says above: `except Exception` does not catch
    # SystemExit, so 0, 1, 2 and the self-check's 3 all pass through untouched. What is
    # caught is the OTHER way this script can end — an exception nobody planned for. CPython
    # exits 1 on an unhandled traceback, i.e. would report a broken GATE as a broken IMAGE
    # and send whoever reads the run to inspect an artefact that may be perfectly fine.
    #
    # The traceback is NOT swallowed: it is the only diagnostic there is for a failure nobody
    # anticipated. stdout is block-buffered when it is a pipe — which it is on the runner —
    # so it is flushed first, otherwise every row printed by main() would land in the log
    # AFTER the traceback of the call that produced them.
    try:
        main()
    except Exception:
        sys.stdout.flush()
        traceback.print_exc()
        print(
            "\nsmoke CRASHED: the exception above came out of this script, not out of the "
            "image. Nothing here graded the artefact, so this run says nothing about whether "
            "the image is fit to publish — exiting {} (`the gate is broken`) rather than {} "
            "(`the image is broken`), because sending anybody to inspect the image would be "
            "sending them to the wrong place.".format(EXIT_SELF_CHECK, EXIT_CHECKS_FAILED),
            file=sys.stderr)
        raise SystemExit(EXIT_SELF_CHECK)
