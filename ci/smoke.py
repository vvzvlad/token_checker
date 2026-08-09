"""Smoke test for the token_checker image, run against a live container.

It is fed to the container over stdin (`docker exec -i <name> python -u - < ci/smoke.py`),
so it never has to be copied into the image, and it can be run by hand the same way
against any locally started container of this image.

It is pure stdlib on purpose: the only third-party imports it ever performs are the ones
it is GATING, one at a time and by name. Anything else imported up here would be a library
that fails at the gate instead of at the thing being gated.

What this gate can be, and what it cannot
-----------------------------------------
There is no test suite in this repo. A liveness check would be worthless, because the
image's command is

    CMD while true; do python token_checker.py; sleep 10; done

so the container stays "running" whether the script works, dies on its first line, or was
never copied into the image at all. "It did not exit" proves exactly nothing here and must
never be mistaken for a gate — least of all in this repo, where the Dockerfile's own
HEALTHCHECK is the thing production watches and it only ever answers for a script that got
all the way into its main loop.

So the checks below are the ones that are genuinely decidable without production
credentials:

* the script COMPILES under the image's own interpreter — the check that catches a base
  image drifting to a Python version the code no longer parses under, which is a class of
  rot that has already bitten this fleet once;
* every third-party module the script imports RESOLVES inside the image, reported one
  module at a time, so a dropped requirement names itself instead of arriving as a stack
  trace out of a program that got half-way through its first Grist call;
* the image's command REALLY RUNS this script — not "the CMD string mentions it", but a
  live `python token_checker.py` process caught in the container's own process table;
* the HEALTH ENDPOINT answers. The container this runs against is started with dummy but
  non-empty values for the four required variables, which is what gets the script past its
  own missing-environment guard and into `main()`'s body. That is safe precisely because
  constructing the Grist client performs no network I/O — it only builds an object — so
  the health thread is listening before anything in the program has tried to reach
  anywhere.

The fifth check of this gate is not in this file at all: running the image with NO
environment and requiring a non-zero exit needs `docker run`, so it lives in the workflow.

Nothing here talks to Grist, to Etherscan, or to any other external API, and nothing here
needs a credential. That is deliberate on both counts: a gate that reddens when somebody
else's service has an outage is a gate everybody learns to ignore, and a gate that needs
real keys cannot run on a pull request at all. The dummy GRIST_SERVER the workflow passes
points at a `.invalid` host, which is reserved by RFC 2606 and can never resolve, so even
the main loop's first Grist call cannot leave the machine.

Two properties matter and are easy to lose:

* Failures leave through SystemExit, never `assert`. Asserts vanish under PYTHONOPTIMIZE=1
  (a common image tweak), which would silently turn this gate permanently green.
* Every target is checked before anything is reported, so one run shows the full extent of
  the breakage rather than only the first broken thing. A check that CANNOT run — the
  script is unreadable, say — reports itself as FAILED; it is never quietly skipped, which
  is the classic way a gate keeps reporting success while proving less and less.
"""

import ast
import importlib
import json
import os
import py_compile
import sys
import sysconfig
import tempfile
import time
import urllib.error
import urllib.request

# WORKDIR in the Dockerfile, and the only place this script looks. Hardcoded rather than
# read back from the container's own working directory on purpose: /app is part of the
# contract between this repo and its image, so a Dockerfile that quietly moves WORKDIR
# somewhere else has to go red here and be looked at, not be politely followed.
APP_DIR = "/app"
SCRIPT_NAME = "token_checker.py"
SCRIPT_PATH = os.path.join(APP_DIR, SCRIPT_NAME)

# The kernel's process table, as seen from inside the container's own PID namespace: PID 1
# is the image's CMD and everything the CMD spawns is in here too. It is a module constant
# rather than a literal buried in the code so this script can also be exercised against a
# stub tree on a machine that has no /proc at all — the check itself is not weakened by
# that, it simply reads its process list from wherever this points.
PROC_ROOT = "/proc"

# How hard to look for a live `python token_checker.py`. The bound is in ATTEMPTS, and what
# it has to cover is the CMD loop's duty cycle, not a guess at startup time: the loop runs
# the script, then sleeps TEN SECONDS, then runs it again. A poll that happens to start
# during that sleep sees nothing at all for up to ~10 s through no fault of the image.
# 1000 attempts spaced 0.02 s apart is ~20 s of pauses, plus one scan of the process table
# per attempt (a few milliseconds on a container running four or five processes) — call it
# 20-30 s of wall clock in the worst case, which covers two full cycles.
# The interval is fine-grained for the OPPOSITE failure: a script that crashes early is
# only alive for the length of an interpreter start plus its imports, a few tenths of a
# second per cycle. At 0.02 s that window still yields a dozen or more chances to be seen;
# at the 1 s interval used by the fleet's web gates it would be a coin toss, and a
# coin-toss gate is worse than none. Under the dummy environment this container runs with,
# the script stays alive continuously and is normally caught on the first attempt — the
# budget is for the failing case, which is the only one that spends it.
PROC_ATTEMPTS = 1000
PROC_PAUSE = 0.02

# `main()` reads HEALTH_PORT and falls back to 8080. The workflow deliberately does NOT set
# it, so this gate exercises the default — which is also the port the Dockerfile's own
# HEALTHCHECK curls, and the one production is watching.
HEALTH_URL = "http://127.0.0.1:8080/health"

# What `HealthCheckHandler.do_GET` writes for a healthy service. Only `status` is asserted;
# the handler sends nothing else on the 200 path, and pinning the whole body would turn a
# harmless addition into a red gate.
HEALTH_STATUS = "healthy"

# Bounded, and honestly: 60 attempts with a 0.5 s pause and a 2 s socket timeout.
#   * port refused outright (the server has not started yet, or never will): the connect
#     fails instantly, so an attempt costs only the pause — ~30 s over the full run;
#   * something accepts the connection and then never answers: each attempt also burns the
#     2 s timeout — ~2 min 30 s over the full run.
# In practice the endpoint answers on the first or second attempt: the health thread starts
# a few hundred milliseconds into `main()`, right after the Grist client is constructed,
# and that construction performs no network I/O. The budget exists for the failure, and it
# has to fit inside the smoke step's 5-minute timeout ALONGSIDE the process poll above —
# ~2 min 30 s plus ~30 s does.
HEALTH_ATTEMPTS = 60
HEALTH_PAUSE = 0.5
HEALTH_TIMEOUT = 2

# How much of an unexpected body reaches the log. A container that is not this application
# can answer with an HTML error page, and an unbounded dump of it would bury the verdict.
EXCERPT_BYTES = 200


def python_version():
    """The interpreter this actually ran under — the image's, not the runner's."""
    return ".".join(str(part) for part in sys.version_info[:3])


def read_source():
    """Return (source_text, None), or (None, reason) when the script is not readable."""
    try:
        with open(SCRIPT_PATH, "rb") as handle:
            raw = handle.read()
    except OSError as error:
        return None, (
            "cannot read {}: {} — either the image never received the file (a COPY naming "
            "a file that no longer exists) or it keeps it somewhere other than {}".format(
                SCRIPT_PATH, error, APP_DIR))
    if not raw:
        return None, "{} is 0 bytes".format(SCRIPT_PATH)
    try:
        return raw.decode("utf-8"), None
    except UnicodeDecodeError as error:
        return None, "{} is not valid UTF-8: {}".format(SCRIPT_PATH, error)


def compile_check():
    """Byte-compile the script with the image's own interpreter; None when it compiles."""
    try:
        # The .pyc goes to a temp directory instead of the default `__pycache__` next to
        # the source: this check leaves the image's filesystem exactly as it found it, and
        # keeps working if /app is ever mounted read-only.
        with tempfile.TemporaryDirectory() as workdir:
            py_compile.compile(
                SCRIPT_PATH,
                cfile=os.path.join(workdir, "smoke.pyc"),
                # Without `doraise` py_compile prints the error and returns None, and this
                # check would pass on a script that does not parse — the exact silent-green
                # failure the whole file is built to avoid.
                doraise=True)
    except py_compile.PyCompileError as error:
        return "does not compile under python {}: {}".format(python_version(), error)
    except OSError as error:
        return "cannot be compiled, {} is not readable: {}".format(SCRIPT_PATH, error)
    except Exception as error:
        return "cannot be compiled: {}: {}".format(type(error).__name__, error)
    return None


def stdlib_names():
    """Every module name that belongs to THIS interpreter's standard library."""
    # Three sources, because none of them is sufficient on its own across the fleet:
    #   * sys.builtin_module_names covers what is compiled into the interpreter (`sys`,
    #     `time`, ...), which never appears as a file anywhere;
    #   * sys.stdlib_module_names is the authoritative list — and it only exists from
    #     Python 3.10. This image is python:3.13 and has it, but the sibling repo that
    #     shares this gate is still on python:3.9, and keeping the two files comparable is
    #     worth more than the three lines saved by assuming it is there;
    #   * listing the stdlib directory is therefore the fallback: every stdlib module is a
    #     file or a package directory in there, and the C extensions live one level down in
    #     lib-dynload. Third-party packages are NOT in there — pip installs into
    #     site-packages, which shows up as a single harmless entry.
    names = set(sys.builtin_module_names)
    names |= set(getattr(sys, "stdlib_module_names", ()))
    stdlib_dir = sysconfig.get_paths().get("stdlib")
    if stdlib_dir:
        for directory in (stdlib_dir, os.path.join(stdlib_dir, "lib-dynload")):
            try:
                entries = os.listdir(directory)
            except OSError:
                # lib-dynload does not exist on every build; the stdlib directory itself
                # not being listable is odd but not worth failing over, since the two
                # sources above still carry the common names.
                continue
            for entry in entries:
                # `datetime.py` -> datetime, `_json.cpython-313-x86_64-linux-gnu.so` ->
                # _json, a package directory -> its own name.
                name = entry.split(".")[0]
                if name:
                    names.add(name)
    return names


def declared_third_party_imports(source):
    """The top-level names the script imports that are NOT in the standard library.

    Derived from the script's own syntax tree rather than from requirements.txt: the file
    that has to keep working is the script, and requirements.txt pins more than the script
    imports (proxmoxer, PyYAML and ansible-output-parser are in there and appear nowhere in
    the code). Checking the pins instead would gate on packages this program never loads.
    """
    tree = ast.parse(source, filename=SCRIPT_PATH)
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                # `import a.b.c` -> `a`: the top-level name is the one that has to be
                # installed, and importing it is what proves the distribution is there.
                imported.add(alias.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom):
            # `from . import x` — a relative import is same-project by definition and
            # cannot be a missing requirement. There are none today; skipping them keeps
            # a future one from being reported as an absent third-party package.
            if node.level:
                continue
            if node.module:
                imported.add(node.module.split(".")[0])
    known = stdlib_names()
    return sorted(name for name in imported if name not in known)


def import_reason(module):
    """Return None when the module imports cleanly inside the image, else the reason."""
    try:
        importlib.import_module(module)
    except Exception as error:
        # Deliberately broad: a missing distribution raises ImportError, but a wheel built
        # for another platform, a broken C extension or a package whose own import-time
        # code blows up raise something else entirely, and all of them mean the same thing
        # here — this program will not start in this image.
        return "{}: {}".format(type(error).__name__, error)
    return None


def import_rows(source):
    """Return (target, reason) rows: the extraction itself, then one row per module."""
    target = "third-party imports declared by {}".format(SCRIPT_NAME)
    if source is None:
        # Never silently skipped. A check that cannot run is a FAILED check, otherwise the
        # gate quietly shrinks to whatever still happens to work.
        return [(target, "not attempted: {} could not be read (see above)".format(
            SCRIPT_PATH))]
    try:
        modules = declared_third_party_imports(source)
    except SyntaxError as error:
        return [(target, "cannot be extracted, the script does not parse: {}".format(
            error))]
    except Exception as error:
        return [(target, "cannot be extracted: {}: {}".format(
            type(error).__name__, error))]
    if not modules:
        # The one outcome that must never pass quietly. An empty list means every import
        # got classified as standard library — a bug in the extraction above, or a script
        # that was gutted — and it would turn this whole check into zero checks while
        # still printing a cheerful `ok` line.
        return [(target, (
            "the extraction produced an EMPTY list, so this check would have verified "
            "nothing at all; {} imports something, and a gate that silently degrades to "
            "no checks is worse than no gate".format(SCRIPT_NAME)))]
    rows = [("{} -> {}".format(target, ", ".join(modules)), None)]
    for module in modules:
        # One row per module, so the log names the missing requirement instead of making
        # somebody read a traceback to find out which import died.
        rows.append(("import {} inside the image".format(module), import_reason(module)))
    return rows


def process_list():
    """Every process in the container's PID namespace, as (pid, [argv...])."""
    entries = []
    try:
        names = os.listdir(PROC_ROOT)
    except OSError as error:
        raise RuntimeError("cannot list {}: {}".format(PROC_ROOT, error))
    for name in names:
        if not name.isdigit():
            continue
        try:
            with open(os.path.join(PROC_ROOT, name, "cmdline"), "rb") as handle:
                raw = handle.read()
        except OSError:
            # The process exited between the listdir and the open. Entirely normal here —
            # this script is scanning a process table whose whole point is that things
            # come and go in it.
            continue
        argv = [part.decode("utf-8", "replace") for part in raw.split(b"\0") if part]
        if argv:
            entries.append((int(name), argv))
    return entries


def check_cmd_names_script():
    """PID 1 is the image's CMD, and it must be the loop that names this script."""
    target = "the container's PID 1 is the image's CMD and names {}".format(SCRIPT_NAME)
    path = os.path.join(PROC_ROOT, "1", "cmdline")
    try:
        with open(path, "rb") as handle:
            raw = handle.read()
    except OSError as error:
        return target, "cannot read {}: {}".format(path, error)
    cmdline = " ".join(part.decode("utf-8", "replace") for part in raw.split(b"\0") if part)
    if not cmdline:
        return target, "{} is empty".format(path)
    if SCRIPT_NAME not in cmdline:
        # Something else is PID 1: an `--entrypoint` was passed, the CMD was rewritten, or
        # this is simply not the image anybody thought it was.
        return target, "PID 1 is {!r}, which never mentions {}".format(
            cmdline, SCRIPT_NAME)
    # On its own this proves only that the CMD STRING mentions the script — a Dockerfile
    # that COPYs the wrong filename leaves this cmdline untouched. The check below is what
    # turns that into "and it actually runs", and the compile check above is what fails
    # when the file is not at /app.
    return "{} ({!r})".format(target, cmdline), None


def check_script_is_running():
    """Catch the CMD loop actually launching the script, not just naming it."""
    target = "the CMD loop actually runs {} (bounded at {} attempts, ~20-30 s)".format(
        SCRIPT_NAME, PROC_ATTEMPTS)
    # This script is itself a `python` process inside the same container; excluded so it
    # can never satisfy its own check. Its argv (`python -u -`) would not match anyway —
    # belt and braces, because the day somebody runs this file by copying it into the
    # image, the argv WOULD contain the script's name.
    mine = {os.getpid(), os.getppid()}
    for attempt in range(1, PROC_ATTEMPTS + 1):
        try:
            processes = process_list()
        except RuntimeError as error:
            return target, str(error)
        for pid, argv in processes:
            if pid in mine:
                continue
            # PID 1 is the shell running the whole `while true; do ... done` string, so its
            # argv contains the script's name at all times, running or sleeping. Counting
            # it would make this check pass on an image whose python cannot start at all.
            if pid == 1:
                continue
            # argv[0] must be an interpreter: it is `python <script>` that has to be seen,
            # not any process that happens to carry the name (the shell above, an editor,
            # a `grep`).
            if "python" not in os.path.basename(argv[0]):
                continue
            if not any(arg.endswith(SCRIPT_NAME) for arg in argv[1:]):
                continue
            return "{} -> seen as pid {}: {!r} on attempt {}".format(
                target, pid, " ".join(argv), attempt), None
        time.sleep(PROC_PAUSE)
    return target, (
        "no `python {}` process appeared in {} in {} attempts (~20-30 s, which is more "
        "than two full turns of the CMD loop's 10-second sleep) — the container is up, but "
        "its command is not running this script: the CMD was overridden, the interpreter "
        "cannot start, or the file it names is not where the command looks for it".format(
            SCRIPT_NAME, PROC_ROOT, PROC_ATTEMPTS))


def health_reason():
    """One attempt at /health; None when it answers 200 with status=healthy."""
    try:
        with urllib.request.urlopen(HEALTH_URL, timeout=HEALTH_TIMEOUT) as response:
            status = response.status
            body = response.read()
    except urllib.error.HTTPError as error:
        # A 4xx/5xx is an ANSWER, not a transport problem, and one of them is expected
        # enough to name: the handler replies 503 with status=unhealthy while a balance
        # check is in flight or after the watchdog has run down. Neither can happen in this
        # container — nothing gets as far as a balance check with an unroutable Grist, and
        # the watchdog is reset on every turn of the main loop — so a 503 here is a real
        # finding and is reported as one.
        return "HTTP {} ({!r})".format(error.code, error.read()[:EXCERPT_BYTES])
    except Exception as error:
        # Connection refused, timeout, a half-closed socket — anything that kept us from an
        # answer at all. Early on this is simply "the server has not started yet", which is
        # why the caller retries rather than failing on the first one.
        return "{}: {}".format(type(error).__name__, error)
    if status != 200:
        return "HTTP {} (expected 200), body {!r}".format(status, body[:EXCERPT_BYTES])
    try:
        payload = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as error:
        return "200 but the body is not JSON ({}): {!r}".format(
            error, body[:EXCERPT_BYTES])
    if not isinstance(payload, dict):
        return "200 but the body is JSON and not an object: {!r}".format(payload)
    if payload.get("status") != HEALTH_STATUS:
        return "200 but status is {!r}, expected {!r}".format(
            payload.get("status"), HEALTH_STATUS)
    return None


def check_health():
    """The health endpoint production watches must answer, and answer healthy."""
    target = "GET {} (expect 200 + status={!r}, bounded at {} attempts: ~30 s when the " \
             "port is refused outright, up to ~2 min 30 s when something accepts the " \
             "connection and never answers)".format(
                 HEALTH_URL, HEALTH_STATUS, HEALTH_ATTEMPTS)
    reason = None
    for attempt in range(1, HEALTH_ATTEMPTS + 1):
        reason = health_reason()
        if reason is None:
            return "{} -> answered on attempt {}".format(target, attempt), None
        time.sleep(HEALTH_PAUSE)
    # The LAST reason is reported, not the first: the first is almost always "connection
    # refused" from the moment before the server was up, which describes nothing. The last
    # one is the state the endpoint was still in when the budget ran out.
    return target, "no healthy answer in {} attempts; last: {}".format(
        HEALTH_ATTEMPTS, reason)


def main():
    rows = []

    source, reason = read_source()
    rows.append(("read {}".format(SCRIPT_PATH), reason))
    rows.append(("py_compile {} under python {}".format(SCRIPT_PATH, python_version()),
                 compile_check()))
    rows.extend(import_rows(source))
    rows.append(check_cmd_names_script())
    rows.append(check_script_is_running())
    rows.append(check_health())

    failures = []
    for target, reason in rows:
        if reason is None:
            print("ok   {}".format(target))
        else:
            print("FAIL {} -> {}".format(target, reason))
            failures.append("{} ({})".format(target, reason))

    if failures:
        print("smoke FAILED: {}/{} targets broken: {}".format(
            len(failures), len(rows), ", ".join(failures)))
        raise SystemExit(1)

    print("smoke ok: {}/{} targets".format(len(rows), len(rows)))


if __name__ == "__main__":
    main()
