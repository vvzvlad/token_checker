# Agent Instructions — token_checker

A single-process loop: read wallet rows from a Grist document, ask api.etherscan.io for the
balance of a token (or of ETH) on one wallet, write the result back to Grist. One wallet per
iteration; a wallet is "due" when its `Value` cell is empty and it has an address, so an
operator re-arms a wallet by clearing that cell. No inbound service — the only port it opens
is the local `/health` endpoint, and the only persistent thing it touches is the Grist
document.

Production is the `tokenchecker` container in the `crypt-common` stack on **nebula**
(`restart: unless-stopped`, `io.portainer.update.enable: true`,
`io.portainer.autoheal.enable: true`). The image is built by CI and published to
`gitea.vvzvlad.xyz/projects/token_checker`.

## Project structure
- `main.py` — thin entry point; calls `src.checker.run()`
- `src/` — all application code
  - `settings.py` — the single config entry point (pydantic-settings)
  - `config_errors.py` — ValidationError → readable message + `exit(1)`
  - `redact.py` — cuts credential values out of text on its way to a log or to Grist
  - `checker.py` — the main loop
  - `balances.py` — the Etherscan calls and the wallet pick-up rule
  - `health.py` — the `/health` endpoint and its class-level health flag
  - `watchdog.py` — the dead-man switch that kills the process on a stuck loop
- `tests/` — pytest
- `ci/smoke.py` — the image gate, run on the CI runner between build and push
- `data/` — runtime state directory (gitignored; empty today, see below)

## Setup
All routine actions go through the `Makefile` — run `make help` to list targets.
```bash
make install           # create .venv and install dev/test deps
cp .env.example .env   # then fill in the values  (shortcut: make env)
```

## Running tests
```bash
make test              # runs .venv/bin/python -m pytest
```
The suite never touches the network: `tests/conftest.py` injects placeholder credentials
before anything can import `src.settings`, every HTTP call is mocked at the `requests`
boundary, and the Grist client is replaced by a recording double.

## Running the app
```bash
make run               # runs .venv/bin/python main.py
```

## What this service is, in the places it can bite you

### The environment variable names are a production contract
`GRIST_SERVER`, `GRIST_DOC_ID`, `GRIST_API_KEY`, `ETHERSCAN_API_KEY` (all required) and
`TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`, `TELEGRAM_BOT_API_SERVER` (all optional) are exactly what the `crypt-common`
stack supplies. Renaming any of them takes production down. `HEALTH_PORT` exists in
`src/settings.py` but the stack does **not** pass it, so the 8080 default is what production
listens on — and it is the port the Dockerfile's `HEALTHCHECK` curls.

### The Grist read that picks the wallet lives in the OUTER `try`, and must stay there
`run()`'s loop has two nested handlers: an outer one that logs and sleeps, and an inner one
that writes `"--"` into the wallet it was working on. `none_value_wallet` is a local of
`run()` while the `while True` is inside `run()`, so the binding **survives the iteration**.

While `find_none_value(grist, NODES_TABLE)` was inside the inner `try`, any transient Grist read failure
sent that handler to `grist.update(NODES_TABLE, none_value_wallet.id, {"Value": "--", "Comment": ...})`
holding the wallet from the **previous** iteration — and that write succeeded. A balance
computed correctly seconds earlier was replaced by `"--"` and an error message about a
failure that had nothing to do with that wallet, with nothing afterwards to tell it apart
from a wallet that genuinely failed. (The other two shapes of the same bug — an unbound name
on the first iteration, `None.id` after an idle one — were only noise in the log.)

So the read now sits in the outer `try`: choosing a wallet is not part of working on one,
and a failure there leaves no wallet the `"--"` message could honestly be about. The inner
`try` wraps exactly the wallet step — the Etherscan call and the write-back — which is what
makes "everything this handler catches happened while working on this wallet" true by
construction. Do not move it back for symmetry. `tests/test_checker.py` pins it with two
iterations whose second one cannot read Grist.

### The health endpoint and the docker HEALTHCHECK are what the deployment acts on
`src/health.py` answers `200 {"status": "healthy"}`, `503 {"status": "unhealthy", ...}` and
`404`; the Dockerfile probes it with `curl -f http://localhost:${HEALTH_PORT:-8080}/health`.
Our Portainer build reads docker's verdict in **both** directions:
- **auto-heal** restarts a container docker reports `unhealthy`;
- **auto-update**, after it recreates the container with a new image, waits for `healthy`
  within `max(120s, start_period + 15s)` and **rolls the image back** otherwise.

So the `HEALTHCHECK` line must stay, and it must converge quickly: the current
`--start-period=10s --interval=10s --retries=3` settles about 40 s in, well inside the
120 s window. Lengthening the interval without a matching start-period is how a perfectly
good image starts getting rolled back.

The endpoint and the probe can disagree, and that is why `ci/smoke.py` checks both
separately: the endpoint is answered by python over a socket, the probe shells out to
`curl`. The base is `python:3.13-slim`, which does not ship `curl` — it is installed
explicitly next to `gosu` in the Dockerfile's apt layer. Drop it from that line and the
endpoint answers perfectly while docker calls the container unhealthy forever, auto-heal
restarts it every ~40 s and every other check stays green.

### The watchdog calls `os._exit`, and that is deliberate
`src/watchdog.py` counts down from 300 s and the main loop resets it every iteration. When
it reaches zero it logs the reason, tries a Telegram notification, flushes, and calls
`os._exit(1)`.

It used to call `sys.exit(1)`, which is a bug in a background thread: `sys.exit` raises
`SystemExit` **in the calling thread only**, so the countdown thread died quietly, the
process carried on, and the watchdog stopped existing — after logging that it had fired. Do
not "tidy" this back into `sys.exit`. `PYTHONUNBUFFERED=1` in the Dockerfile belongs to the
same fix: `os._exit` skips CPython's buffer flush, and the reason has to reach `docker logs`.

Below 60 s remaining the endpoint goes 503 first, on purpose: a restart driven by docker's
health verdict is visible in `docker inspect`, an `os._exit` is only a log line.

The main loop puts the health flag back up at the **top of every iteration**, and that line
is load-bearing: the "all wallets have values" branch passes through neither the balance
call nor either handler, so without it a flag the watchdog had lowered once would stay down
for the rest of the process's life as soon as the loop settled into that branch — `/health`
answering 503 forever, auto-heal restarting a service that works, and the fresh process
settling into the same state.

### Request timeouts do not retire the watchdog
`grist_client` sets a 30 s timeout on every Grist request; the Etherscan and Telegram calls
pass their own. `requests` applies it to the connect and the read phase separately, so one
call can take ~60 s, and an iteration's seven calls (three Settings reads, the Chains read,
the wallets read, the Etherscan lookup, the write-back) can outlast the watchdog's 300 s on
their own. Name resolution, a connection that dribbles bytes and a deadlock in our own code
are bounded by no timeout at all. The watchdog is the cover for all of these: do not weaken
its thresholds on the strength of the timeouts.

### `CMD` is a direct `python main.py`
It used to be `while true; do python token_checker.py; sleep 10; done`. That wrapper made
every startup failure invisible: a container with broken configuration looked alive, because
the shell outlived the program and restarted it forever. Restarting is
`restart: unless-stopped`'s job. Do not reintroduce a retry loop in `CMD`.

### The container runs as non-root without a `USER` directive
`entrypoint.sh` starts as root, heals `/app/data` ownership and drops to `app` (uid 1000)
via gosu; a compose `user:` override is respected. There is deliberately no `USER` line, so
nothing in the image *declares* a non-root user and `docker inspect` cannot answer the
question — `ci/smoke.py` reads `/proc/1/status` instead. Note that `docker exec ... id -u`
would answer 0 in a perfectly healthy container: exec does not go through the ENTRYPOINT.

## CI
Two workflows in `.gitea/workflows/`:
- `tests.yml` — the PR gate: test job, then build + `ci/smoke.py`. No login, no push, and no
  secret expression anywhere in the file.
- `image-check-publish.yml` — push to `main`: the same test job, the same build and the same
  gate, then login, then push `:<sha>` and `:latest` in that order.

`build` **needs** `test`, so red tests cannot produce an image. The suite runs inside a
`python:3.13-slim` container — the same interpreter version the image ships — with the work
tree streamed in as a tar over stdin. Not `actions/setup-python` (setup actions are not
verified on this runner and fail by silently doing nothing) and not a bind mount (the job
runs in a container while docker runs on the host, so `$PWD` means nothing there).

`ci/smoke.py` runs **on the runner** and drives docker itself, because most of what it asks
is about the container as an object. It publishes no port and never talks to `127.0.0.1`:
anything that has to be seen from inside a container is read with `docker exec`. It counts
its own verdicts against `EXPECTED_TARGETS` and fails with a distinct exit code when the
count disagrees — when that fires, find the probe that went quiet; do **not** reconcile the
constant with what the run produced.

## Conventions
- All mutable state goes under `data/`.
- All config comes from ENV / `.env` (see `.env.example`) through `src/settings.py`. No
  `os.getenv` for configuration anywhere else, no defaults for credentials or for addresses
  of our own services.
- Credentials the user provides go ONLY into `.env` — never into code, never via inline env
  vars on the command line. Nothing in this repository may contain a real token, key, doc id
  or server address.
- Never log, raise or write out the raw text of an exception from a `requests` call. Every
  credential this service holds travels in a URL, and `requests` puts the full URL into the
  text of any transport error — so `f"...: {e}"` leaks the key even where no URL is logged.
  Put it through `redact()` from `src/redact.py` first; that goes for `docker logs` and, more
  so, for anything written into a Grist cell.
- `redact()` **silently leaves values shorter than `MIN_SECRET_LENGTH` in place.** Replacement
  is blind — every occurrence anywhere in the text — so a short, non-random value would match
  timestamps, exit codes and ordinary words rather than the secret, damaging the message
  without hiding anything. It is a floor on what can safely be blind-replaced, not a promise
  about a particular variable. What closes the gap that leaves is `src/settings.py`: its
  `Secret` alias takes its `min_length` **from that same constant**, so a credential too short
  to redact fails at startup instead of travelling out unredacted for the life of the
  deployment. **A new credential goes on `Secret` — or, when it may legitimately be absent, on
  `OptionalSecret` — never on `Required` or `Stripped`**, both of which accept it at any length
  while the redaction quietly declines to cover it. All three credentials this process holds
  (`GRIST_API_KEY`, `ETHERSCAN_API_KEY`, `TELEGRAM_BOT_TOKEN` — the set `safe_text()` in
  `src/checker.py` passes to `redact()`) sit on one of the two.
  `OptionalSecret` is `Secret` plus a before-validator that turns an empty or whitespace-only
  value into `None`, and that ordering is what makes an optional credential able to carry the
  floor at all: `TELEGRAM_BOT_TOKEN=` left in the stack's `environment:` block still means "not
  configured" and still starts the container, while a value somebody actually set is held to
  the floor. Do not "simplify" that validator away in favour of dropping the floor — the state
  it buys back is an eight-character bot token accepted at startup and then quoted into
  `docker logs` in full by `raise_for_status()`, which builds its message out of the URL the
  token is a path segment of.
  Do not put the non-secret `GRIST_SERVER` / `GRIST_DOC_ID` on `Secret`, or `TELEGRAM_CHAT_ID`
  on `OptionalSecret`: none of them is ever handed to `redact()` and all may legitimately be
  short. Do not hand a non-credential (an addressee, an id) to `redact()` at all — there is
  nothing to hide and the replacement can only corrupt the text.
  Nowhere in this repository is the length of a real key written down; the alias is the whole
  of the claim.
- A `raise` that wraps a **redacted** exception must carry `from None`. Inside an `except`
  python attaches the original — unredacted — exception as `__context__`, and
  `traceback.format_exc()`, `logger.error(..., exc_info=True)` and an unhandled exit to
  stderr all render the whole chain, so the credential leaves anyway while `str(e)` looks
  clean. Suppressing the context costs nothing when the wrapper already names the class and
  the redacted text. Two worked examples, each with its regression test: the handler in
  `src/balances.py`, and the `SystemExit` in `src/config_errors.py` — the latter because
  pydantic renders every rejected field as `input_value=<the value as it arrived>`, i.e. the
  raw credential, and a `SystemExit` printing no traceback today is not the same as one that
  never will.
- Code comments and log messages are in English.
- All repeated actions go through `make` targets — add or extend a target instead of running
  ad-hoc commands.
- Python always runs inside the local `.venv`, created automatically by `make` on first use
  — never the system Python, and always as `.venv/bin/python -m pip` / `-m pytest` rather
  than the `bin/` scripts.
- Tests are required for new code.
- No `EXPOSE` in the Dockerfile — this service publishes nothing; `/health` is reached from
  inside the container by the probe.
- Do not add a `USER` directive and do not remove the entrypoint.
