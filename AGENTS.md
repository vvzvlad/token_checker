# Agent Instructions — token_checker

A single-process loop: read wallet rows from a Grist document, ask api.etherscan.io for the
balance of a token (or of ETH) on one wallet, write the result back to Grist. One wallet per
iteration; a wallet is "due" when its `Value` cell is empty and it has an address, so an
operator re-arms a wallet by clearing that cell. The only port it opens is the local
`/health` endpoint; the only persistent thing it touches is the Grist document.

Production is the `tokenchecker` container in the `crypt-common` stack on **nebula**
(`restart: unless-stopped`, `io.portainer.update.enable: true`,
`io.portainer.autoheal.enable: true`). CI builds and publishes the image to
`gitea.vvzvlad.xyz/projects/token_checker`.

## Project structure
- `main.py` — thin entry point; calls `src.checker.run()`
- `src/settings.py` — the single config entry point (pydantic-settings)
- `src/config_errors.py` — ValidationError → readable message + `exit(1)`
- `src/redact.py` — cuts credential values out of text on its way to a log or to Grist
- `src/checker.py` — the main loop
- `src/balances.py` — the Etherscan calls and the wallet pick-up rule
- `src/health.py` — the `/health` endpoint and its class-level health flag
- `src/watchdog.py` — the dead-man switch that kills the process on a stuck loop
- `tests/` — pytest; offline, HTTP mocked, Grist replaced by a recording double
- `ci/smoke.py` — the image gate, run on the CI runner between build and push

## Commands
`make help` lists targets: `make install`, `make env` (copies `.env.example`), `make test`,
`make run`, `make clean`. Tests run in CI, not on the laptop.

## Invariants

### Environment variable names are a production contract
`GRIST_SERVER`, `GRIST_DOC_ID`, `GRIST_API_KEY`, `ETHERSCAN_API_KEY` (required) and
`TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`, `TELEGRAM_BOT_API_SERVER` (optional) are exactly
what the stack supplies; renaming one takes production down. The stack does not pass
`HEALTH_PORT`, so the 8080 default is what production listens on and what the Dockerfile's
`HEALTHCHECK` curls.

### The wallet-picking Grist read stays in the outer `try`
`none_value_wallet` outlives the iteration. The inner handler writes `"--"` into the wallet
it was working on, so the read must stay in the outer `try` (log and sleep); under the inner
handler a read failure would overwrite the previous wallet's correct value.
`tests/test_checker.py` pins this.

### Health endpoint and HEALTHCHECK drive the deployment
`/health` answers 200 / 503 / 404; the HEALTHCHECK runs `curl -f` against it. Auto-heal
restarts an `unhealthy` container; auto-update waits for `healthy` within
`max(120s, start_period + 15s)` and rolls back otherwise. The current timings settle in
~40 s; do not lengthen the interval without a matching start-period. `curl` is not in
`python:3.13-slim` and is installed explicitly; `ci/smoke.py` checks the endpoint and
docker's verdict separately.

The loop raises the health flag at the top of every iteration; the idle "all wallets have
values" branch would otherwise keep a flag the watchdog lowered down forever.

### The watchdog calls `os._exit`
It counts down from 300 s, reset every iteration; below 60 s `/health` goes 503 so auto-heal
can act first. At zero it logs, tries a Telegram notification, flushes and calls
`os._exit(1)` — `sys.exit` in a background thread only ends that thread. Request timeouts do
not replace it (one call can take ~60 s, name resolution and deadlocks are unbounded), so do
not weaken its thresholds.

### Container
`CMD` is a direct `python main.py`; do not add a shell retry loop (restarting is
`restart: unless-stopped`'s job). No `USER` directive and no `EXPOSE`: `entrypoint.sh` starts
as root, chowns `/app/data`, drops to `app` (uid 1000) via gosu, respects a compose `user:`
override. `ci/smoke.py` reads the uid from `/proc/1/status` (`docker exec id -u` bypasses the
entrypoint).

### Credentials
- Never log, raise or write the raw text of a `requests` exception: every credential travels
  in a URL and `requests` quotes it. Put it through `redact()` first, for logs and Grist cells.
- `redact()` skips values shorter than `MIN_SECRET_LENGTH`. A new credential goes on `Secret`
  (or `OptionalSecret` when it may be absent), whose `min_length` is that constant — never on
  `Required` or `Stripped`. `OptionalSecret` collapses blank to `None` first, so a blank
  `TELEGRAM_BOT_TOKEN=` still starts the container; keep that validator.
- Do not put `GRIST_SERVER`, `GRIST_DOC_ID` or `TELEGRAM_CHAT_ID` on a floor, and do not hand
  non-credentials to `redact()`.
- A `raise` wrapping a redacted exception carries `from None` (see `src/balances.py` and
  `src/config_errors.py`).
- CI placeholder credentials in both workflows and `SMOKE_ENV` must clear the floor;
  `tests/test_ci_placeholders.py` checks them.

## CI
`.gitea/workflows/`:
- `tests.yml` — PR gate: test job, then build + `ci/smoke.py`. No login, no push, no secrets.
- `image-check-publish.yml` — push to `main`: same test job, build, gate, then login and push
  `:<sha>` and `:latest` in that order.

The suite runs inside `python:3.13-slim` (the image's base) with the tree streamed in as a
tar; no `actions/setup-python`, no bind mount. Shared `run:` bodies are byte-identical
between the two files. `ci/smoke.py` runs on the runner, publishes no port, reads container
internals with `docker exec`, and counts its verdicts against `EXPECTED_TARGETS`: on a
mismatch find the probe that went quiet, do not edit the constant.

## Conventions
- All config comes from ENV / `.env` through `src/settings.py`; no `os.getenv` elsewhere, no
  defaults for credentials or for addresses of our own services.
- No real token, key, doc id or server address in the repository.
- Code comments and log messages in English.
- Repeated actions go through `make` targets; Python runs inside the local `.venv`.
- Tests are required for new code.

## Limits for agents
- Tests: at most three times the code (`wc -l` of `tests/` against the code). Over the limit,
  still write the test you need, and in the same change go through the old tests: drop or merge
  duplicates and tests of implementation rather than behaviour.
- Comments explain the code as it is and why it is built this way. History ("used to", "after
  the review", "since the incident") goes into the commit message.
- This file stays under ~150 lines: only what an agent would get wrong without it. No decision
  history, incident logs or retelling of the code.
