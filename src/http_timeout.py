"""Global default timeout for every outgoing `requests` call.

grist_api 0.1.0 calls `requests.request(...)` without a timeout (see
`grist_api.GristDocAPI.call`). WITHOUT this patch a stalled Grist TCP connection
hung the whole process until the watchdog's 300 s ran out and killed it — the
only thing that ever ended such a call was `os._exit()` and a container restart.

WITH it installed — and it is installed at import time in `src/checker.py`,
before any HTTP call can be made — that same stalled connection raises after 30 s
and the loop handles it like any other error: one line in the log, ten seconds of
sleep, round again. The process is not killed for it any more. That is a
deliberate change of behaviour, and `AGENTS.md` records what it did to the
watchdog's role.

Patching `Session.request` is what reaches grist_api: the module-level
`requests.get` / `requests.request` helpers route through it too. Explicit
timeouts (the Etherscan calls in `src/balances.py` pass `timeout=30`, the
Telegram notification in `src/watchdog.py` passes `timeout=10`) are preserved via
`setdefault`, so this only ever fills in a MISSING one.

What it still does not bound: the value is a connect/read pair rather than a
total budget — `requests` applies it to each phase separately, so one call can
take twice this number — and name resolution happens before either, so a DNS
lookup that never returns is a hang this cannot stop. Nor does it bound
`GristDocAPI.call`'s own `while True`, which sleeps two seconds and retries
forever on a `SQLITE_BUSY` answer: every ATTEMPT inside that loop gets the
timeout below, the loop itself gets nothing. Those cases are what is left for
the watchdog, which is why it is still the primary cover and not a formality —
see AGENTS.md, "The 30 s global request timeout changed what the watchdog is
for".
"""

import requests

DEFAULT_REQUEST_TIMEOUT = 30  # seconds (connect + read ceiling)

# Guards against stacking wrappers if this is called more than once (a test
# importing two modules that both install it, for instance).
_installed = False


def install_default_timeout(timeout=DEFAULT_REQUEST_TIMEOUT):
    """Patch `requests.Session.request` to default `timeout` when unset."""
    global _installed
    if _installed:
        return
    original_request = requests.Session.request

    def _session_request_with_timeout(self, *args, **kwargs):
        kwargs.setdefault("timeout", timeout)
        return original_request(self, *args, **kwargs)

    requests.Session.request = _session_request_with_timeout
    _installed = True
