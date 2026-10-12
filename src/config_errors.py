"""Turn a pydantic-settings ValidationError into a clear, actionable startup message.

Reused by every entrypoint that builds settings — the app and any standalone
service (e.g. an MCP server) — so a missing or invalid environment variable fails
fast with a readable message (naming the env var) instead of a raw pydantic
traceback.
"""

import sys
from typing import Callable, TypeVar

from pydantic import ValidationError

T = TypeVar("T")

# Appended to the one entry it explains, not to the whole block (it would be false
# next to e.g. an integer parse error). Pydantic describes the value after
# stripping, so `GRIST_SERVER=" "` reads "at least 1 character" next to a visibly
# non-empty value. ASCII only: stderr under a C locale would raise
# UnicodeEncodeError.
WHITESPACE_NOTE = (" (the value is not empty as typed: it holds only whitespace, and values"
                   " are stripped of surrounding whitespace before they are checked, so it"
                   " counts as empty)")

# The error kind the note explains: pydantic's type for a failed `min_length`
# (`Required` and `Secret` in src/settings.py, both stripped first). Keyed by kind,
# not input shape: `HEALTH_PORT="  "` is whitespace-only but fails as
# `int_parsing`, where the note would be false. The type string is stable; `msg`
# is pydantic's to reword.
STRIPPED_TO_EMPTY_TYPE = "string_too_short"


def load_settings_or_exit(factory: Callable[[], T]) -> T:
    """Build a settings object via `factory` (e.g. a BaseSettings subclass).

    On a configuration ValidationError, print a clear message that names the
    offending environment variable(s) and exit(1) — no pydantic traceback. Any
    non-ValidationError is left to propagate unchanged.
    """
    try:
        return factory()
    except ValidationError as exc:
        missing = []
        invalid = []
        for err in exc.errors():
            # loc[0] is the field name; the env var is its upper-case form.
            name = str(err["loc"][0]).upper() if err.get("loc") else "?"
            if err.get("type") == "missing":
                missing.append(name)
                continue
            item = "{}: {}".format(name, err.get("msg"))
            # `err["input"]` may be a live credential: only its shape is read,
            # never stored or printed. The type makes the note true of the error;
            # `raw != ""` and `raw.strip() == ""` make it true of the value (not
            # empty as typed, emptied by stripping).
            raw = err.get("input")
            if (err.get("type") == STRIPPED_TO_EMPTY_TYPE
                    and isinstance(raw, str) and raw != "" and raw.strip() == ""):
                item += WHITESPACE_NOTE
            invalid.append(item)
        lines = ["Configuration error in environment / .env:"]
        if missing:
            lines.append("  Missing required variable(s):")
            lines.extend("    - {}".format(n) for n in missing)
        if invalid:
            lines.append("  Invalid value(s):")
            lines.extend("    - {}".format(item) for item in invalid)
        lines.append("")
        lines.append("Set them in .env (see .env.example) and try again.")
        print("\n".join(lines), file=sys.stderr)
        # `from None`: the ValidationError context renders every rejected field
        # as `input_value=<raw value>`, i.e. live credentials, and this generic
        # function cannot know which fields to redact. A caller logging the
        # SystemExit with `exc_info=True` would otherwise print them.
        raise SystemExit(1) from None
