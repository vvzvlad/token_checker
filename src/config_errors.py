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

# Appended to THE ONE entry it explains, never printed for the block.
#
# Said at all because pydantic's own wording describes the value AFTER stripping
# while the operator is looking at the one before it: `GRIST_SERVER=" "` is
# reported as "String should have at least 1 character", which reads as plainly
# false next to a visibly non-empty value in the stack's `environment:` block.
# Whitespace-only is the shape this actually arrives in — a stray space after the
# `=`, a quoted value with a trailing newline, a secret that rendered to nothing.
#
# Attached per-entry because it is true of one value and not of its neighbours.
# Hung under the whole `Invalid value(s):` block it also landed under
# `HEALTH_PORT: Input should be a valid integer`, where nothing was stripped and
# nothing counts as empty — an annotation that contradicts the line it annotates
# is worse than none. A mixed failure (a whitespace value alongside a parse error)
# is the case that shows it, and it is the ordinary case: a broken
# `environment:` block rarely breaks exactly one variable.
#
# ASCII only, deliberately: this goes to stderr, and a container started under a
# C locale would raise UnicodeEncodeError on the way out of the handler whose job
# is to explain a configuration error.
WHITESPACE_NOTE = (" (the value is not empty as typed: it holds only whitespace, and values"
                   " are stripped of surrounding whitespace before they are checked, so it"
                   " counts as empty)")

# The one failure the note explains, keyed by the KIND of error rather than by the
# SHAPE of the input — which is the distinction the note gets wrong the moment the
# two are conflated. Whitespace-only is a shape a value of ANY field can have, and
# `HEALTH_PORT="  "` has it: it fails as `int_parsing` ("Input should be a valid
# integer"), against a field that carries no string constraint and no emptiness
# check at all, so "values are stripped before they are checked, so it counts as
# empty" is simply false underneath it — the contradiction described above, in the
# one shape that a per-entry note does not by itself rule out.
#
# `string_too_short` is pydantic's type for a failed `min_length`, i.e. exactly the
# constraint that "counts as empty" is about: `Required` (min_length=1) and
# `Secret` (min_length=MIN_SECRET_LENGTH) in src/settings.py, both of which strip
# first. Matching on pydantic's own type string is the narrowest handle there is
# here — the human-readable `msg` is pydantic's to reword at any time, and the
# alternative, re-deriving which fields are strings, is exactly the knowledge this
# deliberately generic function does not have.
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
            # `err["input"]` is the raw value — for GRIST_API_KEY or
            # ETHERSCAN_API_KEY, the live credential. Only its SHAPE is read here
            # and it is never stored, formatted or printed; that is the same
            # reason the `raise` at the bottom carries `from None`.
            #
            # The note is built INTO this entry rather than collected into a flag
            # for later, because "later" is after the whole list has been printed
            # and there is no longer any way to say which line it belongs to.
            #
            # Two conditions, and each rules out a different way of lying. The TYPE
            # is what makes the note true of the error (see `STRIPPED_TO_EMPTY_TYPE`
            # above): only a failed `min_length` is a check that stripping decided.
            # The INPUT is what makes it true of the value — `raw != ""` because the
            # note opens by claiming "the value is not empty as typed", which of an
            # honestly empty `GRIST_SERVER=` is the one thing it is not, and
            # `raw.strip() == ""` because the rest of the sentence claims stripping
            # is what emptied it.
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
        # `from None`, per the rule in AGENTS.md, and this is the handler where it
        # buys the most. Inside an `except` python attaches the ValidationError as
        # `__context__`, and pydantic renders every rejected field as
        # `input_value=<the value exactly as it arrived>` — for GRIST_API_KEY or
        # ETHERSCAN_API_KEY that is the live credential, in full, verbatim. It
        # cannot be redacted on the way past either: this function is deliberately
        # generic over the settings class and has no idea which of its fields are
        # secret, so `redact()` is not something it can call.
        #
        # Nothing prints it today — CPython renders no traceback for an unhandled
        # SystemExit. But the docstring above promises this function to every other
        # entrypoint, and the first caller that catches SystemExit and logs it with
        # `exc_info=True` renders the whole chain into `docker logs`. Same argument
        # as the handler in `src/balances.py`, same cost: none.
        raise SystemExit(1) from None
