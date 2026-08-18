"""Single configuration entry point for the whole service.

Every environment variable the program reads is declared here; nothing else calls
`os.getenv` for configuration. A missing or invalid variable fails at startup with
a message naming it (see `src/config_errors.py`), never with a `None` — or an
empty string, which behaves just as badly and looks configured — travelling into
a client.

The variable NAMES below are a production contract: they are what the
`crypt-common` stack on nebula feeds the `tokenchecker` container. Renaming any of
them takes production down — the stack passes GRIST_SERVER, GRIST_DOC_ID,
GRIST_API_KEY, ETHERSCAN_API_KEY, TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID and
nothing else.
"""

from typing import Annotated, Optional

from pydantic import BeforeValidator, StringConstraints
from pydantic_settings import BaseSettings, SettingsConfigDict

from src.config_errors import load_settings_or_exit
from src.redact import MIN_SECRET_LENGTH

# "Present but not actually set" for a required variable. `strip_whitespace`
# runs BEFORE `min_length`, which is the entire reason both are here: a bare
# `min_length=1` accepts `GRIST_SERVER=" "` — a stray space left in the stack's
# `environment:` block, a secret that rendered to whitespace, a value quoted with
# a trailing newline — and that string then behaves exactly like the empty one
# this guard exists to stop. Stripping is also applied to the value that is
# KEPT, so a padded address never reaches a client half-quoted.
Required = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]

# `Required` plus a FLOOR on the length, for the fields that are credentials —
# and the floor is `redact()`'s own threshold rather than a number picked here.
# `redact()` silently declines to replace anything shorter than
# `MIN_SECRET_LENGTH`, so a credential below it is one that travels into `docker
# logs` and into the wallet's `Comment` cell in full, for the life of the
# deployment, with nothing anywhere reporting that it is not being hidden.
# `Required` alone (`min_length=1`) accepts a five-character `ETHERSCAN_API_KEY`
# and that is exactly the state the redaction cannot cover.
#
# The constant is IMPORTED, not restated: the two numbers have to move together,
# so raising the threshold in `src/redact.py` cannot leave a credential stranded
# below it. That turns "every secret this service holds is long enough to redact"
# into a property of the program instead of an observation about today's keys.
#
# Deliberately NOT for `grist_server` / `grist_doc_id`: those are addresses, not
# secrets, no caller ever hands them to `redact()`, and a short Grist doc id is an
# ordinary one — the floor would reject a working production value for a reason
# that does not apply to it.
Secret = Annotated[str, StringConstraints(strip_whitespace=True,
                                          min_length=MIN_SECRET_LENGTH)]

# The stripping half on its own, for the fields that are allowed to be absent.
# Same accident, different consequence: an optional variable may be missing, but
# it may not be padded. `TELEGRAM_BOT_TOKEN` with a trailing newline — a here-doc
# in the stack's `environment:`, a copy-paste out of BotFather — goes straight
# into an f-string URL in `src/watchdog.py`, and nothing on the way objects.
# MEASURED against the pinned requests 2.32.3 / urllib3 2.2.2, not assumed: the
# padding is percent-encoded into the path (`/bot<token>%0A/sendMessage`), the
# request is sent for real, and no exception is raised at any layer. What reaches
# the API is therefore a token that is not the token, so the call comes back
# rejected — an ordinary HTTP answer saying no. `TELEGRAM_CHAT_ID` is the same
# shape one level down: copied verbatim into the JSON body, and likewise not the
# chat id.
#
# So the failure is not a crash anybody would notice; it arrives at the one moment
# the notification exists for, with the watchdog fired and the process about to
# `os._exit(1)`. `src/watchdog.py` now checks that response instead of assuming
# it; stripping here is what stops the padded value from producing it at all.
Stripped = Annotated[str, StringConstraints(strip_whitespace=True)]


def _blank_to_none(value):
    """An empty or whitespace-only optional variable means ABSENT, not "".

    Runs BEFORE the field's own validation, and that ordering is the whole
    mechanism. `TELEGRAM_BOT_TOKEN=` left in the stack's `environment:` block — or
    the same line with a space after the `=`, or a secret that rendered to a
    newline — is a half-configured deployment, which this service supports on
    purpose: the watchdog then logs "notification skipped" and dies quietly.
    Without this hook, any `min_length` on such a field would turn that supported
    state into a container that refuses to start, which is precisely the objection
    that used to keep `TELEGRAM_BOT_TOKEN` off the redaction floor. Collapsing
    blank to `None` first removes it: absence is spelled `None` and skips every
    constraint, so the floor below is left to judge only values somebody actually
    set.

    Non-strings are passed through untouched — pydantic still owns the type error.
    """
    if isinstance(value, str) and value.strip() == "":
        return None
    return value


# The optional aliases. `BeforeValidator` is shared by both because the reading of
# "blank" must not differ between two variables that are configured on adjacent
# lines and are meant to be set together.
BlankIsAbsent = BeforeValidator(_blank_to_none)

# An optional CREDENTIAL: absent, or long enough for `redact()` to cover it. The
# floor is not weaker for being optional — `TELEGRAM_BOT_TOKEN` is one of the
# three credentials this process holds (`safe_text()` in `src/checker.py` hands it
# to `redact()`), and it is the one that travels as a PATH SEGMENT of the URL
# `raise_for_status()` quotes back verbatim. Below `MIN_SECRET_LENGTH` `redact()`
# silently declines, so such a token reaches `docker logs` in full, at the moment
# the watchdog is killing the container. Optional describes whether it may be
# missing, not how short it may be when present.
OptionalSecret = Annotated[Optional[Secret], BlankIsAbsent]

# An optional NON-credential: absent, or stripped, with no floor. Same blank
# handling — the two are set together, so they have to agree on what "set" means —
# but deliberately no `min_length`: see the field below.
OptionalStripped = Annotated[Optional[Stripped], BlankIsAbsent]


class Settings(BaseSettings):
    # --- Credentials and self-hosted addresses: fields WITHOUT a default. ------
    # Grist is our own deployment, so its address depends on the deploy and comes
    # from the environment only — a default here would let a misconfigured
    # container talk to the wrong document, or to nothing, without saying so.
    #
    # None of them is a bare `str`, because "declared but empty" is a real
    # accident (`GRIST_SERVER=` left in the stack's `environment:` block, or the
    # same line with a space after the `=`) and a bare `str` accepts both. Such a
    # string then travels into the client exactly like the `None` this file exists
    # to stop, only quieter — the container comes up, reports healthy, and fails on
    # its first call with a URL error nobody connects to a missing variable. The
    # addresses get `Required` (stripped, then non-empty); the two keys get
    # `Secret`, which is that plus the redaction floor.
    grist_server: Required
    grist_doc_id: Required
    # `Secret` and not `Required` for the two API keys: they are the values that
    # go through `redact()`, so they additionally may not be shorter than the
    # length below which `redact()` stops replacing anything (see the alias).
    grist_api_key: Secret

    # Etherscan V2 API key. A credential never has a default.
    etherscan_api_key: Secret

    # --- Optional: the watchdog's death notice. --------------------------------
    # Both or neither. The watchdog only sends the notification when it has a
    # token AND a chat id, so a half-configured pair is not an error — it is a
    # deployment that gets no Telegram message when the process kills itself.
    # They are optional because the service works without them: the notification
    # is a convenience on top of the log line and the container restart.
    #
    # The token carries the redaction floor like the other two credentials, only
    # through the optional alias: blank still means "not configured", and anything
    # actually set has to be long enough for `redact()` to hide. The chat id does
    # NOT, and the asymmetry is the point — it is the addressee, it is never handed
    # to `redact()` anywhere in this repository (see `safe_text()` in
    # `src/checker.py` and the handler in `src/watchdog.py`), and a real chat id is
    # a short signed integer that a floor would reject for a reason that does not
    # apply to it.
    telegram_bot_token: OptionalSecret = None
    telegram_chat_id: OptionalStripped = None

    # --- Non-secret operational configuration: a default is fine. --------------
    # The port the /health server listens on. 8080 is a production contract: the
    # `crypt-common` stack does NOT pass HEALTH_PORT, and the Dockerfile's
    # HEALTHCHECK curls this same default — so changing it here without changing
    # the Dockerfile leaves docker probing a port nothing listens on, which is
    # what auto-heal restarts the container on.
    health_port: int = 8080

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


# Build settings with clear startup errors: a missing/invalid variable prints a
# readable message naming the env var and exits, instead of a raw pydantic
# traceback.
settings = load_settings_or_exit(Settings)
