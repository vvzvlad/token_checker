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

from pydantic import StringConstraints
from pydantic_settings import BaseSettings, SettingsConfigDict

from src.config_errors import load_settings_or_exit

# "Present but not actually set" for a required variable. `strip_whitespace`
# runs BEFORE `min_length`, which is the entire reason both are here: a bare
# `min_length=1` accepts `GRIST_SERVER=" "` — a stray space left in the stack's
# `environment:` block, a secret that rendered to whitespace, a value quoted with
# a trailing newline — and that string then behaves exactly like the empty one
# this guard exists to stop. Stripping is also applied to the value that is
# KEPT, so a padded address never reaches a client half-quoted.
Required = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]


class Settings(BaseSettings):
    # --- Credentials and self-hosted addresses: fields WITHOUT a default. ------
    # Grist is our own deployment, so its address depends on the deploy and comes
    # from the environment only — a default here would let a misconfigured
    # container talk to the wrong document, or to nothing, without saying so.
    #
    # `Required` (stripped, then non-empty) on every one of them, because
    # "declared but empty" is a real accident (`GRIST_SERVER=` left in the stack's
    # `environment:` block, or the same line with a space after the `=`) and a
    # bare `str` accepts both. Such a string then travels into the client exactly
    # like the `None` this file exists to stop, only quieter — the container comes
    # up, reports healthy, and fails on its first call with a URL error nobody
    # connects to a missing variable.
    grist_server: Required
    grist_doc_id: Required
    grist_api_key: Required

    # Etherscan V2 API key. A credential never has a default.
    etherscan_api_key: Required

    # --- Optional: the watchdog's death notice. --------------------------------
    # Both or neither. The watchdog only sends the notification when it has a
    # token AND a chat id, so a half-configured pair is not an error — it is a
    # deployment that gets no Telegram message when the process kills itself.
    # They are optional because the service works without them: the notification
    # is a convenience on top of the log line and the container restart.
    telegram_bot_token: Optional[str] = None
    telegram_chat_id: Optional[str] = None

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
