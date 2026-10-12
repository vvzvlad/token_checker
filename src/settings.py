"""Single configuration entry point for the whole service.

Every environment variable the program reads is declared here; nothing else calls
`os.getenv` for configuration. A missing or invalid variable fails at startup with
a message naming it (see `src/config_errors.py`), never with a `None` or an empty
string travelling into a client.

The variable NAMES below are a production contract: they are what the
`crypt-common` stack on nebula feeds the `tokenchecker` container. Renaming any of
them takes production down — the stack passes GRIST_SERVER, GRIST_DOC_ID,
GRIST_API_KEY, ETHERSCAN_API_KEY, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID and
TELEGRAM_BOT_API_SERVER and nothing else.
"""

from typing import Annotated, Optional

from pydantic import BeforeValidator, StringConstraints
from pydantic_settings import BaseSettings, SettingsConfigDict

from src.config_errors import load_settings_or_exit
from src.redact import MIN_SECRET_LENGTH

# A required, non-blank variable. `strip_whitespace` runs before `min_length`, so
# `GRIST_SERVER=" "` is rejected, and the kept value is stripped too.
Required = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]

# `Required` plus `redact()`'s own length floor, for credentials: `redact()` does
# not replace anything shorter than `MIN_SECRET_LENGTH`, so a shorter credential
# would leak. The constant is imported so the two cannot drift. Not used for
# `grist_server` / `grist_doc_id`, which are not secrets and may be short.
Secret = Annotated[str, StringConstraints(strip_whitespace=True,
                                          min_length=MIN_SECRET_LENGTH)]

# The stripping half on its own, for optional fields. A padded
# `TELEGRAM_BOT_TOKEN` is percent-encoded into the URL path
# (`/bot<token>%0A/sendMessage`) and silently rejected by the API; a padded
# `TELEGRAM_CHAT_ID` goes verbatim into the JSON body.
Stripped = Annotated[str, StringConstraints(strip_whitespace=True)]


def _blank_to_none(value):
    """An empty or whitespace-only optional variable means ABSENT, not "".

    Runs before the field's own validation. A blank `TELEGRAM_BOT_TOKEN=` is a
    supported half-configured deployment (the watchdog skips the notification);
    collapsing it to `None` keeps the length floor from refusing to start, so the
    floor judges only values somebody actually set.

    Non-strings are passed through untouched — pydantic still owns the type error.
    """
    if isinstance(value, str) and value.strip() == "":
        return None
    return value


# Shared by both optional aliases, so the pair agrees on what "blank" means.
BlankIsAbsent = BeforeValidator(_blank_to_none)

# An optional credential: absent, or long enough for `redact()` to cover it.
# `TELEGRAM_BOT_TOKEN` is a path segment of the URL `raise_for_status()` quotes.
OptionalSecret = Annotated[Optional[Secret], BlankIsAbsent]

# An optional non-credential: absent, or stripped, with no floor.
OptionalStripped = Annotated[Optional[Stripped], BlankIsAbsent]


class Settings(BaseSettings):
    # --- Credentials and self-hosted addresses: fields WITHOUT a default. ------
    # Grist is our own deployment, so its address depends on the deploy and comes
    # from the environment only — a default here would let a misconfigured
    # container talk to the wrong document, or to nothing, without saying so.
    # Not bare `str`, which accepts `GRIST_SERVER=` or a lone space. Addresses get
    # `Required`; the two keys get `Secret` (plus the redaction floor).
    grist_server: Required
    grist_doc_id: Required
    grist_api_key: Secret

    # Etherscan V2 API key. A credential never has a default.
    etherscan_api_key: Secret

    # --- Optional: the watchdog's death notice. --------------------------------
    # Both or neither: the notification is sent only with a token AND a chat id; a
    # half-configured pair just means no Telegram message. The token carries the
    # redaction floor; the chat id is the addressee, never redacted, and a short
    # signed integer, so it has no floor.
    telegram_bot_token: OptionalSecret = None
    telegram_chat_id: OptionalStripped = None
    # A self-hosted Bot API server, `http(s)://host[:port]`; unset means the
    # public https://api.telegram.org.
    telegram_bot_api_server: OptionalStripped = None

    # --- Non-secret operational configuration: a default is fine. --------------
    # The port the /health server listens on. 8080 is a production contract: the
    # stack does not pass HEALTH_PORT and the Dockerfile's HEALTHCHECK curls the
    # same default; change both together.
    health_port: int = 8080

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


# Build settings with clear startup errors: a missing/invalid variable prints a
# readable message naming the env var and exits, instead of a raw pydantic
# traceback.
settings = load_settings_or_exit(Settings)
