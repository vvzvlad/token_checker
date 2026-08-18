"""The configuration object: the variable NAMES, the defaults, and the failure path.

The four required names are a production contract — the `crypt-common` stack on
nebula supplies exactly those, plus the two optional Telegram ones — so they are
pinned here rather than left to be noticed in production. `HEALTH_PORT` is pinned
for a second reason: the stack does NOT pass it, so the default in this file is
what production actually listens on, and it has to keep matching the port the
Dockerfile's HEALTHCHECK curls.
"""

import pytest
from pydantic import ValidationError

from src.config_errors import load_settings_or_exit
from src.settings import Settings

REQUIRED_VARS = ("GRIST_SERVER", "GRIST_DOC_ID", "GRIST_API_KEY", "ETHERSCAN_API_KEY")
OPTIONAL_VARS = ("TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID", "HEALTH_PORT")


def _fill_required(monkeypatch):
    monkeypatch.setenv("GRIST_SERVER", "http://grist.invalid")
    monkeypatch.setenv("GRIST_DOC_ID", "doc-1")
    monkeypatch.setenv("GRIST_API_KEY", "key-1")
    monkeypatch.setenv("ETHERSCAN_API_KEY", "etherscan-1")


def _clear_optional(monkeypatch):
    for name in OPTIONAL_VARS:
        monkeypatch.delenv(name, raising=False)


def test_required_variables_map_to_their_env_names(monkeypatch):
    # If a field were renamed, its env var name would change with it and the
    # stack would stop configuring the container.
    _fill_required(monkeypatch)
    _clear_optional(monkeypatch)
    s = Settings(_env_file=None)
    assert s.grist_server == "http://grist.invalid"
    assert s.grist_doc_id == "doc-1"
    assert s.grist_api_key == "key-1"
    assert s.etherscan_api_key == "etherscan-1"


def test_optional_defaults_match_the_documented_contract(monkeypatch):
    _fill_required(monkeypatch)
    _clear_optional(monkeypatch)
    s = Settings(_env_file=None)
    # None, not "" — the watchdog decides whether to notify by truthiness, and an
    # empty string would read the same way while hiding a half-set variable.
    assert s.telegram_bot_token is None
    assert s.telegram_chat_id is None
    # The port the Dockerfile's HEALTHCHECK curls when HEALTH_PORT is unset,
    # which is how production runs.
    assert s.health_port == 8080


def test_optional_variables_are_read_from_the_environment(monkeypatch):
    _fill_required(monkeypatch)
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "123:abc")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "-100")
    monkeypatch.setenv("HEALTH_PORT", "9090")
    s = Settings(_env_file=None)
    assert s.telegram_bot_token == "123:abc"
    assert s.telegram_chat_id == "-100"
    assert s.health_port == 9090          # coerced to int, not left as "9090"


def test_non_numeric_health_port_is_rejected(monkeypatch):
    _fill_required(monkeypatch)
    monkeypatch.setenv("HEALTH_PORT", "eighty-eighty")
    with pytest.raises(ValidationError):
        Settings(_env_file=None)


@pytest.mark.parametrize("missing", REQUIRED_VARS)
def test_each_credential_is_mandatory(monkeypatch, missing):
    # No silent fallback, no empty default: one absent variable must fail.
    _fill_required(monkeypatch)
    monkeypatch.delenv(missing, raising=False)
    with pytest.raises(ValidationError):
        Settings(_env_file=None)


@pytest.mark.parametrize("value", ["", " ", " \t\n "])
@pytest.mark.parametrize("blank", REQUIRED_VARS)
def test_a_required_variable_may_not_be_empty(monkeypatch, blank, value):
    # "Declared but empty" is a real accident — `GRIST_SERVER=` left in the stack's
    # `environment:` block, a secret that resolved to nothing — and a bare `str`
    # accepts it. The empty string then travels into the client exactly like the
    # `None` this module exists to stop, only quieter: the container starts,
    # reports healthy, and fails on its first call with an error nobody connects
    # to a missing variable.
    #
    # The whitespace cases are the same accident wearing a disguise, and
    # `min_length=1` alone lets both through: one stray space after the `=`, or a
    # secret that rendered to a newline, produces a value that is "set" by every
    # test the container can make and behaves exactly like the empty string.
    _fill_required(monkeypatch)
    monkeypatch.setenv(blank, value)
    with pytest.raises(ValidationError):
        Settings(_env_file=None)


def test_padding_is_stripped_off_a_value_that_is_kept(monkeypatch):
    # The other half of the same rule: a variable that is genuinely set but padded
    # (a quoted value with a trailing newline, a copy-paste with a leading space)
    # is accepted, and what reaches the client is the value without the padding —
    # not an address with a space in the middle of the URL it builds.
    _fill_required(monkeypatch)
    monkeypatch.setenv("GRIST_SERVER", "  http://grist.invalid\n")
    assert Settings(_env_file=None).grist_server == "http://grist.invalid"


@pytest.mark.parametrize("blank", REQUIRED_VARS)
def test_an_empty_variable_exits_1_and_names_that_variable(capsys, monkeypatch, blank):
    # Same treatment as a missing one: exit 1 with the variable named, no pydantic
    # traceback. It arrives as an "invalid value" rather than a "missing" one, so
    # this pins that the other half of src/config_errors.py names it too.
    _fill_required(monkeypatch)
    monkeypatch.setenv(blank, "")
    with pytest.raises(SystemExit) as exc_info:
        load_settings_or_exit(lambda: Settings(_env_file=None))
    assert exc_info.value.code == 1
    err = capsys.readouterr().err
    assert blank in err
    assert "Traceback" not in err


@pytest.mark.parametrize("missing", REQUIRED_VARS)
def test_one_missing_variable_exits_1_and_names_that_variable(capsys, monkeypatch, missing):
    # The whole point of the guard: the log says WHICH variable is missing, so a
    # forgotten line in the stack's `environment:` block is a five-second fix
    # rather than a container nobody can explain.
    _fill_required(monkeypatch)
    monkeypatch.delenv(missing, raising=False)
    with pytest.raises(SystemExit) as exc_info:
        load_settings_or_exit(lambda: Settings(_env_file=None))
    assert exc_info.value.code == 1
    err = capsys.readouterr().err
    assert "Missing required variable(s):" in err
    assert missing in err
    assert "Traceback" not in err
    assert "pydantic" not in err.lower()


def test_a_bare_environment_names_every_missing_variable(capsys, monkeypatch):
    for name in REQUIRED_VARS:
        monkeypatch.delenv(name, raising=False)
    with pytest.raises(SystemExit) as exc_info:
        load_settings_or_exit(lambda: Settings(_env_file=None))
    assert exc_info.value.code == 1
    err = capsys.readouterr().err
    for name in REQUIRED_VARS:
        assert name in err


def test_extra_environment_variables_do_not_break_startup(monkeypatch):
    # extra="ignore": the container's environment carries plenty of unrelated
    # variables (TZ, PATH, ...) and none of them may fail the start.
    _fill_required(monkeypatch)
    monkeypatch.setenv("SOMETHING_UNRELATED", "value")
    assert Settings(_env_file=None).grist_doc_id == "doc-1"
