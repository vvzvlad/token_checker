"""The configuration object: the variable NAMES, the defaults, and the failure path.

The four required names are a production contract — the `crypt-common` stack on
nebula supplies exactly those, plus the two optional Telegram ones — so they are
pinned here rather than left to be noticed in production. `HEALTH_PORT` is pinned
for a second reason: the stack does NOT pass it, so the default in this file is
what production actually listens on, and it has to keep matching the port the
Dockerfile's HEALTHCHECK curls.
"""

import traceback

import pytest
from conftest import long_enough
from pydantic import ValidationError

from src.config_errors import load_settings_or_exit
from src.redact import MIN_SECRET_LENGTH
from src.settings import Settings

REQUIRED_VARS = ("GRIST_SERVER", "GRIST_DOC_ID", "GRIST_API_KEY", "ETHERSCAN_API_KEY")
OPTIONAL_VARS = ("TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID", "TELEGRAM_BOT_API_SERVER",
                 "HEALTH_PORT")

# The three credentials, and the two that are not. The split is what `Secret` /
# `OptionalSecret` vs `Required` / `OptionalStripped` encodes in src/settings.py,
# and several tests below turn on it. `TELEGRAM_BOT_TOKEN` is in the first list
# despite being optional: optional says it may be ABSENT, not that it may be too
# short to redact when it is there.
CREDENTIAL_VARS = ("GRIST_API_KEY", "ETHERSCAN_API_KEY", "TELEGRAM_BOT_TOKEN")
ADDRESS_VARS = ("GRIST_SERVER", "GRIST_DOC_ID")

# Padded from `MIN_SECRET_LENGTH` rather than written out at a length that happens
# to clear it today: raising the constant must fail the tests that are ABOUT the
# floor, and only those. See `long_enough` in tests/conftest.py.
GRIST_KEY = long_enough("grist-key-1")
ETHERSCAN_KEY = long_enough("etherscan-key-1")
# A stand-in for a real bot token, which is `<numeric id>:<secret>`.
BOT_TOKEN = long_enough("123456789:AA-not-a-real-bot-token")


def _fill_required(monkeypatch):
    monkeypatch.setenv("GRIST_SERVER", "http://grist.invalid")
    monkeypatch.setenv("GRIST_DOC_ID", "doc-1")
    # The two keys carry `Secret`, i.e. a minimum length taken from
    # `MIN_SECRET_LENGTH`, so the placeholders here have to clear it — otherwise
    # every test that merely fills the environment would fail for a reason it is
    # not about.
    monkeypatch.setenv("GRIST_API_KEY", GRIST_KEY)
    monkeypatch.setenv("ETHERSCAN_API_KEY", ETHERSCAN_KEY)


def _clear_optional(monkeypatch):
    for name in OPTIONAL_VARS:
        monkeypatch.delenv(name, raising=False)


def _line_about(err, name):
    """The single reported line naming `name`, so a note can be pinned TO it.

    Insisting on exactly one is part of the assertion: an explanation that lands
    on the right line and also on another one is the block-wide behaviour these
    tests exist to exclude.
    """
    lines = [line for line in err.splitlines() if name in line]
    assert len(lines) == 1, "expected exactly one reported line about {}, got {}".format(
        name, lines)
    return lines[0]


def test_required_variables_map_to_their_env_names(monkeypatch):
    # If a field were renamed, its env var name would change with it and the
    # stack would stop configuring the container.
    _fill_required(monkeypatch)
    _clear_optional(monkeypatch)
    s = Settings(_env_file=None)
    assert s.grist_server == "http://grist.invalid"
    assert s.grist_doc_id == "doc-1"
    assert s.grist_api_key == GRIST_KEY
    assert s.etherscan_api_key == ETHERSCAN_KEY


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
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", BOT_TOKEN)
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "-100")
    monkeypatch.setenv("HEALTH_PORT", "9090")
    s = Settings(_env_file=None)
    assert s.telegram_bot_token == BOT_TOKEN
    # A four-character chat id, accepted: `OptionalStripped` carries no floor. The
    # asymmetry with the token above is deliberate — see the field comments.
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
    # `GRIST_SERVER=` or a lone space/newline must fail at startup, not on the
    # first Grist call; `min_length=1` alone accepts the whitespace cases.
    _fill_required(monkeypatch)
    monkeypatch.setenv(blank, value)
    with pytest.raises(ValidationError):
        Settings(_env_file=None)


@pytest.mark.parametrize("name", CREDENTIAL_VARS)
def test_a_credential_below_the_redaction_floor_is_rejected(monkeypatch, name):
    """The floor is `redact()`'s own, and this is what makes it more than a habit.

    `redact()` leaves anything shorter than `MIN_SECRET_LENGTH` in place, so a
    shorter key would leak. Parametrised over all three credentials, and written
    against the imported constant so the two move together.
    """
    _fill_required(monkeypatch)
    monkeypatch.setenv(name, "x" * (MIN_SECRET_LENGTH - 1))
    with pytest.raises(ValidationError):
        Settings(_env_file=None)
    # The boundary itself is accepted — the guard is "shorter than", not "at most".
    monkeypatch.setenv(name, "x" * MIN_SECRET_LENGTH)
    assert getattr(Settings(_env_file=None), name.lower()) == "x" * MIN_SECRET_LENGTH


@pytest.mark.parametrize("name", ADDRESS_VARS)
def test_the_non_secret_variables_keep_the_shorter_floor(monkeypatch, name):
    # Deliberately NOT on the credential alias: these are addresses, no caller
    # hands them to `redact()`, so the redaction floor has nothing to say about
    # them — and a short Grist doc id is an ordinary one. Putting them on `Secret`
    # for symmetry would reject a working production value for a reason that does
    # not apply to it.
    _fill_required(monkeypatch)
    monkeypatch.setenv(name, "x")
    assert getattr(Settings(_env_file=None), name.lower()) == "x"


def test_padding_is_stripped_off_a_value_that_is_kept(monkeypatch):
    # The other half of the same rule: a variable that is genuinely set but padded
    # (a quoted value with a trailing newline, a copy-paste with a leading space)
    # is accepted, and what reaches the client is the value without the padding —
    # not an address with a space in the middle of the URL it builds.
    _fill_required(monkeypatch)
    monkeypatch.setenv("GRIST_SERVER", "  http://grist.invalid\n")
    assert Settings(_env_file=None).grist_server == "http://grist.invalid"


@pytest.mark.parametrize("pad_left,pad_right", [
    ("", "\n"),          # a here-doc / quoted value in `environment:`
    ("  ", ""),          # a copy-paste that took the leading space
    ("", " \t"),
])
def test_padding_is_stripped_off_the_optional_variables_too(monkeypatch, pad_left, pad_right):
    """The optional pair is stripped exactly like the required four.

    A padded `TELEGRAM_BOT_TOKEN` is percent-encoded into the URL path
    (`/bot<token>%0A/sendMessage`) and silently rejected by the API, losing the
    watchdog's notification.
    """
    _fill_required(monkeypatch)
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", pad_left + BOT_TOKEN + pad_right)
    monkeypatch.setenv("TELEGRAM_CHAT_ID", pad_left + "-100" + pad_right)
    s = Settings(_env_file=None)
    assert s.telegram_bot_token == BOT_TOKEN
    assert s.telegram_chat_id == "-100"


@pytest.mark.parametrize("value", ["", " ", "\n", " \t\n "])
def test_a_blank_optional_variable_reads_as_not_configured(monkeypatch, value):
    """Blank means ABSENT, and this is the half that lets the token carry a floor.

    Not a validation error: a blank `TELEGRAM_BOT_TOKEN=` is a supported
    half-configured deployment. `None` specifically, because the before-validator
    collapses blank to absence before the length floor runs.
    """
    _fill_required(monkeypatch)
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", value)
    monkeypatch.setenv("TELEGRAM_CHAT_ID", value)
    s = Settings(_env_file=None)
    assert s.telegram_bot_token is None
    assert s.telegram_chat_id is None


@pytest.mark.parametrize("value", ["", " "])
def test_a_blank_bot_token_still_lets_the_container_start(capsys, monkeypatch, value):
    # The floor on the token may not cost a startup. Driven through
    # `load_settings_or_exit`, as main.py runs it.
    _fill_required(monkeypatch)
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", value)
    monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)
    s = load_settings_or_exit(lambda: Settings(_env_file=None))
    assert s.telegram_bot_token is None
    assert capsys.readouterr().err == ""


def test_a_bot_token_too_short_to_redact_is_refused_by_name_at_startup(capsys, monkeypatch):
    """Set, but below the floor: the one case that must NOT be read as "absent".

    `raise_for_status()` quotes the URL, the token is a path segment of it, and
    `redact()` skips anything below `MIN_SECRET_LENGTH`. The error must name the
    variable without quoting the value.
    """
    short = "x" * (MIN_SECRET_LENGTH - 1)
    _fill_required(monkeypatch)
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", short)
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "-100")
    with pytest.raises(SystemExit) as exc_info:
        load_settings_or_exit(lambda: Settings(_env_file=None))
    assert exc_info.value.code == 1
    err = capsys.readouterr().err
    assert "TELEGRAM_BOT_TOKEN" in err
    assert "Traceback" not in err


def test_a_real_length_bot_token_is_accepted(monkeypatch):
    # The third leg: blank is absent, short is refused, and an ordinary token goes
    # through untouched. Without this the two above would be satisfied by a field
    # that rejected everything.
    _fill_required(monkeypatch)
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", BOT_TOKEN)
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "-1001234567890")
    s = load_settings_or_exit(lambda: Settings(_env_file=None))
    assert s.telegram_bot_token == BOT_TOKEN
    assert s.telegram_chat_id == "-1001234567890"


def test_the_chat_id_carries_no_length_floor(monkeypatch):
    # Deliberately not symmetrical with the token beside it. The chat id is the
    # ADDRESSEE, it is handed to `redact()` nowhere in this repository, and a real
    # one is a short signed integer — a floor would reject a working production
    # value for a reason that does not apply to it. Written against the constant
    # so it keeps testing "below the floor" if the floor moves.
    _fill_required(monkeypatch)
    short = "-" + "1" * (MIN_SECRET_LENGTH - 2)
    assert len(short) < MIN_SECRET_LENGTH
    monkeypatch.setenv("TELEGRAM_CHAT_ID", short)
    assert Settings(_env_file=None).telegram_chat_id == short


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
    # And NOT annotated with the stripping note. This value is empty exactly as
    # typed, so the note's opening claim — "the value is not empty as typed" —
    # would be flatly false about it, underneath a message ("String should have at
    # least 1 character") that is for once literally accurate. Same contradiction
    # as the one on HEALTH_PORT above, arriving through the other half of the
    # condition: this failure IS the `min_length` that stripping is blamed for
    # elsewhere, and only the emptiness guard keeps the explanation off it.
    assert "whitespace" not in err


def test_a_whitespace_value_is_explained_rather_than_called_empty(capsys, monkeypatch):
    # `GRIST_SERVER=" "` is reported by pydantic as "String should have at least 1
    # character", which is literally false about what the operator typed — the
    # value in the `environment:` block is plainly not empty. The stripping is what
    # makes the claim true, so the message has to say that the stripping happened;
    # otherwise the reader is left comparing a correct-looking line against an
    # error that appears to describe a different variable.
    _fill_required(monkeypatch)
    monkeypatch.setenv("GRIST_SERVER", " ")
    with pytest.raises(SystemExit):
        load_settings_or_exit(lambda: Settings(_env_file=None))
    err = capsys.readouterr().err
    assert "GRIST_SERVER" in err
    # On the line about GRIST_SERVER, not merely somewhere in the output: the note
    # is an annotation on one entry and it has to be readable as one.
    assert "whitespace" in _line_about(err, "GRIST_SERVER")


@pytest.mark.parametrize("value", ["notanumber", "  "])
def test_a_parse_error_is_not_annotated_with_the_stripping_note(capsys, monkeypatch, value):
    """`HEALTH_PORT` fails to PARSE, and nothing about that was decided by stripping.

    The second value, `HEALTH_PORT="  "`, is whitespace-only by shape, so it
    tells a kind-based condition from a shape-based one: neither value may carry
    the note.
    """
    _fill_required(monkeypatch)
    monkeypatch.setenv("HEALTH_PORT", value)
    with pytest.raises(SystemExit):
        load_settings_or_exit(lambda: Settings(_env_file=None))
    err = capsys.readouterr().err
    assert "HEALTH_PORT" in err
    assert "whitespace" not in err


def test_the_stripping_note_annotates_only_the_value_it_is_about(capsys, monkeypatch):
    """The MIXED failure, which is the case the per-entry note exists for.

    With a single error a per-entry note and a per-block one look the same; with
    `GRIST_SERVER=" "` next to `HEALTH_PORT=notanumber` the note must sit on the
    GRIST_SERVER line only.
    """
    _fill_required(monkeypatch)
    monkeypatch.setenv("GRIST_SERVER", " ")
    monkeypatch.setenv("HEALTH_PORT", "notanumber")
    with pytest.raises(SystemExit):
        load_settings_or_exit(lambda: Settings(_env_file=None))
    err = capsys.readouterr().err
    # Both failures are reported — an operator fixing one at a time is the other
    # half of what makes the mixed case ordinary.
    assert "whitespace" in _line_about(err, "GRIST_SERVER")
    assert "whitespace" not in _line_about(err, "HEALTH_PORT")


def test_the_exit_does_not_carry_the_raw_value_out_in_a_chained_traceback(capsys, monkeypatch):
    """The `from None` on the `SystemExit`, and why this handler needs it most.

    The ValidationError context renders rejected fields as
    `input_value=<raw value>`, and this generic function cannot redact. Asserts
    against the rendered chain and against stderr, which must name the variable
    without quoting its value.
    """
    # Bound to a name: a traceback quotes each frame's source line, so a literal
    # on the call line would appear in the render on its own.
    raw = "9f3ab2"
    assert len(raw) < MIN_SECRET_LENGTH, "must be rejected for its LENGTH, not its shape"
    _fill_required(monkeypatch)
    monkeypatch.setenv("ETHERSCAN_API_KEY", raw)

    with pytest.raises(SystemExit) as excinfo:
        load_settings_or_exit(lambda: Settings(_env_file=None))

    error = excinfo.value
    rendered = "".join(traceback.format_exception(type(error), error, error.__traceback__))
    assert raw not in rendered
    err = capsys.readouterr().err
    # The operator still gets everything they need: the variable, and what is
    # wrong with it — just not the value itself.
    assert "ETHERSCAN_API_KEY" in err
    assert raw not in err


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
