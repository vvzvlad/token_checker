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
OPTIONAL_VARS = ("TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID", "HEALTH_PORT")

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


@pytest.mark.parametrize("name", CREDENTIAL_VARS)
def test_a_credential_below_the_redaction_floor_is_rejected(monkeypatch, name):
    """The floor is `redact()`'s own, and this is what makes it more than a habit.

    `redact()` leaves anything shorter than `MIN_SECRET_LENGTH` in the text
    untouched. A key below that is therefore a key that travels into `docker logs`
    and into the wallet's `Comment` cell in full, for the life of the deployment,
    with nothing anywhere reporting it — `min_length=1` accepts it and the
    redaction simply declines. Refusing it at startup is the only moment anybody
    can be told.

    Parametrised over all THREE credentials, `TELEGRAM_BOT_TOKEN` included. It
    used to sit on the bare `Stripped` alias, which has no floor at all, so an
    eight-character bot token was accepted at startup and `raise_for_status()`
    then quoted it back into the log in full — the one credential the rule was
    supposed to cover least well of the three, given that it is the one travelling
    as a path segment of a URL that gets quoted back verbatim.

    Written against the imported constant rather than against 8, because the two
    have to move as one: raising the threshold in src/redact.py must not be able
    to leave a credential stranded underneath it.
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

    It was not, and the consequence is quiet rather than loud, which is what makes
    it worth pinning. An unstripped `TELEGRAM_BOT_TOKEN` goes into an f-string URL
    in `src/watchdog.py` and NOTHING on the way objects: measured against the
    pinned requests 2.32.3 / urllib3 2.2.2, the padding is percent-encoded into
    the path (`/bot<token>%0A/sendMessage`), the request is sent for real, and no
    exception is raised at any layer. What arrives at the API is a token that is
    not the token, so the call comes back rejected rather than delivered.

    So the loss is a notification that never happened, at the one moment it exists
    for: the watchdog is firing and the process is about to `os._exit(1)`.
    Stripping is what stops the padded value from being sent; `src/watchdog.py`
    checking the response is what stops a rejected one from being logged as sent.
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

    Deliberately NOT a validation error, unlike the required four: the optional
    pair is allowed to be missing, and `TELEGRAM_BOT_TOKEN=` left in the stack's
    `environment:` block — with or without a stray space after the `=` — is a
    half-configured deployment this service supports on purpose.

    `None` rather than merely falsy, because that is the mechanism rather than a
    coincidence. The before-validator in src/settings.py collapses blank to
    absence BEFORE any constraint runs, which is the only reason a `min_length`
    can sit on `TELEGRAM_BOT_TOKEN` at all: without it the floor would reject the
    empty value and turn a supported configuration into a container that refuses
    to start — the objection that used to keep the token off the floor entirely.
    """
    _fill_required(monkeypatch)
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", value)
    monkeypatch.setenv("TELEGRAM_CHAT_ID", value)
    s = Settings(_env_file=None)
    assert s.telegram_bot_token is None
    assert s.telegram_chat_id is None


@pytest.mark.parametrize("value", ["", " "])
def test_a_blank_bot_token_still_lets_the_container_start(capsys, monkeypatch, value):
    # The end the previous test is a means to: the floor on the token may not cost
    # a startup. Driven through `load_settings_or_exit`, which is what main.py
    # actually runs, so a regression shows up as the exit(1) it would be in
    # production rather than as a pydantic error object nobody sees.
    _fill_required(monkeypatch)
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", value)
    monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)
    s = load_settings_or_exit(lambda: Settings(_env_file=None))
    assert s.telegram_bot_token is None
    assert capsys.readouterr().err == ""


def test_a_bot_token_too_short_to_redact_is_refused_by_name_at_startup(capsys, monkeypatch):
    """Set, but below the floor: the one case that must NOT be read as "absent".

    This is the defect this alias was changed for. On the bare `Stripped` alias an
    eight-character `TELEGRAM_BOT_TOKEN` started the container happily, and the
    first failed notification put it in the log in full: `raise_for_status()`
    builds its message out of the URL, the token is a path segment of that URL,
    and `redact()` declines to replace anything below `MIN_SECRET_LENGTH`.

    Naming the variable is the point of failing at all — the operator has to know
    WHICH of the six to look at, and nothing may quote the value itself.
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

    The note used to be hung on the whole `Invalid value(s)` block, so the operator
    read an explanation about whitespace directly under an integer-parsing failure —
    an annotation that contradicts the line it annotates is worse than none.

    The second value is the case that moving the note onto its own line does not
    fix, and it is the reason the condition asks what KIND of error this is rather
    than what the input LOOKS like. `HEALTH_PORT="  "` — the same stray space after
    the `=` that the note exists for, one variable over — is whitespace-only by
    shape, so a shape-based test appended the note to "Input should be a valid
    integer": a field with no string constraint, no emptiness check, and nothing
    anywhere that "counts as empty". Both values here must come out unannotated,
    and only the second one can tell the two conditions apart.
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

    One broken `environment:` block rarely breaks exactly one variable, and the
    two single-variable tests above cannot tell a per-entry note from a per-block
    one: with a single error, "somewhere in the output" and "on the right line"
    are the same assertion. Here they are not. The note was computed per error and
    printed once after the whole list, so `GRIST_SERVER=" "` next to
    `HEALTH_PORT=notanumber` put the whitespace explanation underneath the
    integer-parsing failure — the exact contradiction the note was moved off the
    block to avoid, still present in the only shape that shows it.
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

    This `raise` runs inside `except ValidationError`, so python attaches the
    ValidationError as `__context__` — and pydantic renders every rejected field
    as `input_value=<the value exactly as it arrived>`. For a credential that is
    the live key, verbatim, and `load_settings_or_exit` cannot redact it: it is
    generic over the settings class and does not know which fields are secret.

    Nothing prints it today, because CPython renders no traceback for an unhandled
    SystemExit. That is a property of one caller, not of this function, which the
    module docstring offers to every entrypoint — and the first one that catches
    SystemExit and logs it with `exc_info=True` renders the whole chain.

    So this asserts against the rendered chain, which is the thing that leaks, and
    against stderr, which must name the variable without quoting its value.
    """
    # Bound to a name rather than written inline: a traceback quotes the SOURCE
    # LINE of every frame it walks, so a literal on the call line below would show
    # up in the render on its own account and fail this test regardless of what
    # the code does.
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
