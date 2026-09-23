"""`scripts/notify_failure.sh` behaves correctly, driven as a process (#94).

Split from `tests/test_deploy.py`, which owns the *wiring* — that the unit sets
`OnFailure=`, that it names the template, that the template runs this script.
This file owns the script's own behaviour, reached by a different mechanism
(`subprocess` against a stub HTTP server rather than a regex over an ini file),
per the same split `tests/test_check_main_checkout.py` makes for the checkout
guard.

**The contract under test is "never make the outage worse".** This script runs
only when `replicator.service` has already failed, so every branch exits 0 and
every failure of its own is reported in the record it was going to write anyway.
A notifier that is down, slow, unauthenticated or unconfigured must still leave a
journal line naming the unit that failed — that line is the floor the whole
handler guarantees, and the POST is the part that may not arrive.
"""

import glob
import json
import os
import re
import subprocess
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, HTTPServer, ThreadingHTTPServer
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
NOTIFY = REPO_ROOT / "scripts" / "notify_failure.sh"

UNIT_NAME = "replicator.service"

# Every variable the script reads. Scrubbed from each invocation below so a
# developer who sourced /etc/replicator/.env into their shell cannot turn an
# "unconfigured" assertion green by having configured it — the same reasoning
# tests/test_check_main_checkout.py applies to REPLICATOR_ALLOW_ANY_CHECKOUT.
NOTIFY_VARS = (
    "REPLICATOR_NOTIFY_URL",
    "REPLICATOR_NOTIFY_TOKEN",
    "REPLICATOR_NOTIFY_TIMEOUT_SECONDS",
    "REPLICATOR_NOTIFY_MODE",
    "REPLICATOR_NOTIFY_TEMPLATE_ID",
    "REPLICATOR_NOTIFY_CHANNEL_IDS",
    # systemd ≥251 sets these on an OnFailure= handler; a test run from inside
    # one must not inherit them.
    "MONITOR_INVOCATION_ID",
    "MONITOR_UNIT",
)


def _run(*args: str, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    """Run the real script with a scrubbed environment."""
    clean = {k: v for k, v in os.environ.items() if k not in NOTIFY_VARS}
    clean.update(env or {})
    return subprocess.run(
        ["bash", str(NOTIFY), *args],
        capture_output=True,
        text=True,
        env=clean,
        timeout=30,
    )


def _records(result: subprocess.CompletedProcess[str]) -> list[dict]:
    """Every JSON object the script emitted, across both streams.

    Which stream carries the record is not the contract — that it is machine
    readable and names the unit is. Non-JSON lines are ignored rather than
    asserted against, so a future human-readable line cannot fail these tests.
    """
    found = []
    for stream in (result.stdout, result.stderr):
        for line in stream.splitlines():
            line = line.strip()
            if not line.startswith("{"):
                continue
            try:
                found.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return found


class _Stub(BaseHTTPRequestHandler):
    """Records one POST body, answers with whatever status the test asked for."""

    status = 202
    reply = b"{}"
    received: list[dict] = []

    def do_POST(self):  # noqa: N802 - BaseHTTPRequestHandler's spelling
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length).decode()
        try:
            parsed = json.loads(body)
        except json.JSONDecodeError:
            parsed = {"_raw": body}
        type(self).received.append(
            {
                "body": parsed,
                "auth": self.headers.get("Authorization"),
                "api_key": self.headers.get("X-API-Key"),
            },
        )
        self.send_response(type(self).status)
        self.end_headers()
        self.wfile.write(type(self).reply)

    def log_message(self, *args):
        """Silence the default stderr access log — pytest captures it as noise."""


@pytest.fixture
def notifier():
    """A stub notifier on a loopback port of its own.

    Never the real `http://notifier:9000`: this suite must not dispatch
    notifications to the cohort's live service, and must pass on a machine with
    no tailnet at all.
    """
    _Stub.received = []
    _Stub.status = 202
    _Stub.reply = b"{}"
    server = HTTPServer(("127.0.0.1", 0), _Stub)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server, _Stub
    finally:
        server.shutdown()
        server.server_close()


class _HungStub(BaseHTTPRequestHandler):
    """Accepts the connection and never answers, until the client gives up."""

    def do_POST(self):  # noqa: N802 - BaseHTTPRequestHandler's spelling
        self.rfile.read(int(self.headers.get("Content-Length", 0)))
        # Comfortably past any timeout under test; the client hangs up first.
        time.sleep(10)

    def log_message(self, *args):
        """Silence the default stderr access log — pytest captures it as noise."""


class _HungServer(ThreadingHTTPServer):
    """Threaded and non-blocking on close, or teardown waits out the stall itself.

    On a plain `HTTPServer` the sleeping handler holds the single serve loop, so
    `shutdown()` blocks until it returns and this one test cost the suite the
    full stall. Threaded, with the abandoned handler left as a daemon.
    """

    daemon_threads = True
    block_on_close = False


@pytest.fixture
def hung_notifier():
    """A stub that accepts and then stalls — the shape a degraded tailnet produces.

    Distinct from `notifier`: a refused connection fails fast and never reaches
    the timeout, so only a stub that answers nothing exercises the ceiling.
    """
    server = _HungServer(("127.0.0.1", 0), _HungStub)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()


def _url(server: HTTPServer) -> str:
    host, port = server.server_address[:2]
    return f"http://{host}:{port}/v1/notifications"


# The floor: a record naming the failed unit, on every path, always exit 0.


def test_an_unconfigured_notifier_still_records_the_failure():
    """The journal line is the guarantee; the POST is the part that may not arrive."""
    result = _run(UNIT_NAME)

    assert result.returncode == 0, result.stderr
    records = _records(result)
    assert records, f"no JSON record emitted; stderr={result.stderr!r}"
    assert any(r.get("unit") == UNIT_NAME for r in records), records


def test_the_record_is_marked_critical():
    """Severity has to be in the record, or a journal filter cannot find it."""
    result = _run(UNIT_NAME)

    assert any(r.get("level") == "CRITICAL" for r in _records(result)), _records(result)


def test_a_missing_unit_argument_still_exits_zero():
    """A handler invoked wrongly must not add a second failed unit to the incident."""
    result = _run()

    assert result.returncode == 0, result.stderr
    assert _records(result), "a misinvocation still has to leave a trace"


# The POST, when one is configured.


def test_a_configured_notifier_receives_the_failed_unit(notifier):
    server, stub = notifier

    result = _run(UNIT_NAME, env={"REPLICATOR_NOTIFY_URL": _url(server)})

    assert result.returncode == 0, result.stderr
    assert len(stub.received) == 1, stub.received
    assert UNIT_NAME in json.dumps(stub.received[0]["body"]), stub.received[0]


def test_a_configured_token_is_sent_as_a_bearer(notifier):
    server, stub = notifier

    _run(
        UNIT_NAME,
        env={"REPLICATOR_NOTIFY_URL": _url(server), "REPLICATOR_NOTIFY_TOKEN": "s3cret"},
    )

    assert stub.received[0]["auth"] == "Bearer s3cret", stub.received[0]


def test_no_authorization_header_is_sent_without_a_token(notifier):
    """An empty bearer is worse than none — it reads as a configured credential."""
    server, stub = notifier

    _run(UNIT_NAME, env={"REPLICATOR_NOTIFY_URL": _url(server)})

    assert stub.received[0]["auth"] is None, stub.received[0]


# Every way the POST can fail, none of which may fail the handler.


def test_an_unreachable_notifier_is_not_fatal():
    """Broker outages and notifier outages correlate — this is the likely case, not the edge."""
    # Port 1 on loopback: reliably closed, and refused immediately rather than
    # timing out, so this asserts the failure branch without spending the timeout.
    result = _run(UNIT_NAME, env={"REPLICATOR_NOTIFY_URL": "http://127.0.0.1:1/v1/notifications"})

    assert result.returncode == 0, result.stderr
    assert any(r.get("unit") == UNIT_NAME for r in _records(result)), _records(result)


def test_a_notifier_error_status_is_not_fatal(notifier):
    server, stub = notifier
    stub.status = 500

    result = _run(UNIT_NAME, env={"REPLICATOR_NOTIFY_URL": _url(server)})

    assert result.returncode == 0, result.stderr


def test_a_failed_dispatch_is_itself_recorded():
    """Silent loss of the notification would recreate the gap #94 is about."""
    result = _run(UNIT_NAME, env={"REPLICATOR_NOTIFY_URL": "http://127.0.0.1:1/v1/notifications"})

    combined = result.stdout + result.stderr
    assert "notify" in combined.lower(), combined
    assert any(
        "fail" in json.dumps(r).lower() or r.get("notify_dispatched") is False
        for r in _records(result)
    ), _records(result)


def test_a_malformed_url_is_not_fatal():
    result = _run(UNIT_NAME, env={"REPLICATOR_NOTIFY_URL": "not-a-url"})

    assert result.returncode == 0, result.stderr


def test_the_dispatch_is_time_bounded(hung_notifier):
    """A hung notifier must not hold the handler open indefinitely.

    systemd gives the handler its own start timeout; blowing through it would
    turn the notification into a second failed unit — and a hung notifier is the
    expected case here, since the outages that fire this handler are the ones
    that degrade the tailnet both VMs sit on.

    Pointed at a stub that never answers (CR 7). It previously used the
    *responsive* stub, so it spent no time at the timeout and asserted nothing
    the other tests did not already cover.
    """
    server = hung_notifier
    budget = 3

    start = time.monotonic()
    result = _run(
        UNIT_NAME,
        env={
            "REPLICATOR_NOTIFY_URL": _url(server),
            "REPLICATOR_NOTIFY_TIMEOUT_SECONDS": "1",
        },
    )
    elapsed = time.monotonic() - start

    assert result.returncode == 0, result.stderr
    assert elapsed < budget, f"took {elapsed:.1f}s against a 1s ceiling — not bounded"
    # 28 is curl's timeout code; asserting it rules out the test passing because
    # the connection was refused rather than because the ceiling was enforced.
    assert any(r.get("curl_exit") == 28 for r in _records(result)), _records(result)


def test_a_timeout_above_the_units_own_ceiling_is_refused():
    """A dispatch ceiling above `TimeoutStartSec=` would be enforced by systemd instead.

    And systemd enforces it by killing the handler, which is the "notification
    becomes a second failed unit" outcome the whole gap exists to prevent. CR 4
    validated the format and left the ceiling open (CR 12).
    """
    result = _run(UNIT_NAME, env={"REPLICATOR_NOTIFY_TIMEOUT_SECONDS": "99999"})

    assert result.returncode == 0, result.stderr
    assert "99999" in result.stderr, result.stderr
    assert "ceiling" in result.stderr.lower(), result.stderr


def test_the_unit_ceiling_and_the_scripts_cap_are_one_decision():
    """The script's cap is meaningless if the unit's own timeout drops below it."""
    unit = (REPO_ROOT / "deploy" / "replicator-failure-notify@.service").read_text()
    timeout_start = int(re.search(r"^TimeoutStartSec=(\d+)$", unit, flags=re.MULTILINE).group(1))
    cap = int(re.search(r"^TIMEOUT_MAX=(\d+)$", NOTIFY.read_text(), flags=re.MULTILINE).group(1))

    assert cap < timeout_start, (
        f"the script caps dispatch at {cap}s but the unit kills it at {timeout_start}s"
    )


def test_a_bad_timeout_is_named_and_defaulted():
    """CR 4's behaviour, which shipped with no test of its own (CR 13)."""
    result = _run(UNIT_NAME, env={"REPLICATOR_NOTIFY_TIMEOUT_SECONDS": "abc"})

    assert result.returncode == 0, result.stderr
    assert "abc" in result.stderr, result.stderr
    assert "not a positive integer" in result.stderr, result.stderr


def test_a_failed_dispatch_records_why(notifier):
    """CR 2's reason mapping, untested until now (CR 13)."""
    result = _run(UNIT_NAME, env={"REPLICATOR_NOTIFY_URL": "http://127.0.0.1:1/v1/notifications"})

    reasons = [r.get("reason") for r in _records(result) if r.get("reason")]
    assert reasons, _records(result)
    assert "refused" in reasons[0].lower(), reasons


def test_both_dispatch_records_name_the_host_and_build(notifier):
    """CR 9's behaviour: each record attributes itself without correlation (CR 13)."""
    server, _ = notifier

    result = _run(
        UNIT_NAME,
        env={"REPLICATOR_NOTIFY_URL": _url(server), "BUILD_ID": "deadbee"},
    )

    dispatched = [r for r in _records(result) if "notify_dispatched" in r]
    assert dispatched, _records(result)
    for record in dispatched:
        assert record.get("host"), record
        assert record.get("build") == "deadbee", record


@pytest.mark.parametrize("mode", ["webhook", "notifier"])
def test_the_token_never_reaches_the_curl_command_line(notifier, mode):
    """CR 5's security property, proven once by hand and by nothing repeatable (CR 13).

    Parametrized over both modes (#108): notifier mode sends the same token as
    `X-API-Key`, and a second header path is a second place for it to leak.

    Reads the argv of every process on the box while the dispatch is in flight.
    The stub stalls so there is a window to look in; without one the check races
    the request and passes for the wrong reason.
    """
    secret = "TOKEN" + uuid.uuid4().hex

    server = _HungServer(("127.0.0.1", 0), _HungStub)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        clean = {k: v for k, v in os.environ.items() if k not in NOTIFY_VARS}
        clean.update(
            {
                "REPLICATOR_NOTIFY_URL": _url(server),
                "REPLICATOR_NOTIFY_TOKEN": secret,
                "REPLICATOR_NOTIFY_TIMEOUT_SECONDS": "5",
                **(NOTIFIER_MODE if mode == "notifier" else {}),
            }
        )
        proc = subprocess.Popen(
            ["bash", str(NOTIFY), UNIT_NAME],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env=clean,
        )
        try:
            # Scan the WHOLE window rather than stopping at the first curl. An
            # earlier version stopped as soon as it saw `b"curl"` anywhere in any
            # argv, which matched an unrelated process on the first pass — before
            # the dispatch had spawned — so it exited having looked at nothing.
            # It passed with the token back on argv, proving only that it ran.
            deadline = time.monotonic() + 3
            saw_curl = False
            leaked: list[str] = []
            while time.monotonic() < deadline:
                for cmdline in glob.glob("/proc/[0-9]*/cmdline"):
                    try:
                        argv = Path(cmdline).read_bytes()
                    except OSError:
                        continue
                    # argv[0]'s basename, not a substring anywhere in the line:
                    # any process whose arguments merely mention curl would do.
                    argv0 = argv.split(b"\0", 1)[0]
                    if os.path.basename(argv0.decode(errors="replace")) == "curl":
                        saw_curl = True
                    if secret.encode() in argv:
                        leaked.append(cmdline)
                time.sleep(0.05)

            assert saw_curl, "never caught curl in flight — the check proved nothing"
            assert not leaked, f"the token is in the argv of: {leaked[:3]}"
        finally:
            proc.kill()
            proc.wait()
    finally:
        server.shutdown()
        server.server_close()


def test_a_token_carrying_a_newline_is_refused_not_truncated(notifier):
    """curl's config is line-oriented, so a newline silently shortens the credential (CR 14).

    Measured rather than assumed: curl does **not** treat the remainder as a new
    directive — an injected `user = "…"` line is ignored and no Basic auth is
    sent. What it does is end the quoted value at the newline and dispatch
    `Bearer tok`, exit 0. So the risk is not injection but a silently wrong
    credential, whose 401 reads as the notifier's fault rather than the token's —
    the same masquerade CR 4 removed for the timeout.
    """
    server, stub = notifier

    result = _run(
        UNIT_NAME,
        env={
            "REPLICATOR_NOTIFY_URL": _url(server),
            "REPLICATOR_NOTIFY_TOKEN": "tok\ntrailing",
        },
    )

    assert result.returncode == 0, result.stderr
    assert "REPLICATOR_NOTIFY_TOKEN" in result.stderr, result.stderr
    assert "newline" in result.stderr.lower(), result.stderr
    # Refused means unsent, not sent-truncated.
    if stub.received:
        assert stub.received[0]["auth"] != "Bearer tok", stub.received[0]


def test_the_script_never_reads_an_env_file_itself():
    """AGENTS.md's env boundary: the repo `.env` holds org-wide PATs the handler must never load.

    Asserted unconditionally (CR 6). The previous form led with
    ``"/etc/replicator/.env" not in text or …``, and since the script names no env
    file at all the left side was always true — so the check that mattered never
    ran. Config reaches this script only as environment systemd already exported,
    which is what keeps the choice of file in the unit where the boundary is
    documented.
    """
    text = NOTIFY.read_text()
    # Comments are prose, and prose is full of ". " — the loader check below has to
    # read code only, or it matches the end of an ordinary sentence.
    code = "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("#"))

    assert ".env" not in text, "no env file belongs in this script — the unit chooses it"
    for loader in (". ", "source ", "set -a"):
        assert loader not in code, f"the handler loads an env file itself via {loader!r}"


# Notifier mode (#108): the cohort notifier's /dispatch shape, agreed on
# CannObserv/notifier#70 and pinned against its live openapi.json
# (DispatchRequest / DispatchOut).

TEMPLATE_ID = "01M37PFEYQFJ5GP82BRM370X1A"
CHANNEL_A = "01M37PFEYQFJ5GP82BRM370X1R"
CHANNEL_B = "01M37PFEYRHRNK3ZDK242F657N"
INVOCATION = "6029eb88c19c47dabe713c9fbe2e5c78"

NOTIFIER_MODE = {
    "REPLICATOR_NOTIFY_MODE": "notifier",
    "REPLICATOR_NOTIFY_TEMPLATE_ID": TEMPLATE_ID,
    "REPLICATOR_NOTIFY_CHANNEL_IDS": f"{CHANNEL_A}, {CHANNEL_B}",
}

# The seven incident fields — the template's variables_schema requires exactly
# these, so the script and deploy/notifier-template.json must agree on them.
INCIDENT_FIELDS = {"level", "event", "unit", "host", "build", "message", "timestamp"}


def _dispatch_out(status: str) -> bytes:
    """A DispatchOut body, attempts included — their own `status` must not be read."""
    return json.dumps(
        {
            "id": "01M37Q0000000000000000000A",
            "tenant_id": "01M37NQ4YP99CWYHB8M06C4STD",
            "template_id": TEMPLATE_ID,
            "idempotency_key": None,
            "rendered_title": "t",
            "rendered_body": "b",
            "status": status,
            "metadata": {},
            "created_at": "2026-09-23T18:00:00Z",
            "attempts": [{"channel_id": CHANNEL_A, "status": "succeeded"}],
        }
    ).encode()


def _notifier_env(server: HTTPServer, **extra: str) -> dict[str, str]:
    return {"REPLICATOR_NOTIFY_URL": _url(server), **NOTIFIER_MODE, **extra}


def test_notifier_mode_sends_the_dispatch_shape(notifier):
    server, stub = notifier
    stub.reply = _dispatch_out("succeeded")

    result = _run(UNIT_NAME, env=_notifier_env(server))

    assert result.returncode == 0, result.stderr
    body = stub.received[0]["body"]
    assert body["template_id"] == TEMPLATE_ID, body
    assert body["channel_ids"] == [CHANNEL_A, CHANNEL_B], body
    assert set(body["variables"]) == INCIDENT_FIELDS, body
    assert body["variables"]["unit"] == UNIT_NAME, body
    assert body["metadata"] == {"event": "unit_failed"}, body


def test_notifier_mode_sends_the_token_as_an_api_key_not_a_bearer(notifier):
    server, stub = notifier
    stub.reply = _dispatch_out("succeeded")

    _run(UNIT_NAME, env=_notifier_env(server, REPLICATOR_NOTIFY_TOKEN="nk_s3cret"))

    assert stub.received[0]["api_key"] == "nk_s3cret", stub.received[0]
    assert stub.received[0]["auth"] is None, stub.received[0]


def test_the_idempotency_key_is_the_failed_invocation(notifier):
    """systemd ≥251 hands an OnFailure= handler the failed run's InvocationID."""
    server, stub = notifier
    stub.reply = _dispatch_out("succeeded")

    _run(UNIT_NAME, env=_notifier_env(server, MONITOR_INVOCATION_ID=INVOCATION))

    assert stub.received[0]["body"]["idempotency_key"] == f"{UNIT_NAME}:{INVOCATION}"


def test_no_invocation_id_means_no_idempotency_key(notifier):
    """A fabricated key could collide across failures and swallow a real alert.

    Notifier returns the prior record for a replayed key and makes no new
    delivery attempt, so null is the only safe value when systemd gave us none.
    """
    server, stub = notifier
    stub.reply = _dispatch_out("succeeded")

    _run(UNIT_NAME, env=_notifier_env(server))

    assert stub.received[0]["body"].get("idempotency_key") is None, stub.received[0]


@pytest.mark.parametrize(
    ("status", "level"),
    [("succeeded", "INFO"), ("partial", "WARNING"), ("failed", "ERROR")],
)
def test_delivery_is_scored_on_the_body_not_the_202(notifier, status, level):
    """Notifier answers 202 for all three; only the body says whether anyone was told."""
    server, stub = notifier
    stub.reply = _dispatch_out(status)

    result = _run(UNIT_NAME, env=_notifier_env(server))

    assert result.returncode == 0, result.stderr
    scored = [r for r in _records(result) if "delivery_status" in r]
    assert scored, _records(result)
    assert scored[0]["delivery_status"] == status, scored
    assert scored[0]["level"] == level, scored


def test_an_unreadable_202_body_is_not_scored_as_delivered(notifier):
    server, stub = notifier
    stub.reply = b"not json"

    result = _run(UNIT_NAME, env=_notifier_env(server))

    assert result.returncode == 0, result.stderr
    scored = [r for r in _records(result) if "delivery_status" in r]
    assert scored and scored[0]["delivery_status"] == "unknown", _records(result)
    assert scored[0]["level"] != "INFO", scored


@pytest.mark.parametrize(
    "missing", ["REPLICATOR_NOTIFY_TEMPLATE_ID", "REPLICATOR_NOTIFY_CHANNEL_IDS"]
)
def test_incomplete_notifier_config_records_and_does_not_dispatch(notifier, missing):
    """A request notifier would 422 is not worth sending; the reason is worth logging."""
    server, stub = notifier
    env = _notifier_env(server)
    del env[missing]

    result = _run(UNIT_NAME, env=env)

    assert result.returncode == 0, result.stderr
    assert stub.received == [], stub.received
    assert missing in result.stderr, result.stderr
    assert any(r.get("unit") == UNIT_NAME for r in _records(result)), _records(result)


def test_an_unknown_mode_is_named_and_does_not_dispatch(notifier):
    """A typo'd mode must not send the flat payload to a /dispatch endpoint."""
    server, stub = notifier

    result = _run(UNIT_NAME, env=_notifier_env(server, REPLICATOR_NOTIFY_MODE="notifer"))

    assert result.returncode == 0, result.stderr
    assert stub.received == [], stub.received
    assert "notifer" in result.stderr, result.stderr


def test_webhook_mode_is_still_the_default(notifier):
    server, stub = notifier

    _run(UNIT_NAME, env={"REPLICATOR_NOTIFY_URL": _url(server)})

    assert set(stub.received[0]["body"]) == INCIDENT_FIELDS, stub.received[0]


# The template notifier stores (deploy/notifier-template.json). The operator
# POSTs it once; its variables_schema has to accept what the script sends.

TEMPLATE = REPO_ROOT / "deploy" / "notifier-template.json"


def test_the_template_requires_exactly_the_incident_fields():
    schema = json.loads(TEMPLATE.read_text())["variables_schema"]

    assert set(schema["required"]) == INCIDENT_FIELDS, schema
    assert all(schema["properties"][f] == {"type": "string"} for f in INCIDENT_FIELDS), schema


def test_the_template_schema_cannot_reject_a_future_field_or_level():
    """On an alert path a 422 loses the alert (notifier#70): no enums, extras allowed."""
    schema = json.loads(TEMPLATE.read_text())["variables_schema"]

    assert schema.get("additionalProperties", True) is not False, schema
    assert "enum" not in json.dumps(schema), schema


def test_the_templates_sample_is_what_the_script_emits():
    """sample_variables must be a record the script really produces, not a hand-written one."""
    template = json.loads(TEMPLATE.read_text())
    record = next(r for r in _records(_run(UNIT_NAME)) if r.get("event") == "unit_failed")

    assert set(template["sample_variables"]) == set(record) == INCIDENT_FIELDS


def test_the_template_carries_only_template_create_fields():
    """Notifier's TemplateCreate; an extra key is silently ignored, so a typo would vanish."""
    template = json.loads(TEMPLATE.read_text())

    assert set(template) == {
        "name",
        "title_template",
        "body_template",
        "variables_schema",
        "sample_variables",
    }, template


# #108 follow-ups, from the 2026-09-23 smoke tests: run 2's 422 was diagnosed from
# config because the body never reached the journal, the template id it carried
# was a pasted `<id from step 1>`, and every alert named the worker's build.


def test_a_rejected_dispatch_logs_an_excerpt_of_the_reply(notifier):
    """Notifier's 422 detail names the field; without it the journal only says 422."""
    server, stub = notifier
    stub.status = 422
    stub.reply = json.dumps(
        {"detail": [{"loc": ["body", "channel_ids"], "msg": "List should have at least 1 item"}]}
    ).encode()

    result = _run(UNIT_NAME, env={"REPLICATOR_NOTIFY_URL": _url(server)})

    failed = [r for r in _records(result) if r.get("notify_dispatched") is False]
    assert failed, _records(result)
    assert "channel_ids" in failed[0].get("response", ""), failed


def test_the_excerpt_is_bounded_and_keeps_the_record_parseable(notifier):
    """An arbitrary webhook body: long, multi-line, quoted, non-ASCII."""
    server, stub = notifier
    stub.status = 500
    stub.reply = ('<html>\n"quoted" \\ back\x01 é ' + "x" * 5000).encode()

    result = _run(UNIT_NAME, env={"REPLICATOR_NOTIFY_URL": _url(server)})

    failed = [r for r in _records(result) if r.get("notify_dispatched") is False]
    assert failed, f"the record did not parse as JSON; stderr={result.stderr!r}"
    excerpt = failed[0]["response"]
    assert 0 < len(excerpt) <= 512, len(excerpt)
    assert excerpt.isascii() and excerpt.isprintable(), repr(excerpt[:80])


def test_an_unconfirmed_delivery_logs_the_reply_it_could_not_read(notifier):
    server, stub = notifier
    stub.reply = b"not json"

    result = _run(UNIT_NAME, env=_notifier_env(server))

    scored = [r for r in _records(result) if r.get("delivery_status") == "unknown"]
    assert scored and scored[0].get("response") == "not json", _records(result)


@pytest.mark.parametrize(
    ("var", "value"),
    [
        ("REPLICATOR_NOTIFY_TEMPLATE_ID", "<id from step 1>"),
        ("REPLICATOR_NOTIFY_CHANNEL_IDS", f"{CHANNEL_A},not-a-ulid"),
    ],
)
def test_a_malformed_id_is_refused_locally_and_named(notifier, var, value):
    """Refused before the request, so a typo costs a journal line, not an alert."""
    server, stub = notifier

    result = _run(UNIT_NAME, env=_notifier_env(server, **{var: value}))

    assert result.returncode == 0, result.stderr
    assert stub.received == [], stub.received
    assert var in result.stderr, result.stderr
    assert "ULID" in result.stderr, result.stderr


def test_a_lowercase_ulid_is_accepted(notifier):
    """Notifier's own pattern accepts either case; this check must not be stricter."""
    server, stub = notifier
    stub.reply = _dispatch_out("succeeded")

    _run(UNIT_NAME, env=_notifier_env(server, REPLICATOR_NOTIFY_TEMPLATE_ID=TEMPLATE_ID.lower()))

    assert len(stub.received) == 1, stub.received


def test_another_units_failure_does_not_carry_the_workers_build():
    """/run/replicator/build-id is the worker's; a smoke test once alerted as `build a85fe5a`."""
    result = _run("notify-smoke-test.service", env={"BUILD_ID": "deadbee"})

    record = next(r for r in _records(result) if r.get("event") == "unit_failed")
    assert record["build"] == "<n/a>", record
    assert "deadbee" not in record["message"], record
    assert "build" not in record["message"], record


def test_the_workers_failure_still_carries_its_build():
    result = _run(UNIT_NAME, env={"BUILD_ID": "deadbee"})

    record = next(r for r in _records(result) if r.get("event") == "unit_failed")
    assert record["build"] == "deadbee", record
    assert "(build deadbee)" in record["message"], record
