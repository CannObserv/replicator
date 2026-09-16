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
    received: list[dict] = []

    def do_POST(self):  # noqa: N802 - BaseHTTPRequestHandler's spelling
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length).decode()
        try:
            parsed = json.loads(body)
        except json.JSONDecodeError:
            parsed = {"_raw": body}
        type(self).received.append(
            {"body": parsed, "auth": self.headers.get("Authorization")},
        )
        self.send_response(type(self).status)
        self.end_headers()
        self.wfile.write(b"{}")

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


def test_the_token_never_reaches_the_curl_command_line(notifier):
    """CR 5's security property, proven once by hand and by nothing repeatable (CR 13).

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
