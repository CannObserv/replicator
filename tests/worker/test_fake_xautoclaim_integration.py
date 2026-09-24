"""The ``fake_redis`` fixture's ``XAUTOCLAIM`` against the real server (#109).

``claim_once`` follows ``XAUTOCLAIM``'s cursor, and fakeredis 2.37.0 does not
model it — so ``tests/conftest.py`` rebuilds the command from ``XPENDING`` and
``XCLAIM``, and the whole default suite's recovery tests stand on that rebuild.
This is what keeps it honest: each scenario is set up identically on both
servers and must get the same reply, ids compared by their position in the
stream rather than their value, since the two servers mint different ones.

The scenarios are the parts of the reply ``claim_once`` branches on: the cursor
at the end of the list (``0-0``), the cursor after a claim, the attempt budget
spent with nothing claimed, a trimmed entry reported in ``deleted`` whatever its
idle time (and spending ``count`` as a claim would), and the exclusive start a
walk resumes from.
"""

import asyncio
from dataclasses import dataclass, field

import pytest

pytestmark = pytest.mark.integration

GROUP = "replicator.itest"

# The window the scenarios claim against, and how long an "old" entry is left to
# age past it. The gap between them is the margin a busy VM's scheduler gets.
MIN_IDLE_MS = 100
AGE_SECONDS = 0.25


@dataclass(frozen=True)
class Scenario:
    """A PEL of ``size`` entries on a dead consumer, then one ``XAUTOCLAIM``.

    ``young`` are re-claimed by another consumer after the aging sleep, so their
    idle clock is fresh; ``trimmed`` are ``XDEL``-ed out of the stream while still
    pending. ``start`` is a position in the stream, or ``None`` for ``0-0``, or
    ``"past"`` for an id beyond every entry; ``exclusive`` starts just past it,
    the ``(<id>`` form ``claim_once`` resumes a walk with.
    """

    size: int
    count: int
    young: frozenset[int] = field(default_factory=frozenset)
    trimmed: frozenset[int] = field(default_factory=frozenset)
    start: int | str | None = None
    exclusive: bool = False


SCENARIOS = {
    "claims-the-head-and-points-at-the-next": Scenario(size=3, count=1),
    "claims-the-last-and-reports-the-end": Scenario(size=3, count=1, start=2),
    "spends-the-budget-on-young-entries": Scenario(size=13, count=1, young=frozenset(range(12))),
    "passes-young-entries-to-claim-an-old-one": Scenario(size=5, count=1, young=frozenset({0, 1})),
    "reports-a-trimmed-entry-and-claims-past-it": Scenario(
        size=3, count=1, trimmed=frozenset({1}), start=1
    ),
    "reports-a-trimmed-entry-however-young": Scenario(
        size=3, count=1, young=frozenset({0, 1}), trimmed=frozenset({1})
    ),
    "spends-count-on-a-trimmed-entry": Scenario(size=3, count=2, trimmed=frozenset({0})),
    "claims-a-batch": Scenario(size=4, count=2, young=frozenset({1})),
    "starts-just-past-an-entry": Scenario(size=3, count=1, start=0, exclusive=True),
    "starts-just-past-the-last-entry": Scenario(size=2, count=1, start=1, exclusive=True),
    "starts-past-everything": Scenario(size=2, count=1, start="past"),
}


async def reply_for(client, topic: str, scenario: Scenario):
    """Build ``scenario`` on ``client`` and return its reply in stream positions."""
    await client.xgroup_create(topic, GROUP, id="0", mkstream=True)
    ids = [await client.xadd(topic, {"n": str(n)}) for n in range(scenario.size)]
    await client.xreadgroup(GROUP, "replicator@dead", {topic: ">"}, count=scenario.size)
    await asyncio.sleep(AGE_SECONDS)
    if scenario.young:
        await client.xclaim(topic, GROUP, "replicator@busy", 0, [ids[n] for n in scenario.young])
    if scenario.trimmed:
        await client.xdel(topic, *(ids[n] for n in scenario.trimmed))
    if scenario.start is None:
        start = "0-0"
    elif scenario.start == "past":
        start = f"{int(ids[-1].split(b'-')[0]) + 1}-0"
    else:
        start = ids[int(scenario.start)].decode()
    if scenario.exclusive:
        start = f"({start}"

    cursor, claimed, deleted = await client.xautoclaim(
        topic,
        GROUP,
        "replicator@me",
        min_idle_time=MIN_IDLE_MS,
        start_id=start,
        count=scenario.count,
    )
    position = {entry_id: n for n, entry_id in enumerate(ids)}
    return (
        "0-0" if cursor in (b"0-0", "0-0") else position[cursor],
        [position[entry_id] for entry_id, _fields in claimed],
        [position[entry_id] for entry_id in deleted],
    )


@pytest.mark.parametrize("scenario", SCENARIOS.values(), ids=SCENARIOS.keys())
async def test_the_fake_answers_as_the_real_server_does(
    real_redis, fake_redis, scratch_topic, scenario
):
    assert await reply_for(fake_redis, scratch_topic, scenario) == await reply_for(
        real_redis, scratch_topic, scenario
    )
