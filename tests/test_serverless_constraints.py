"""Serverless-compute constraint guards.

Regression test for issue #23: `01_bronze.py`'s `foreachBatch` called
`batch_df.persist()` and `src/quality.py`'s `apply_expectations` called
`df.cache()`. Both raise on Databricks Free Edition serverless —
`[NOT_SUPPORTED_WITH_SERVERLESS] PERSIST TABLE is not supported on serverless
compute` — terminating the stream on the first micro-batch.

CI has no Spark (a hard project constraint: "Spark runs on Databricks serverless
only — never local"), so we can't exercise the stream. Instead we guard the
*source*: the Spark block cache (`.persist(` / `.cache(`) must never reappear on
code that runs on serverless — the notebooks and the `src/` modules they import.
Pure-Python, no Spark required.
"""

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]

# Spark block-cache calls. Matched as method calls (leading dot) so prose/comments
# mentioning `persist()`/`cache()` (backtick-quoted, no dot) don't false-positive.
FORBIDDEN = re.compile(r"\.(persist|cache)\s*\(")

# Everything that runs on serverless: the streaming notebooks and the src modules
# they import inside foreachBatch.
SERVERLESS_SOURCES = sorted(
    {*(ROOT / "notebooks").glob("*.py"), *(ROOT / "src").glob("*.py")}
)


@pytest.mark.parametrize("path", SERVERLESS_SOURCES, ids=lambda p: p.name)
def test_no_block_cache_on_serverless_path(path: Path):
    """No `.persist(` / `.cache(` on any serverless code path (issue #23)."""
    offenders = [
        f"{path.name}:{i}: {line.strip()}"
        for i, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1)
        if FORBIDDEN.search(line)
    ]
    assert not offenders, (
        "Spark block cache is NOT_SUPPORTED_WITH_SERVERLESS — remove these "
        "persist()/cache() calls:\n" + "\n".join(offenders)
    )


def test_guard_actually_catches_a_cache_call():
    """The forbidden-pattern regex matches real calls (so the guard can't rot)."""
    assert FORBIDDEN.search("batch_df.persist()")
    assert FORBIDDEN.search("df = df.cache()")
    assert FORBIDDEN.search("x.cache ()")
    # Prose / comments must not trip it.
    assert not FORBIDDEN.search("No `persist()`/`cache()` on serverless")
    assert not FORBIDDEN.search("PERSIST TABLE is not supported")
