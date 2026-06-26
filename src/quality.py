"""Reusable data-quality expectation helpers (WP5).

Importable by WP1–WP3 with a one-line call. Failed rows are persisted to
`ops.dq_failures` (never silently dropped). Mirrors Lakeflow/DLT `@dlt.expect`
semantics for `foreachBatch` contexts.
"""

# TODO(WP5): not_null(keys), range checks (price/qty), enum checks; persist
# failures to CONFIG.tbl_dq_failures.
