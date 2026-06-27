"""Pytest bootstrap — make repo-root imports work without installing the package.

Adds the repo root (for ``src.config``) and ``fixtures/`` (for
``generate_fixtures``) to ``sys.path`` so the pure-Python WP0 tests import cleanly.
"""

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent
for _p in (_ROOT, _ROOT / "fixtures"):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))
