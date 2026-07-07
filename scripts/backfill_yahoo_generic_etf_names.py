from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.archive.backfill_yahoo_generic_etf_names import *  # noqa: F403
from scripts.archive.backfill_yahoo_generic_etf_names import main


if __name__ == "__main__":
    raise SystemExit(main())
