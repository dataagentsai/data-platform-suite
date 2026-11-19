"""Write the spike's change feed as JSON files, one batch per call: batch 1 = first load, batch 2 = changes.

    python make_changes.py <folder> 1|2
"""

import json
import sys
from pathlib import Path

BATCHES = {
    1: [  # first load; dealer codes with stray spaces and a tab, which clean_text must strip
        {"order_id": "O1", "line_no": 1, "dealer_code": " D-0042 ", "promised_date": "2026-11-10", "qty": 4, "op": "I", "change_seq": 10},
        {"order_id": "O1", "line_no": 2, "dealer_code": "D-0042\t", "promised_date": "2026-11-10", "qty": 2, "op": "I", "change_seq": 11},
        {"order_id": "O2", "line_no": 1, "dealer_code": "D-0007", "promised_date": "2026-11-12", "qty": 8, "op": "I", "change_seq": 12},
    ],
    2: [  # changes: a promised date moved twice (the older change arrives after the newer), a qty change, a delete
        {"order_id": "O1", "line_no": 1, "dealer_code": "D-0042", "promised_date": "2026-11-12", "qty": 4, "op": "U", "change_seq": 30},
        {"order_id": "O1", "line_no": 1, "dealer_code": "D-0042", "promised_date": "2026-11-11", "qty": 4, "op": "U", "change_seq": 20},
        {"order_id": "O2", "line_no": 1, "dealer_code": "D-0007", "promised_date": "2026-11-12", "qty": 6, "op": "U", "change_seq": 31},
        {"order_id": "O1", "line_no": 2, "dealer_code": "D-0042", "promised_date": "2026-11-10", "qty": 2, "op": "D", "change_seq": 32},
    ],
}

folder, batch = Path(sys.argv[1]), int(sys.argv[2])
folder.mkdir(parents=True, exist_ok=True)
(folder / f"batch-{batch}.json").write_text("\n".join(json.dumps(r) for r in BATCHES[batch]) + "\n")
print(f"wrote {len(BATCHES[batch])} changes to {folder / f'batch-{batch}.json'}")
