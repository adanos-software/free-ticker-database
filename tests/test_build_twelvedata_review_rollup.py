import csv
import json

from scripts.build_twelvedata_review_rollup import csv_count, csv_counter, error_count


def write_csv(path, rows):
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def test_csv_count_and_counter(tmp_path) -> None:
    path = tmp_path / "rows.csv"
    write_csv(
        path,
        [
            {"listing_key": "A", "decision_candidate": "needs_official_evidence"},
            {"listing_key": "B", "decision_candidate": "uncertain"},
            {"listing_key": "C", "decision_candidate": "needs_official_evidence"},
        ],
    )

    assert csv_count(path) == 3
    assert csv_counter(path, "decision_candidate") == [("needs_official_evidence", 2), ("uncertain", 1)]


def test_error_count_reads_error_payload(tmp_path) -> None:
    path = tmp_path / "errors.json"
    path.write_text(json.dumps({"errors": [{"batch_index": 1}, {"batch_index": 2}]}), encoding="utf-8")

    assert error_count(path) == 2
    assert error_count(tmp_path / "missing.json") == 0
