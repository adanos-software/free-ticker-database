import csv
import json

from scripts.build_cfi_code_review import build_payload, main


def write_reference(path, rows):
    fieldnames = [
        "source_key",
        "provider",
        "source_url",
        "ticker",
        "name",
        "exchange",
        "asset_type",
        "listing_status",
        "reference_scope",
        "official",
        "isin",
        "cfi",
        "sector",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def test_build_payload_surfaces_official_cfi_as_review_only_gate(tmp_path):
    reference_csv = tmp_path / "reference.csv"
    write_reference(
        reference_csv,
        [
            {
                "source_key": "bahrain",
                "ticker": "ABC",
                "exchange": "BHB",
                "asset_type": "Stock",
                "listing_status": "active",
                "name": "ABC Common Share",
                "isin": "BH0008794115",
                "cfi": "ESVUFR",
            },
            {
                "source_key": "bahrain",
                "ticker": "PREF",
                "exchange": "BHB",
                "asset_type": "Stock",
                "listing_status": "active",
                "name": "ABC Preference Share",
                "isin": "BH0000000000",
                "cfi": "EPXXXX",
            },
            {
                "source_key": "empty",
                "ticker": "NONE",
                "exchange": "BHB",
                "asset_type": "Stock",
                "listing_status": "active",
                "name": "No CFI",
            },
        ],
    )

    payload = build_payload(reference_csv)

    assert payload["summary"]["cfi_evidence_rows"] == 2
    assert payload["summary"]["blocked_non_common_stock_review_rows"] == 1
    assert payload["summary"]["decision_totals"] == {
        "accepted_common_stock_cfi_evidence": 1,
        "blocked_non_common_stock_review": 1,
    }
    assert all("identity" in row["source_gate"] for row in payload["rows"])
    assert all("checksum" in row["source_gate"] for row in payload["rows"])
    assert all("collision" in row["source_gate"] for row in payload["rows"])


def test_main_writes_cfi_review_artifacts(tmp_path):
    reference_csv = tmp_path / "reference.csv"
    csv_out = tmp_path / "cfi.csv"
    json_out = tmp_path / "cfi.json"
    md_out = tmp_path / "cfi.md"
    write_reference(
        reference_csv,
        [
            {
                "source_key": "ngm",
                "ticker": "ECC-B",
                "exchange": "STO",
                "asset_type": "Stock",
                "listing_status": "active",
                "name": "Ecoclime Group B",
                "isin": "SE0012729937",
                "cfi": "ESVUFR",
            }
        ],
    )

    main(
        [
            "--reference-csv",
            str(reference_csv),
            "--csv-out",
            str(csv_out),
            "--json-out",
            str(json_out),
            "--md-out",
            str(md_out),
        ]
    )

    assert csv_out.exists()
    assert json.loads(json_out.read_text(encoding="utf-8"))["summary"]["cfi_evidence_rows"] == 1
    assert "CFI evidence rows" in md_out.read_text(encoding="utf-8")
