from __future__ import annotations

from pathlib import Path


def replace_once(source: str, old: str, new: str, *, label: str) -> str:
    count = source.count(old)
    if count != 1:
        raise SystemExit(f"{label}: expected one patch target, found {count}")
    return source.replace(old, new, 1)


def patch_rebuild() -> None:
    path = Path("scripts/rebuild_canonical.py")
    source = path.read_text(encoding="utf-8")

    source = replace_once(
        source,
        "        build_canonical_v4, build_coverage_contracts, build_coverage_report,\n"
        "        build_exchange_source_audit, build_listing_history, build_reference_reconciliation,\n"
        "        enrich_global_identifiers, normalize_source_registry,\n",
        "        build_adanos_ticker_reference, build_canonical_v4, build_coverage_contracts,\n"
        "        build_coverage_report, build_entry_quality_report, build_exchange_source_audit,\n"
        "        build_listing_history, build_reference_reconciliation,\n"
        "        build_source_gap_classification, build_source_of_truth_decisions,\n"
        "        enrich_global_identifiers, normalize_source_registry, update_readme_snapshot,\n",
        label="package imports",
    )
    source = replace_once(
        source,
        "    import build_canonical_v4\n"
        "    import build_coverage_contracts\n"
        "    import build_coverage_report\n"
        "    import build_exchange_source_audit\n"
        "    import build_listing_history\n"
        "    import build_reference_reconciliation\n"
        "    import enrich_global_identifiers\n"
        "    import normalize_source_registry\n",
        "    import build_adanos_ticker_reference\n"
        "    import build_canonical_v4\n"
        "    import build_coverage_contracts\n"
        "    import build_coverage_report\n"
        "    import build_entry_quality_report\n"
        "    import build_exchange_source_audit\n"
        "    import build_listing_history\n"
        "    import build_reference_reconciliation\n"
        "    import build_source_gap_classification\n"
        "    import build_source_of_truth_decisions\n"
        "    import enrich_global_identifiers\n"
        "    import normalize_source_registry\n"
        "    import update_readme_snapshot\n",
        label="direct execution imports",
    )
    source = replace_once(
        source,
        "def rebuild(\n"
        "    *, apply_identity_fixes: bool = False, apply_official_name_updates: bool = False\n"
        ") -> dict[str, Any]:\n",
        "def rebuild_validation_dependents() -> None:\n"
        "    \"\"\"Regenerate every validator input derived from the rebuilt current dataset.\"\"\"\n"
        "\n"
        "    build_entry_quality_report.main([])\n"
        "    build_source_gap_classification.main([])\n"
        "    build_source_of_truth_decisions.main([])\n"
        "    if build_adanos_ticker_reference.main([]) != 0:\n"
        "        raise SystemExit(\"Adanos ticker reference rebuild failed\")\n"
        "    if update_readme_snapshot.main([]) != 0:\n"
        "        raise SystemExit(\"README snapshot rebuild failed\")\n"
        "\n"
        "\n"
        "def rebuild(\n"
        "    *, apply_identity_fixes: bool = False, apply_official_name_updates: bool = False\n"
        ") -> dict[str, Any]:\n",
        label="validation dependent helper",
    )
    source = replace_once(
        source,
        "    build_coverage_report.build_report()\n"
        "    if build_exchange_source_audit.main([]) != 0:\n"
        "        raise SystemExit(\"exchange source audit failed\")\n"
        "    evidence_as_of = _built_at_datetime()\n",
        "    build_coverage_report.build_report()\n"
        "    if build_exchange_source_audit.main([]) != 0:\n"
        "        raise SystemExit(\"exchange source audit failed\")\n"
        "    rebuild_validation_dependents()\n"
        "    evidence_as_of = _built_at_datetime()\n",
        label="validation dependent call",
    )
    path.write_text(source, encoding="utf-8")


def patch_test() -> None:
    path = Path("tests/test_rebuild_canonical_foundation.py")
    source = path.read_text(encoding="utf-8")
    if "test_validation_dependents_are_rebuilt_in_dependency_order" in source:
        return
    test = """


def test_validation_dependents_are_rebuilt_in_dependency_order(monkeypatch) -> None:
    calls: list[str] = []
    monkeypatch.setattr(canonical.build_entry_quality_report, "main", lambda argv: calls.append("entry_quality"))
    monkeypatch.setattr(canonical.build_source_gap_classification, "main", lambda argv: calls.append("source_gap"))
    monkeypatch.setattr(canonical.build_source_of_truth_decisions, "main", lambda argv: calls.append("source_truth"))
    monkeypatch.setattr(canonical.build_adanos_ticker_reference, "main", lambda argv: calls.append("adanos") or 0)
    monkeypatch.setattr(canonical.update_readme_snapshot, "main", lambda argv: calls.append("readme") or 0)
    canonical.rebuild_validation_dependents()
    assert calls == ["entry_quality", "source_gap", "source_truth", "adanos", "readme"]
"""
    path.write_text(source.rstrip() + test + "\n", encoding="utf-8")


def main() -> None:
    patch_rebuild()
    patch_test()


if __name__ == "__main__":
    main()
