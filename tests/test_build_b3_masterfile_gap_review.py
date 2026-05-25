from scripts.build_b3_masterfile_gap_review import (
    b3_listing_context_for,
    build_b3_coverage_snapshot,
    build_review_rows,
    official_candidate_context_for,
    review_gate_context_for,
    summarize,
)


def test_build_review_rows_classifies_active_directory_gaps_by_official_source_presence() -> None:
    listings = [
        {
            "listing_key": "B3::PETR4",
            "ticker": "PETR4",
            "exchange": "B3",
            "asset_type": "Stock",
            "name": "Petrobras",
        },
        {
            "listing_key": "B3::AFOF11",
            "ticker": "AFOF11",
            "exchange": "B3",
            "asset_type": "ETF",
            "etf_category": "Equity",
            "name": "Alianza FOFII",
        },
        {
            "listing_key": "B3::ADMF3",
            "ticker": "ADMF3",
            "exchange": "B3",
            "asset_type": "Stock",
            "name": "CIABRASF",
        },
        {
            "listing_key": "NYSE::PBR",
            "ticker": "PBR",
            "exchange": "NYSE",
            "asset_type": "Stock",
            "name": "Petrobras ADR",
        },
    ]
    masterfile_rows = [
        {
            "source_key": "b3_instruments_equities",
            "source_url": "https://arquivos.b3.com.br/bdi/table/InstrumentsEquities",
            "ticker": "PETR4",
            "exchange": "B3",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
        },
        {
            "source_key": "b3_listed_etfs",
            "source_url": "https://www.b3.com.br/en_us/products-and-services/trading/etf/",
            "ticker": "AFOF11",
            "name": "ALIANZA FOFII FUNDO DE INVESTIMENTO IMOBILIARIO",
            "exchange": "B3",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "sector": "Fixed Income",
            "isin": "BRAFOFCTF000",
        },
    ]
    rows = build_review_rows(
        listings=listings,
        masterfile_rows=masterfile_rows,
    )

    assert [row["listing_key"] for row in rows] == ["B3::AFOF11", "B3::ADMF3"]
    assert rows[0]["source_presence"] == "present_only_in_non_exchange_directory_source"
    assert rows[0]["candidate_sources"] == "b3_listed_etfs"
    assert rows[0]["candidate_names"] == "ALIANZA FOFII FUNDO DE INVESTIMENTO IMOBILIARIO"
    assert rows[0]["candidate_asset_types"] == "ETF"
    assert rows[0]["candidate_isins"] == "BRAFOFCTF000"
    assert rows[0]["candidate_sectors"] == "Fixed Income"
    assert rows[0]["candidate_category_review_decision"] == (
        "official_candidate_category_differs_from_current_requires_review"
    )
    assert rows[0]["official_subset_review_decision"] == "official_subset_category_mismatch_requires_apply_gate"
    assert rows[0]["official_subset_closure_eligibility"] == "blocked_until_category_apply_gate"
    assert rows[0]["b3_resolution_queue"] == "official_subset_category_requires_review"
    assert rows[0]["review_bucket"] == "official_b3_non_directory_source_review"
    assert rows[0]["review_priority"] == "P2"
    assert rows[0]["review_strategy"] == "review_official_subset_category_and_scope_before_apply_gate"
    assert rows[0]["recommended_next_source"] == (
        "Official B3 subset source plus category taxonomy evidence with exact listing-key match."
    )
    assert rows[0]["source_gate"] == (
        "Apply category only after official subset category, listing key, and current dataset category are reviewed."
    )
    assert rows[0]["b3_listing_context"] == b3_listing_context_for(rows[0])
    assert rows[0]["official_candidate_context"] == official_candidate_context_for(rows[0])
    assert rows[0]["review_gate_context"] == review_gate_context_for(rows[0])
    assert rows[1]["source_presence"] == "absent_from_all_b3_masterfile_sources"
    assert rows[1]["official_subset_review_decision"] == "not_official_subset_source_gap"
    assert rows[1]["official_subset_closure_eligibility"] == (
        "blocked_until_current_official_active_source_evidence"
    )
    assert rows[1]["b3_resolution_queue"] == "absent_from_all_b3_sources_local_share_source_gap"
    assert rows[1]["review_bucket"] == "missing_from_all_b3_masterfile_sources_source_gap"
    assert rows[1]["review_priority"] == "P3"
    assert rows[1]["review_strategy"] == "keep_local_share_gap_until_current_official_b3_or_issuer_evidence"
    assert rows[1]["recommended_next_source"] == (
        "Current B3 exchange directory, B3 issuer page, CVM filing, or issuer investor-relations listing evidence."
    )
    assert rows[1]["source_gate"] == (
        "Keep row as source gap until current official B3 or issuer evidence proves the active local-share listing."
    )

    coverage_snapshot = build_b3_coverage_snapshot(
        listings=listings,
        masterfile_rows=masterfile_rows,
        review_rows=rows,
    )
    summary = summarize(rows, "2026-05-24T00:00:00Z", coverage_snapshot)
    assert summary["rows"] == 2
    assert summary["open_review_rows"] == 2
    assert summary["closed_no_data_change_rows"] == 0
    assert summary["open_review_source_presence_totals"] == {
        "absent_from_all_b3_masterfile_sources": 1,
        "present_only_in_non_exchange_directory_source": 1,
    }
    assert summary["open_review_resolution_queue_totals"] == {
        "absent_from_all_b3_sources_local_share_source_gap": 1,
        "official_subset_category_requires_review": 1,
    }
    assert summary["open_review_next_source_totals"] == {
        "Current B3 exchange directory, B3 issuer page, CVM filing, or issuer investor-relations listing evidence.": 1,
        "Official B3 subset source plus category taxonomy evidence with exact listing-key match.": 1,
    }
    assert summary["open_review_evidence_path_totals"] == {
        "current_b3_exchange_directory_or_cvm_issuer_listing_evidence": 1,
        "official_b3_subset_category_apply_evidence": 1,
    }
    assert summary["source_gap_resolution_gate_totals"] == {
        "apply_only_after_listing_keyed_category_review": 1,
        "do_not_delete_or_rename_until_current_b3_cvm_or_issuer_listing_evidence_is_reviewed": 1,
    }
    assert summary["coverage_snapshot"] == {
        "dataset_rows": 3,
        "active_exchange_directory_rows": 1,
        "all_b3_masterfile_rows": 2,
        "active_directory_matched_dataset_rows": 1,
        "active_directory_missing_dataset_rows": 2,
        "active_directory_match_rate": 33.33,
        "official_any_source_matched_dataset_rows": 2,
        "official_any_source_missing_dataset_rows": 1,
        "official_any_source_match_rate": 66.67,
        "active_exchange_directory_source_keys": ["b3_instruments_equities"],
        "official_non_directory_source_keys": ["b3_listed_etfs"],
        "active_directory_gap_rows": 2,
        "official_non_directory_gap_rows": 1,
        "absent_from_all_b3_source_gap_rows": 1,
        "diagnosis": (
            "Active B3 exchange-directory coverage is measured against b3_instruments_equities; rows found only in "
            "official ETF/BDR subset sources remain parser/scope review cases, and rows absent from all B3 sources "
            "remain source gaps."
        ),
    }
    assert summary["coverage_diagnosis"] == {
        "status": "active_directory_coverage_has_official_subset_parser_or_scope_gap",
        "dataset_rows": 3,
        "active_directory_match_rate": 33.33,
        "active_directory_missing_dataset_rows": 2,
        "open_review_rows": 2,
        "closed_no_data_change_rows": 0,
        "official_non_directory_gap_rows": 1,
        "absent_from_all_b3_source_gap_rows": 1,
        "official_subset_candidate_isin_rows": 1,
        "official_subset_candidate_sector_rows": 1,
        "rows_requiring_parser_or_scope_review": 1,
        "rows_requiring_external_active_evidence": 1,
        "data_change_authorized": False,
        "root_cause": (
            "Residual B3 coverage gaps split between official B3 subset rows outside the active exchange-directory "
            "parser scope and listings absent from all current B3 masterfile sources."
        ),
        "source_gate": (
            "No B3 ISIN, sector, category, name, symbol, or scope change is authorized until the exact listing-keyed "
            "official source evidence and apply gate are reviewed."
        ),
    }
    assert summary["source_presence_totals"] == {
        "absent_from_all_b3_masterfile_sources": 1,
        "present_only_in_non_exchange_directory_source": 1,
    }
    assert summary["candidate_source_totals"] == {"b3_listed_etfs": 1}
    assert summary["candidate_sector_present_rows"] == 1
    assert summary["candidate_isin_present_rows"] == 1
    assert summary["candidate_category_review_decision_totals"] == {
        "no_official_candidate_category": 1,
        "official_candidate_category_differs_from_current_requires_review": 1,
    }
    assert summary["official_subset_review_decision_totals"] == {
        "not_official_subset_source_gap": 1,
        "official_subset_category_mismatch_requires_apply_gate": 1,
    }
    assert summary["official_subset_closure_eligibility_totals"] == {
        "blocked_until_category_apply_gate": 1,
        "blocked_until_current_official_active_source_evidence": 1,
    }
    assert summary["official_subset_closure_ready_rows"] == 0
    assert summary["b3_resolution_queue_totals"] == {
        "absent_from_all_b3_sources_local_share_source_gap": 1,
        "official_subset_category_requires_review": 1,
    }
    assert summary["b3_resolution_queue_asset_type_totals"] == {
        "absent_from_all_b3_sources_local_share_source_gap": {"Stock": 1},
        "official_subset_category_requires_review": {"ETF": 1},
    }
    assert summary["b3_resolution_queue_gap_category_totals"] == {
        "absent_from_all_b3_sources_local_share_source_gap": {"local_share_line": 1},
        "official_subset_category_requires_review": {"unit_or_fund_line": 1},
    }
    assert summary["candidate_category_mismatch_rows"] == 1
    assert summary["candidate_category_mismatch_examples"] == [
        {
            "listing_key": "B3::AFOF11",
            "ticker": "AFOF11",
            "current_etf_category": "Equity",
            "candidate_sectors": "Fixed Income",
            "candidate_sources": "b3_listed_etfs",
        }
    ]
    assert summary["review_strategy_totals"] == {
        "keep_local_share_gap_until_current_official_b3_or_issuer_evidence": 1,
        "review_official_subset_category_and_scope_before_apply_gate": 1,
    }
    assert summary["top_b3_masterfile_gap_review_batches"] == [
        {
            "review_priority": "P2",
            "b3_resolution_queue": "official_subset_category_requires_review",
            "asset_type": "ETF",
            "b3_gap_category": "unit_or_fund_line",
            "source_presence": "present_only_in_non_exchange_directory_source",
            "rows": 1,
            "review_strategy": "review_official_subset_category_and_scope_before_apply_gate",
            "verification_evidence_required": (
                "official_b3_source_row_plus_scope_decision_or_parser_fix_before_reclassifying_gap"
            ),
            "b3_source_gap_evidence_path": "official_b3_subset_category_apply_evidence",
            "source_gap_resolution_gate": "apply_only_after_listing_keyed_category_review",
            "recommended_next_source": "Official B3 subset source plus category taxonomy evidence with exact listing-key match.",
            "source_gate": (
                "Apply category only after official subset category, listing key, and current dataset category are reviewed."
            ),
        },
        {
            "review_priority": "P3",
            "b3_resolution_queue": "absent_from_all_b3_sources_local_share_source_gap",
            "asset_type": "Stock",
            "b3_gap_category": "local_share_line",
            "source_presence": "absent_from_all_b3_masterfile_sources",
            "rows": 1,
            "review_strategy": "keep_local_share_gap_until_current_official_b3_or_issuer_evidence",
            "verification_evidence_required": (
                "new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key"
            ),
            "b3_source_gap_evidence_path": "current_b3_exchange_directory_or_cvm_issuer_listing_evidence",
            "source_gap_resolution_gate": (
                "do_not_delete_or_rename_until_current_b3_cvm_or_issuer_listing_evidence_is_reviewed"
            ),
            "recommended_next_source": (
                "Current B3 exchange directory, B3 issuer page, CVM filing, or issuer investor-relations listing evidence."
            ),
            "source_gate": (
                "Keep row as source gap until current official B3 or issuer evidence proves the active local-share listing."
            ),
        },
    ]
    assert summary["top_open_b3_masterfile_review_batches"] == summary["top_b3_masterfile_gap_review_batches"]
    assert summary["top_open_b3_masterfile_review_rows"] == [
        {
            "listing_key": "B3::AFOF11",
            "ticker": "AFOF11",
            "asset_type": "ETF",
            "name": "Alianza FOFII",
            "b3_gap_category": "unit_or_fund_line",
            "b3_resolution_queue": "official_subset_category_requires_review",
            "review_priority": "P2",
            "review_strategy": "review_official_subset_category_and_scope_before_apply_gate",
            "verification_evidence_required": (
                "official_b3_source_row_plus_scope_decision_or_parser_fix_before_reclassifying_gap"
            ),
            "b3_source_gap_evidence_path": "official_b3_subset_category_apply_evidence",
            "source_gap_resolution_gate": "apply_only_after_listing_keyed_category_review",
            "recommended_next_source": (
                "Official B3 subset source plus category taxonomy evidence with exact listing-key match."
            ),
            "source_gate": (
                "Apply category only after official subset category, listing key, and current dataset category are reviewed."
            ),
        },
        {
            "listing_key": "B3::ADMF3",
            "ticker": "ADMF3",
            "asset_type": "Stock",
            "name": "CIABRASF",
            "b3_gap_category": "local_share_line",
            "b3_resolution_queue": "absent_from_all_b3_sources_local_share_source_gap",
            "review_priority": "P3",
            "review_strategy": "keep_local_share_gap_until_current_official_b3_or_issuer_evidence",
            "verification_evidence_required": (
                "new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key"
            ),
            "b3_source_gap_evidence_path": "current_b3_exchange_directory_or_cvm_issuer_listing_evidence",
            "source_gap_resolution_gate": (
                "do_not_delete_or_rename_until_current_b3_cvm_or_issuer_listing_evidence_is_reviewed"
            ),
            "recommended_next_source": (
                "Current B3 exchange directory, B3 issuer page, CVM filing, or issuer investor-relations listing evidence."
            ),
            "source_gate": (
                "Keep row as source gap until current official B3 or issuer evidence proves the active local-share listing."
            ),
        },
    ]
    assert summary["policy"]["no_guessing"].startswith("This review does not authorize inferred")


def test_b3_bdr_subset_without_category_closes_without_data_change() -> None:
    listings = [
        {
            "listing_key": "B3::BIAU39",
            "ticker": "BIAU39",
            "exchange": "B3",
            "asset_type": "ETF",
            "etf_category": "Equity",
            "name": "Ishares Gold Trust",
        },
        {
            "listing_key": "B3::ADMF3",
            "ticker": "ADMF3",
            "exchange": "B3",
            "asset_type": "Stock",
            "name": "CIABRASF",
        },
    ]
    masterfile_rows = [
        {
            "source_key": "b3_bdr_etfs",
            "source_url": "https://www.b3.com.br/en_us/products-and-services/trading/etf/bdr-etf/",
            "ticker": "BIAU39",
            "name": "ISHARES GOLD TRUST",
            "exchange": "B3",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
        },
    ]

    rows = build_review_rows(
        listings=listings,
        masterfile_rows=masterfile_rows,
    )

    assert [row["listing_key"] for row in rows] == ["B3::BIAU39", "B3::ADMF3"]
    assert rows[0]["source_presence"] == "present_only_in_non_exchange_directory_source"
    assert rows[0]["b3_gap_category"] == "bdr_or_foreign_receipt"
    assert rows[0]["candidate_sources"] == "b3_bdr_etfs"
    assert rows[0]["candidate_isins"] == ""
    assert rows[0]["candidate_sectors"] == ""
    assert rows[0]["candidate_category_review_decision"] == "no_official_candidate_category"
    assert rows[0]["official_subset_review_decision"] == (
        "official_subset_bdr_without_category_no_data_change"
    )
    assert rows[0]["official_subset_closure_eligibility"] == (
        "closure_ready_official_subset_bdr_without_category_source_gap"
    )
    assert rows[0]["b3_resolution_queue"] == "official_bdr_subset_without_category_source_gap_closed"
    assert rows[0]["review_strategy"] == (
        "close_bdr_subset_gap_without_data_change_keep_category_source_gap"
    )
    assert rows[0]["source_gate"] == (
        "No B3 category, ISIN, name, symbol, or scope change is authorized; "
        "the official BDR subset evidence only closes the active-directory gap."
    )

    coverage_snapshot = build_b3_coverage_snapshot(
        listings=listings,
        masterfile_rows=masterfile_rows,
        review_rows=rows,
    )
    summary = summarize(rows, "2026-05-24T00:00:00Z", coverage_snapshot)
    assert summary["rows"] == 2
    assert summary["open_review_rows"] == 1
    assert summary["closed_no_data_change_rows"] == 1
    assert summary["open_review_source_presence_totals"] == {
        "absent_from_all_b3_masterfile_sources": 1,
    }
    assert summary["open_review_resolution_queue_totals"] == {
        "absent_from_all_b3_sources_local_share_source_gap": 1,
    }
    assert summary["open_review_next_source_totals"] == {
        "Current B3 exchange directory, B3 issuer page, CVM filing, or issuer investor-relations listing evidence.": 1,
    }
    assert summary["open_review_evidence_path_totals"] == {
        "current_b3_exchange_directory_or_cvm_issuer_listing_evidence": 1,
    }
    assert summary["official_subset_closure_eligibility_totals"] == {
        "blocked_until_current_official_active_source_evidence": 1,
        "closure_ready_official_subset_bdr_without_category_source_gap": 1,
    }
    assert summary["official_subset_closure_ready_rows"] == 1
    assert summary["coverage_diagnosis"]["rows_requiring_parser_or_scope_review"] == 0
    assert summary["coverage_diagnosis"]["rows_requiring_external_active_evidence"] == 1
    assert summary["top_open_b3_masterfile_review_batches"] == [
        {
            "review_priority": "P3",
            "b3_resolution_queue": "absent_from_all_b3_sources_local_share_source_gap",
            "asset_type": "Stock",
            "b3_gap_category": "local_share_line",
            "source_presence": "absent_from_all_b3_masterfile_sources",
            "rows": 1,
            "review_strategy": "keep_local_share_gap_until_current_official_b3_or_issuer_evidence",
            "verification_evidence_required": (
                "new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key"
            ),
            "b3_source_gap_evidence_path": "current_b3_exchange_directory_or_cvm_issuer_listing_evidence",
            "source_gap_resolution_gate": (
                "do_not_delete_or_rename_until_current_b3_cvm_or_issuer_listing_evidence_is_reviewed"
            ),
            "recommended_next_source": (
                "Current B3 exchange directory, B3 issuer page, CVM filing, or issuer investor-relations listing evidence."
            ),
            "source_gate": (
                "Keep row as source gap until current official B3 or issuer evidence proves the active local-share listing."
            ),
        },
    ]
    assert summary["top_open_b3_masterfile_review_rows"] == [
        {
            "listing_key": "B3::ADMF3",
            "ticker": "ADMF3",
            "asset_type": "Stock",
            "name": "CIABRASF",
            "b3_gap_category": "local_share_line",
            "b3_resolution_queue": "absent_from_all_b3_sources_local_share_source_gap",
            "review_priority": "P3",
            "review_strategy": "keep_local_share_gap_until_current_official_b3_or_issuer_evidence",
            "verification_evidence_required": (
                "new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key"
            ),
            "b3_source_gap_evidence_path": "current_b3_exchange_directory_or_cvm_issuer_listing_evidence",
            "source_gap_resolution_gate": (
                "do_not_delete_or_rename_until_current_b3_cvm_or_issuer_listing_evidence_is_reviewed"
            ),
            "recommended_next_source": (
                "Current B3 exchange directory, B3 issuer page, CVM filing, or issuer investor-relations listing evidence."
            ),
            "source_gate": (
                "Keep row as source gap until current official B3 or issuer evidence proves the active local-share listing."
            ),
        },
    ]


def test_b3_gap_classification_uses_asset_type_before_numeric_suffix() -> None:
    listings = [
        {
            "listing_key": "B3::HCRA16",
            "ticker": "HCRA16",
            "exchange": "B3",
            "asset_type": "ETF",
            "etf_category": "Fixed Income",
            "name": "HEDGE CREDITO AGRO FIAGRO DE RESP LIMITADA",
        },
        {
            "listing_key": "B3::CPTS11B",
            "ticker": "CPTS11B",
            "exchange": "B3",
            "asset_type": "ETF",
            "etf_category": "Real Estate",
            "name": "Capitania Securities II Fundo Investimento Imobiliario FII",
        },
        {
            "listing_key": "B3::ADMF3",
            "ticker": "ADMF3",
            "exchange": "B3",
            "asset_type": "Stock",
            "name": "CIABRASF",
        },
    ]

    rows = build_review_rows(listings=listings, masterfile_rows=[])

    by_key = {row["listing_key"]: row for row in rows}
    assert by_key["B3::HCRA16"]["b3_gap_category"] == "unit_or_fund_line"
    assert by_key["B3::HCRA16"]["b3_resolution_queue"] == (
        "absent_from_all_b3_sources_fund_or_receipt_source_gap"
    )
    assert by_key["B3::CPTS11B"]["b3_gap_category"] == "unit_or_fund_line"
    assert by_key["B3::CPTS11B"]["b3_resolution_queue"] == (
        "absent_from_all_b3_sources_fund_or_receipt_source_gap"
    )
    assert by_key["B3::ADMF3"]["b3_gap_category"] == "local_share_line"
    assert by_key["B3::ADMF3"]["b3_resolution_queue"] == (
        "absent_from_all_b3_sources_local_share_source_gap"
    )

    coverage_snapshot = build_b3_coverage_snapshot(
        listings=listings,
        masterfile_rows=[],
        review_rows=rows,
    )
    summary = summarize(rows, "2026-05-24T00:00:00Z", coverage_snapshot)
    assert summary["open_review_resolution_queue_totals"] == {
        "absent_from_all_b3_sources_fund_or_receipt_source_gap": 2,
        "absent_from_all_b3_sources_local_share_source_gap": 1,
    }
    assert summary["b3_resolution_queue_gap_category_totals"] == {
        "absent_from_all_b3_sources_fund_or_receipt_source_gap": {"unit_or_fund_line": 2},
        "absent_from_all_b3_sources_local_share_source_gap": {"local_share_line": 1},
    }


def test_b3_masterfile_gap_contexts_are_derived_from_row_fields() -> None:
    row = {
        "listing_key": "B3::AFOF11",
        "ticker": "AFOF11",
        "asset_type": "ETF",
        "current_etf_category": "Equity",
        "b3_gap_category": "unit_or_fund_line",
        "source_presence": "present_only_in_non_exchange_directory_source",
        "candidate_sources": "b3_listed_etfs",
        "candidate_isins": "BRAFOFCTF000",
        "candidate_sectors": "Fixed Income",
        "active_exchange_directory_match": "false",
        "any_official_b3_source_match": "true",
        "b3_resolution_queue": "official_subset_category_requires_review",
        "residual_decision": "official_b3_non_directory_source_requires_scope_or_parser_review",
        "review_bucket": "official_b3_non_directory_source_review",
        "official_subset_review_decision": "official_subset_category_mismatch_requires_apply_gate",
        "official_subset_closure_eligibility": "blocked_until_category_apply_gate",
        "apply_eligibility": "review_scope_or_parser_before_any_data_change",
        "verification_evidence_required": (
            "official_b3_source_row_plus_scope_decision_or_parser_fix_before_reclassifying_gap"
        ),
    }

    assert (
        b3_listing_context_for(row)
        == "listing_key=B3::AFOF11;ticker=AFOF11;asset_type=ETF;"
        "b3_gap_category=unit_or_fund_line;current_etf_category=Equity"
    )
    assert (
        official_candidate_context_for(row)
        == "source_presence=present_only_in_non_exchange_directory_source;candidate_sources=b3_listed_etfs;"
        "candidate_isins_present=true;candidate_sectors_present=true;"
        "active_exchange_directory_match=false;any_official_b3_source_match=true"
    )
    assert (
        review_gate_context_for(row)
        == "b3_resolution_queue=official_subset_category_requires_review;"
        "residual_decision=official_b3_non_directory_source_requires_scope_or_parser_review;"
        "review_bucket=official_b3_non_directory_source_review;"
        "official_subset_review_decision=official_subset_category_mismatch_requires_apply_gate;"
        "official_subset_closure_eligibility=blocked_until_category_apply_gate;"
        "apply_eligibility=review_scope_or_parser_before_any_data_change;"
        "verification_evidence_required=official_b3_source_row_plus_scope_decision_or_parser_fix_before_reclassifying_gap"
    )
