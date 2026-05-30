from scripts.build_isin_identity_collision_review_queue import (
    build_queue_rows,
    cluster_listings_by_name,
    compact_name_key,
    is_full_name,
    name_tokens,
    next_review_batches,
    shared_tickers_across_clusters,
    summarize,
)


def listing(listing_key, ticker, exchange, name, isin, *, country="", country_code="", asset_type="Stock"):
    return {
        "listing_key": listing_key,
        "ticker": ticker,
        "exchange": exchange,
        "name": name,
        "isin": isin,
        "country": country,
        "country_code": country_code,
        "asset_type": asset_type,
    }


def country_from_isin_stub(isin):
    return {"AU": "Australia", "CA": "Canada", "US": "United States"}.get(isin[:2])


def always_valid(_isin):
    return True


def test_is_full_name_rejects_trading_mnemonics_and_accepts_full_names():
    assert is_full_name("Ameriprise Financial Inc")
    assert is_full_name("AMP Ltd")
    assert is_full_name("Liberty Gold Corp")
    # Uppercase mnemonic with no corporate suffix is not a reliable issuer name.
    assert not is_full_name("CHINA RES BEER")
    assert not is_full_name("")


def test_name_tokens_strip_corporate_forms_so_variants_match():
    assert name_tokens("Liberty Gold Corp.") == name_tokens("Liberty Gold Corp")
    assert "LIBERTY" in name_tokens("Liberty Gold Corp")
    # Corporate suffixes and class noise drop out entirely.
    assert name_tokens("The Home Depot Inc") == {"HOME", "DEPOT"}


def test_compact_name_key_collapses_spacing_and_punctuation_variants():
    assert compact_name_key("Lend Lease Group") == compact_name_key("Lendlease")
    assert compact_name_key("Moody's Corporation") == compact_name_key("Moodys Corporation")
    assert compact_name_key("JC Decaux SA") == compact_name_key("JCDecaux SA")
    # Distinct issuers keep distinct keys.
    assert compact_name_key("Genex Pharmaceutical Inc") != compact_name_key("Genix Pharmaceuticals Corporation")


def test_clustering_reconciles_unicode_and_compound_variants_of_one_issuer():
    # Nordic diacritic-drop, German umlaut expansion, and compound spacing all
    # describe a single issuer and must not be reported as a collision.
    same_issuer_cases = [
        ("Huhtamaki Oyj", "Huhtamäki Oyj"),
        ("Suedzucker AG", "Südzucker AG"),
        ("A.P. Moeller-Maersk A/S", "A.P. Møller - Mærsk A/S"),
        ("Lend Lease Group", "Lendlease Corporation"),
        ("Moody's Corporation", "Moodys Corporation"),
    ]
    for ascii_name, native_name in same_issuer_cases:
        rows = [
            listing("X::1", "AAA", "X", ascii_name, "EXAMPLEISIN01"),
            listing("Y::2", "BBB", "Y", native_name, "EXAMPLEISIN01"),
        ]
        clusters = cluster_listings_by_name(rows)
        assert len(clusters) == 1, f"{ascii_name!r} and {native_name!r} should cluster together"


def test_cluster_listings_groups_same_issuer_and_splits_distinct_issuers():
    rows = [
        listing("TSX::LGD", "LGD", "TSX", "Liberty Gold Corp.", "CA53056H1047"),
        listing("OTC::LGDTF", "LGDTF", "OTC", "Liberty Gold Corp", "CA53056H1047"),
        listing("OTC::PGWFF", "PGWFF", "OTC", "PGG Wrightson Limited", "CA53056H1047"),
    ]
    clusters = cluster_listings_by_name(rows)
    assert len(clusters) == 2
    sizes = sorted(len(c["rows"]) for c in clusters)
    assert sizes == [1, 2]


def test_shared_tickers_detected_only_across_distinct_clusters():
    clusters = [
        {"rows": [listing("NYSE ARCA::SPXU", "SPXU", "NYSE ARCA", "ProShares UltraPro Short S&P500", "CA08663L1040", asset_type="ETF")]},
        {"rows": [listing("TSX::SPXU", "SPXU", "TSX", "Global X S&P 500 Index Bull ETF", "CA08663L1040", asset_type="ETF")]},
    ]
    assert shared_tickers_across_clusters(clusters) == ["SPXU"]


def test_build_queue_rows_flags_distinct_issuer_collision_without_data_change():
    rows = [
        listing("NYSE::AMP", "AMP", "NYSE", "Ameriprise Financial Inc", "AU000000AMP6"),
        listing("OTC::AMLTF", "AMLTF", "OTC", "AMP Ltd", "AU000000AMP6"),
        # An ISIN held by a single issuer (variants) is not a collision.
        listing("NYSE::WY", "WY", "NYSE", "Weyerhaeuser Company", "US9621661043"),
        listing("LSE::0LWG", "0LWG", "LSE", "Weyerhaeuser Co.", "US9621661043"),
    ]
    queue = build_queue_rows(rows, isin_valid_fn=always_valid, country_from_isin_fn=country_from_isin_stub)
    assert len(queue) == 1
    item = queue[0]
    assert item["isin"] == "AU000000AMP6"
    assert item["registered_country"] == "Australia"
    assert item["decision_candidate"] == "isin_shared_by_distinct_issuers"
    assert item["review_queue"] == "manual_isin_identity_review"
    assert item["closure_status"] == "open_needs_official_identifier_evidence"
    assert "name_clusters_disjoint" in item["collision_signals"]
    assert item["listing_keys"] == "NYSE::AMP|OTC::AMLTF"


def test_build_queue_rows_flags_ticker_collision_subsignal():
    rows = [
        listing("NYSE ARCA::SPXU", "SPXU", "NYSE ARCA", "ProShares UltraPro Short S&P500", "CA08663L1040", asset_type="ETF"),
        listing("TSX::SPXU", "SPXU", "TSX", "Global X S&P 500 Index Bull ETF", "CA08663L1040", asset_type="ETF"),
    ]
    queue = build_queue_rows(rows, isin_valid_fn=always_valid, country_from_isin_fn=country_from_isin_stub)
    assert len(queue) == 1
    item = queue[0]
    assert item["decision_candidate"] == "ticker_collision_isin_misassignment_suspected"
    assert "ticker_collision" in item["collision_signals"]
    assert item["shared_tickers"] == "SPXU"


def test_summarize_reports_closure_and_next_batches():
    rows = [
        listing("NYSE::AMP", "AMP", "NYSE", "Ameriprise Financial Inc", "AU000000AMP6"),
        listing("OTC::AMLTF", "AMLTF", "OTC", "AMP Ltd", "AU000000AMP6"),
        listing("NYSE ARCA::SPXU", "SPXU", "NYSE ARCA", "ProShares UltraPro Short S&P500", "CA08663L1040", asset_type="ETF"),
        listing("TSX::SPXU", "SPXU", "TSX", "Global X S&P 500 Index Bull ETF", "CA08663L1040", asset_type="ETF"),
    ]
    queue = build_queue_rows(rows, isin_valid_fn=always_valid, country_from_isin_fn=country_from_isin_stub)
    summary = summarize(queue, generated_at="2026-05-30T00:00:00Z")
    assert summary["generated_at"] == "2026-05-30T00:00:00Z"
    assert summary["collision_groups"] == 2
    assert summary["listings_involved"] == 4
    assert summary["ticker_collision_groups"] == 1
    assert summary["open_groups"] == 2
    assert summary["closed_groups"] == 0
    assert summary["direct_identifier_apply_allowed_rows"] == 0
    assert summary["decision_candidate_totals"] == {
        "isin_shared_by_distinct_issuers": 1,
        "ticker_collision_isin_misassignment_suspected": 1,
    }


def test_next_review_batches_sorted_by_group_count():
    rows = [
        {"decision_candidate": "isin_shared_by_distinct_issuers", "registered_country": "Canada"},
        {"decision_candidate": "isin_shared_by_distinct_issuers", "registered_country": "Canada"},
        {"decision_candidate": "isin_shared_by_distinct_issuers", "registered_country": "Australia"},
    ]
    batches = next_review_batches(rows)
    assert batches[0] == {
        "decision_candidate": "isin_shared_by_distinct_issuers",
        "registered_country": "Canada",
        "collision_groups": 2,
    }
