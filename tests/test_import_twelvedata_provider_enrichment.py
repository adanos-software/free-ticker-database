from scripts.import_twelvedata_provider_enrichment import eodhd_exchange_matches


def test_eodhd_exchange_match_requires_otc_provider_venue_for_pink_mic() -> None:
    row = {"source_mic": "PINX"}

    assert eodhd_exchange_matches(row, "OTC")
    assert not eodhd_exchange_matches(row, "NASDAQ")
