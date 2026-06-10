# Twelve Data Source Name Updates 2

Package 2 applies validation-safe openfigi_only Common Stock/REIT name updates after excluding ADR/depositary scope-sensitive rows, package 1 local_figi_match rows, and validation side effects.

- Name updates: 62
- Attempted before validation filter: 74
- Evidence: OpenFIGI-only name support; no local FIGI conflict because local FIGI is absent
- Scope: Common Stock and REIT only; ADR/depositary rows remain excluded for a dedicated scope-safe package
- Excluded after validation filter: 12 rows

## Exchanges

| Exchange | Rows |
| --- | ---: |
| OTC | 59 |
| NASDAQ | 1 |
| NYSE | 1 |
| TSXV | 1 |

## FIGI Relation

| Relation | Rows |
| --- | ---: |
| openfigi_only | 62 |

## Provider Support

| Provider | Rows |
| --- | ---: |
| OpenFIGI | 60 |
| OpenFIGI|AlphaVantage | 2 |

## Types

| Type | Rows |
| --- | ---: |
| Common Stock | 58 |
| REIT | 4 |

## Validation Exclusions

OTC::FMTYF, OTC::IFUUF, OTC::KYDKF, OTC::MBGAF, OTC::NWHUF, OTC::OQLGF, OTC::PIERF, OTC::SHPPF, OTC::WDGNF, TSXV::AXO, TSXV::KLX, TSXV::MKT
