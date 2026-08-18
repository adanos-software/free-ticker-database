-- Canonical v4 PostgreSQL schema. CSV headers are governed by
-- schema/canonical_v4_contract.json and validated before loading.

create table sources (
  source_id uuid primary key,
  source_key text unique not null,
  provider text not null,
  source_url text not null,
  authority_level text not null,
  reference_scope text not null,
  license_status text not null,
  license_name text,
  license_url text,
  derived_facts_redistribution_status text not null,
  raw_redistribution_allowed boolean not null,
  attribution_required text not null,
  commercial_use_status text not null,
  terms_version text,
  terms_sha256 text,
  license_reviewed_at timestamptz,
  freshness_sla_days integer not null check (freshness_sla_days > 0),
  enabled boolean not null
);

create table source_observations (
  observation_id uuid primary key,
  source_id uuid not null references sources(source_id),
  source_key text not null,
  source_record_id text,
  observed_at timestamptz not null,
  effective_at timestamptz,
  raw_uri text not null,
  raw_sha256 text,
  normalized_rows_sha256 text not null,
  parser_name text not null,
  parser_version text not null,
  parse_status text not null
);

create table venues (
  venue_id uuid primary key,
  exchange_code text unique not null,
  operating_mic char(4),
  segment_mic char(4),
  canonical_name text not null,
  country_code char(2),
  status text not null
);

create table issuers (
  issuer_id uuid primary key,
  legal_name text not null,
  normalized_name text not null,
  lei char(20),
  domicile_country_code char(2),
  status text not null
);

create table instruments (
  instrument_id uuid primary key,
  issuer_id uuid references issuers(issuer_id),
  isin char(12),
  asset_type text not null,
  security_type text not null,
  name text not null,
  country_code char(2),
  status text not null,
  valid_from timestamptz not null,
  valid_to timestamptz
);

create table listings (
  listing_id uuid primary key,
  listing_key text not null,
  instrument_id uuid not null references instruments(instrument_id),
  venue_id uuid not null references venues(venue_id),
  local_symbol text not null,
  is_primary boolean not null,
  status text not null,
  valid_from timestamptz not null,
  valid_to timestamptz,
  current boolean not null,
  source_observation_id uuid references source_observations(observation_id),
  unique(listing_key, valid_from)
);

create table identifier_assertions (
  assertion_id uuid primary key,
  entity_type text not null check (entity_type in ('issuer','instrument','listing')),
  entity_id uuid not null,
  scheme text not null,
  value text not null,
  observation_id uuid references source_observations(observation_id),
  confidence numeric(5,4) not null,
  adjudication_status text not null,
  valid_from timestamptz,
  valid_to timestamptz
);

create table field_assertions (
  assertion_id uuid primary key,
  entity_type text not null,
  entity_id uuid not null,
  field_name text not null,
  field_value text not null,
  observation_id uuid not null references source_observations(observation_id),
  confidence numeric(5,4) not null,
  adjudication_status text not null,
  valid_from timestamptz,
  valid_to timestamptz
);

create table provenance_gaps (
  gap_id uuid primary key,
  entity_type text not null,
  entity_id uuid not null,
  listing_key text not null,
  field_name text not null,
  current_value text,
  gap_class text not null,
  required_evidence text not null
);

create table listing_events (
  event_id uuid primary key,
  listing_id uuid not null references listings(listing_id),
  listing_key text not null,
  event_type text not null,
  field_name text,
  old_value text,
  new_value text,
  effective_at timestamptz not null,
  observed_at timestamptz not null,
  observation_id uuid not null references source_observations(observation_id),
  evidence_status text not null
);

create table coverage_contracts (
  contract_id uuid primary key,
  contract_key text unique not null,
  exchange text not null,
  asset_type text not null,
  claim_type text not null,
  source_keys text not null,
  denominator_method text not null,
  denominator integer not null,
  covered_reference_keys integer not null,
  missing_reference_keys integer not null,
  identity_conflict_keys integer not null,
  recall_pct numeric(7,4),
  freshness_status text not null,
  license_status text not null,
  contract_status text not null,
  generated_at timestamptz not null,
  check (denominator >= 0 and covered_reference_keys >= 0 and missing_reference_keys >= 0)
);

create index listings_current_symbol_idx on listings(venue_id, local_symbol) where current;
create index instruments_isin_idx on instruments(isin) where isin is not null;
create index identifier_assertions_value_idx on identifier_assertions(scheme, value);
create index field_assertions_entity_idx on field_assertions(entity_type, entity_id, field_name);
create index source_observations_source_idx on source_observations(source_id, observed_at desc);
