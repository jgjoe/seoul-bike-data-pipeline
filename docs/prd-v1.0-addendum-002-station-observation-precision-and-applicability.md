# PRD v1.0 Addendum 002 — station observation precision and applicability

Status: **FINAL / Accepted 2026-09-21**

This addendum clarifies PRD §14 observed station-history semantics using the actual Seoul station snapshot publication pattern. The frozen PRD body remains unchanged.

## Source precision

Official station snapshots do not all provide day-level observation dates. The pipeline preserves the precision supplied by the source instead of inventing a more precise date.

- `observation_period` stores `YYYY-MM-DD` for day precision or `YYYY-MM` for month precision.
- `observation_precision` is `DAY` or `MONTH`.
- A `MONTH` observation must not be rewritten as an invented month-end observation date.

## Applicability boundary

Observation provenance and fact-enrichment applicability are separate concepts.

- `DAY` observation: `applicable_from` is that calendar date.
- `MONTH` observation: `applicable_from` is the first day of the following month.
- The derived `applicable_from` for a `MONTH` observation is a conservative join boundary, not a claimed real-world observation date.
- `applicable_until` is exclusive. Station-history intervals are `[applicable_from, applicable_until)`.
- The final known interval has `applicable_until = null`.

The PRD §14.3 names `observed_from` / `observed_until` are superseded for implementation by `applicable_from` / `applicable_until` so a derived join boundary is not mistaken for source observation provenance.

## Missing observations and interval compression

- If a station appears in one snapshot and is absent from the next snapshot, absence is not interpreted as closure.
- The previous applicable interval ends at the next snapshot applicability boundary and the missing span is an observation gap.
- A previous state is not carried forward across an observation gap.
- Consecutive snapshots with the same canonical station state may be compressed into one `dim_station_history` interval while the underlying snapshot observations remain preserved separately.

## Same-snapshot consolidation

The same canonical `station_no` may appear more than once in a snapshot.

- Identity attributes may be consolidated when non-null values agree. A null value may be filled from another row in the same station group.
- If two or more different non-null identity values exist for the same identity field, the whole station group is quarantined and no canonical winner is chosen.
- Complementary LCD and QR rows are consolidated. Canonical `operation_mode` is one of `LCD`, `QR`, `LCD+QR`, or `null`.
- An exact duplicate source row may collapse into one consolidated observation with a WARNING and duplicate count; it is not quarantined solely for being an exact duplicate.
- `설치시기` remains source-row provenance. It does not participate in station identity conflict detection or canonical state equality because official complementary rows can contain different installation dates.
- Canonical station-state equality for v1 uses: `station_name`, `district`, `address`, `latitude`, `longitude`, `lcd_capacity`, `qr_capacity`, `operation_mode`.

## Station row and snapshot failure boundary

- Canonical `station_no` follows the existing station-number rule: trim surrounding whitespace, require decimal digits, and strip leading zeroes (`lstrip("0") or "0"`).
- A row with missing/invalid canonical `station_no` is station-DQ quarantine; other structurally valid station rows in that snapshot may still be processed.
- Unknown station source schema, unreadable artifact, or structural row-width / worksheet interpretation failure is a snapshot-level HARD FAIL.

## Slice boundary

The next bounded implementation slice ends after station snapshot ingestion, canonical source-row observation, same-snapshot consolidation, station DQ/reconciliation, and durable immutable snapshot publish. Cross-snapshot `dim_station_history` construction is a later slice, followed by trip enrichment.

This addendum preserves P-05: station history is observed snapshot history and must not imply exact real-world station change dates.
