# PRD v1.0 Addendum 001 — `station_history_match` nullability

Status: **FINAL / Accepted 2026-09-21**

This addendum clarifies the type/meaning of `station_history_match` in [PRD v1.0 FINAL](prd-v1.0-final.md) without changing P-05 observed-history semantics.

## Clarification

For Clean canonical rows, `station_history_match` is a **nullable boolean** until station-history enrichment is performed.

| Value | Meaning |
|---|---|
| `null` | station-history enrichment has not yet been evaluated for this Clean row |
| `true` | enrichment was evaluated and an applicable prior station observation was found |
| `false` | enrichment was evaluated and no applicable prior station observation was found |

`station_history_match = false` is therefore an evaluated no-match condition and is the state that carries the station-unmatched WARNING described by PRD §14.4. `null` must not be interpreted as an unmatched station.

## Scope

- PRD §16 field type for `station_history_match` is clarified from `boolean` to `nullable boolean` at the Clean stage.
- Slice 9 Clean Parquet publish writes `station_history_match = null` because station-history enrichment remains a later warehouse/enrichment stage.
- Future station-history enrichment may replace `null` with `true` or `false` according to PRD §14.4.
- P-05 remains unchanged: station history is snapshot-observed history and must not imply exact real-world change dates.
- `station_history_match = false` remains a WARNING condition and does not by itself change `trusted_fact_eligible`.

The frozen PRD body is intentionally not rewritten; this addendum is the explicit clarification record.
