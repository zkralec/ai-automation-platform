# Jobs System v2 Hardening Notes

## Scope

Hardening focused on stage-level reliability and contract stability for:
- `jobs_collect_v1`
- `jobs_normalize_v1`
- `jobs_rank_v1`
- `jobs_shortlist_v1`
- `jobs_digest_v2`

Current supported active collection sources:
- `linkedin`
- `indeed`

Current collection control model:
- minimum breadth targets can be set with `minimum_raw_jobs_total`, `minimum_unique_jobs_total`, and `minimum_jobs_per_source`
- `result_limit_per_source` and `max_total_jobs` remain safety caps
- collection observability records whether the minimum was reached and why collection stopped

Legacy disabled sources may still appear in older configs, but they are ignored safely and excluded from active-source summaries.

## Coverage Added

### `jobs_collect_v1`
- Multi-source success path using fixture data (`linkedin`, `indeed`)
- Empty successful collection (zero jobs, no source errors)
- Existing coverage retained for partial source failure and all-sources-failed retryable error

### `jobs_normalize_v1`
- Empty upstream raw-job input contract behavior
- Duplicate-heavy upstream input to validate collapse/duplicate group reporting
- Existing cross-source dedupe + ambiguous-case coverage retained

### `jobs_rank_v1`
- Empty normalized-job input contract behavior
- Strict-mode malformed LLM output failure path (all retries exhausted -> retryable runtime error)
- Existing structured LLM success and malformed-then-retry-success coverage retained

### `jobs_shortlist_v1`
- Empty scored-job input contract behavior
- Duplicate-heavy scored input to validate anti-repetition and per-company cap rejection tracking
- Existing compatibility/freshness weighting coverage retained

### `jobs_digest_v2`
- Empty shortlist behavior (digest artifacts still produced, notify skipped by policy)
- Fallback digest output shape check for UI stability
- Existing structured LLM success, malformed fallback, and strict malformed failure coverage retained

### Worker reliability (cross-stage)
- `jobs_digest_v2` success with simulated notify follow-up enqueue failure:
  - digest task remains `success`
  - digest `result.json` artifact is persisted
  - notify follow-up absence does not erase digest output

## Fixture Data

Added reusable fixture file:
- `worker/tests/fixtures/jobs_v2_samples.json`

Fixture sections:
- `collect_multisource_by_source`
- `normalize_duplicate_heavy_raw_jobs`
- `shortlist_duplicate_heavy_scored_jobs`
- `digest_top_jobs_sample`

## Remaining Weak Spots / Follow-up

1. Add a full DB-backed sequential pipeline test (`collect -> normalize -> rank -> shortlist -> digest`) that executes handlers through `worker.run_task` for all stages, not only digest+notify failure handling.
2. Add explicit schema-evolution guard tests for artifact key stability (snapshot/golden-file style) to detect accidental UI contract drift.
3. Add source-adapter contract tests that verify each collector returns the expected `SUPPORTED_FIELDS` and mandatory raw fields (`source`, `source_url`, `title`) under failure and success.
