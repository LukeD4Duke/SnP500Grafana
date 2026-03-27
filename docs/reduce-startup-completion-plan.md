# Reduce Populated-DB Startup Completion Time

## Summary

Change populated-DB startup to become ready immediately after schema init and startup incremental OHLCV sync. Move indicator refresh, analytics refresh, and report generation off the blocking startup path and run them as background one-shot jobs. Refresh only symbols touched by the startup sync, not the full universe.

Keep empty-DB behavior unchanged: first boot still performs the full historical load before the service is considered initialized. That is the current product contract and the main remaining long-startup case.

## Key Changes

- **Startup sequencing**
  - For populated DBs, keep `startup incremental sync` as the only blocking post-schema step.
  - After that sync completes, start the scheduler immediately.
  - Queue a one-shot background job to run:
    1. indicator refresh for `startup_result.successful_symbols`
    2. analytics snapshot refresh
    3. report generation
  - Do not run `refresh_indicators()`, `refresh_analytics()`, or `generate_configured_reports()` inline during populated-DB startup.

- **Indicator scope**
  - Preserve the existing “touched symbols only” input from the startup incremental sync.
  - Do not expand startup refresh to all symbols on populated DBs.
  - Keep split-triggered full rebuild behavior per affected symbol.
  - Keep `INDICATOR_REBUILD_ON_STARTUP` meaningful only for explicit full rebuild cases; it should not force a full-universe populated-DB startup block.

- **Scheduler behavior**
  - Reuse APScheduler rather than adding another worker/service.
  - Add a one-shot startup catch-up job for populated DBs using a near-immediate trigger.
  - Prevent overlap with the scheduled daily update job by using a distinct job id and `max_instances=1`, or equivalent locking/guarding, so startup catch-up and normal scheduled refresh cannot run concurrently.

- **Analytics and reports**
  - Run analytics and report generation only after the background indicator job completes.
  - Keep report outputs and analytics table contracts unchanged.
  - Accept that dashboards may be briefly stale after restart until the startup catch-up job finishes.

- **Config surface**
  - Make fast-start behavior the new default.
  - Add one explicit config switch to restore legacy blocking behavior if needed, for example:
    - `STARTUP_POST_SYNC_MODE=background|blocking`
    - default: `background`
  - Update compose, config parsing, and README together.

## Test Plan

- **Unit / behavior checks**
  - Populated DB startup:
    - schema init runs
    - startup incremental sync runs
    - scheduler starts without waiting for indicators
    - one-shot startup catch-up job is scheduled
  - Background catch-up job:
    - indicator refresh receives only touched symbols
    - analytics and reports run after indicators
    - no overlap with daily scheduled sync
  - Legacy mode:
    - `STARTUP_POST_SYNC_MODE=blocking` preserves current inline behavior

- **Runtime verification**
  - Rebuild and start with a populated DB.
  - Confirm logs show:
    - startup incremental sync completes
    - scheduler starts immediately after sync
    - startup catch-up runs afterward in background
  - Confirm Grafana becomes reachable before indicator refresh finishes.
  - Confirm analytics/report outputs appear after the background job completes.

- **Regression checks**
  - Empty DB still performs full historical load first.
  - Backfill flow still works and may remain blocking unless explicitly optimized later.
  - Split-triggered per-symbol full indicator rebuild still occurs.

## Assumptions

- The main goal is faster readiness for populated DB restarts, not first-time bootstrap.
- Temporary post-restart dashboard staleness is acceptable.
- The new default should be fast-start background processing, with one config switch to opt back into legacy blocking behavior.
