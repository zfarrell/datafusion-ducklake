# Documentation Index

## Current Documents

| Document | Purpose | Last Updated |
|----------|---------|-------------|
| [remaining-work-audit.md](remaining-work-audit.md) | Comprehensive remaining work audit with cross-document reconciliation. The authoritative source for current project status. | Feb 28 (commit d332b28) |
| [slt-failure-report.md](slt-failure-report.md) | SQLLogicTest failure analysis: 120/248 passing (48.4%), categorized by root cause and fixability. | Feb 28 |
| [duckdb-behavior-reference.md](duckdb-behavior-reference.md) | Reference document for DuckDB + DuckLake behavior (DDL, DML, types, time travel, metadata functions). Tested against DuckDB v1.4.4 + DuckLake v0.3. | Feb 22 |
| [ducklake-issues-analysis.md](ducklake-issues-analysis.md) | Analysis of upstream DuckLake GitHub issues and their impact on our implementation. | Feb 22 |
| [edge-case-findings.md](edge-case-findings.md) | Results from 33 edge case tests (32 passed, 1 failed). Documents specific findings and confirmed correct behaviors. | Feb 22 |
| [EXCLUDED_TESTS.md](EXCLUDED_TESTS.md) | Catalog of DuckLake SLT tests not ported to DataFusion, with exclusion reasons per test. | Feb 28 |
| [testing-strategy.md](testing-strategy.md) | Four-tier testing strategy (SLT compat, DataFusion contracts, behavioral verification, unit tests). | Feb 22 |
| [test-portability-survey.md](test-portability-survey.md) | Portability survey of all 342 DuckLake SLT test files, categorized as A (portable), B (needs adaptation), C (not portable). | Feb 22 |

## Root-Level Documents

| Document | Purpose |
|----------|---------|
| [CHANGELOG.md](../CHANGELOG.md) | Standard changelog (Keep a Changelog format). |
| [README.md](../README.md) | Project README. |
| [CLAUDE.md](../CLAUDE.md) | Claude Code project instructions. |

## Legacy Documents (docs/legacy/)

Moved here because their content is >50% inaccurate relative to current code state, or they were superseded by newer documents.

| Document | Why Legacy | Superseded By |
|----------|-----------|---------------|
| [remaining-gaps.md](legacy/remaining-gaps.md) | 6+ of 14 gaps already resolved (Postgres/MySQL methods, file pruning, compaction, NOT NULL, snapshot refresh). Written before commits 4f73c9b, 5f66562, 995419c, 48a648f. | remaining-work-audit.md |
| [gap-analysis.md](legacy/gap-analysis.md) | Shows DELETE, UPDATE, DROP TABLE, Views, ALTER TABLE, Virtual Columns, Column Statistics, Complex Types, NOT NULL as "NOT IMPLEMENTED" -- all now implemented. | remaining-work-audit.md |
| [phase4-review-plan.md](legacy/phase4-review-plan.md) | Process plan for Phase 4 review, which has been completed. | N/A (completed process) |
| [CHANGES.md](legacy/CHANGES.md) | Specific work session changes from Feb 22. | CHANGELOG.md |
| [FINAL_VALIDATION_REPORT.md](legacy/FINAL_VALIDATION_REPORT.md) | Self-labeled "SUPERSEDED" on line 1. Phase 1 validation. | RE_VALIDATION_REPORT.md (also legacy) |
| [PHASE4_FINDINGS.md](legacy/PHASE4_FINDINGS.md) | Phase 4 testing findings from Feb 22. | remaining-work-audit.md |
| [PROGRESS.md](legacy/PROGRESS.md) | Progress tracking from Feb 22. References "uncommitted Phase 5 fixes" and stale test counts. | remaining-work-audit.md |
| [RE_VALIDATION_REPORT.md](legacy/RE_VALIDATION_REPORT.md) | Phase 3 re-validation from Feb 22. Superseded by later fixes. | remaining-work-audit.md |
| [VALIDATION_REPORT.md](legacy/VALIDATION_REPORT.md) | Phase 1 validation from Feb 22. Contains inaccurate counts. | FINAL_VALIDATION_REPORT.md (also legacy) |
| [WORK_LOG.md](legacy/WORK_LOG.md) | Session work log tracking Phases 1-5 from Feb 22. | remaining-work-audit.md |
