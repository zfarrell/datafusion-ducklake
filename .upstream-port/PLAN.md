# Upstream-port plan

Long-lived plan document for the carry-forward of `ducklake-features/integration` into
upstream `datafusion-contrib/datafusion-ducklake`. Lives on a meta branch (not on
`main`) so it neither pollutes the upstream-tracking baseline nor gets rebased by
in-flight feature work.

- **Audit & background:** GitHub issue [#26](https://github.com/zfarrell/datafusion-ducklake/issues/26)
  (meta tracker) and [#27](https://github.com/zfarrell/datafusion-ducklake/issues/27)
  (SLT triage).
- **Source-of-truth branches** for each workstream live on the fork under
  `feat/<NN>-…`. They are stacked off `upstream-port/integration` (the foundation
  rebase) and contain the per-ticket work + status comments.

## The staging model

For each workstream:

1. Construct a **focused PR** off `zfarrell:main` carrying only the files/changes
   that workstream owns (new files + targeted modifications to shared files).
   Suggested branch name: `pr/<NN>-<slug>`.
2. **Trifecta-review** the fork PR: Sonnet code review + Codex code review +
   manual exercise. Resolve findings TDD-style.
3. **Merge** the fork PR once clean (acts as the recorded review trail).
4. **Open the equivalent against upstream** (`datafusion-contrib/datafusion-ducklake`).
   The fork PR + audit issue (#26) + trifecta comments are the context.

`zfarrell:main` is the baseline for fork PRs. It tracks `upstream:main` 1:1, plus
whatever fork PRs have already been merged through this process. Each fork PR
rebases onto the latest `zfarrell:main` before review.

## Why not a single foundation PR

The earlier proposal was a "foundation" PR carrying the entire integration tree
(~83k LOC). On inspection, every line in that tree belongs to a specific feature
ticket — there is no shared infrastructure that doesn't have a feature home. The
"cleanups" the foundation port performed (empty test scaffolds, dropped
`compaction_functions.rs`, `docs/` → `.audit/`) were all cleanups of content
that came IN with the integration tree port and don't apply when each feature
ships on its own.

## Suggested PR sequence (dependency order)

| # | Fork PR branch | Touches | Depends on |
|---|---|---|---|
| 25 | `pr/25-footer-size` | `src/table_writer.rs`, `src/parquet_meta.rs` (new), 6 read sites | — |
| 13 | `pr/13-dialect-macros` | `src/dialect.rs`, `src/metadata_writer_impl.rs`, `src/metadata_provider_impl.rs`, `src/metadata_writer_validation.rs` (all new), refactor of `metadata_writer_sqlite.rs` + `metadata_provider_sqlite.rs` | — |
| 16 | `pr/16-query-planner` | `src/query_planner.rs` (new), `lib.rs` export, `catalog.rs`/`table.rs` hooks | — |
| 17 | `pr/17-delete-exec` | `src/delete_exec.rs` (new), `MetadataWriter::register_dml_files` trait additions, `table.rs` DELETE wiring | #16 |
| 18 | `pr/18-update-exec` | `src/update_exec.rs` (new), `src/config.rs` (new — `DuckLakeConfig`), `table.rs` UPDATE wiring | #16, #17 |
| 19 | `pr/19-merge-exec` | `src/merge_exec.rs` (new), MERGE wiring | #16, #17, #18 |
| 20 | `pr/20-ddl` | New DDL methods on `MetadataWriter` (via #13 macros), `catalog.rs`/`schema.rs` wiring | #13 |
| 21 | `pr/21-cdc` | `src/cdc_common.rs`, `src/table_insertions.rs` (new), bug fixes to existing `table_changes.rs`/`table_deletions.rs`, `delete_file_schema` nullable | — |
| 14 | `pr/14-postgres-writer` | `src/metadata_writer_postgres.rs` (new) | #13 |
| 15 | `pr/15-mysql-writer` | `src/metadata_writer_mysql.rs` (new) | #13 |
| 23 | `pr/23-types` | `types.rs` additions, `parse_values.rs` (new), round-trip tests | — |
| 22 | `pr/22-virtual-columns` | `src/virtual_column_exec.rs` (new), `row_id.rs` modifications, scan-stack composition in `table.rs`, `with_row_lineage` on catalog/schema | — |

#22 is sequenced last because of the intricate scan-stack composition + the
`DeleteFilterExec`-above-`RowIdExec` ordering invariant the carry-forward
discovered. Better to land in isolation when nothing else is moving the
read-side wiring.

#24 (R10/R11 hardening) has no code changes — the verification ticket confirmed
all 24 R-fixes survived the rebase. No PR needed.

## Where each workstream's pre-PR context lives

| Workstream | Fork ref | Status comment |
|---|---|---|
| #12 foundation | `zfarrell/upstream-port/integration` | [#12](https://github.com/zfarrell/datafusion-ducklake/issues/12#issuecomment-4522137534) |
| #13 dialect/macros | `zfarrell/feat/13-dialect-macro-layer` | [#13](https://github.com/zfarrell/datafusion-ducklake/issues/13#issuecomment-4522298165) |
| #14 PG writer | `zfarrell/feat/14-postgres-writer` | [#14](https://github.com/zfarrell/datafusion-ducklake/issues/14#issuecomment-4523963189) |
| #15 MySQL writer | `zfarrell/feat/15-mysql-writer` | [#15](https://github.com/zfarrell/datafusion-ducklake/issues/15#issuecomment-4524029097) |
| #16 query planner | `zfarrell/feat/16-query-planner` | [#16](https://github.com/zfarrell/datafusion-ducklake/issues/16#issuecomment-4522522269) |
| #17 DELETE | `zfarrell/feat/17-delete-exec` | [#17](https://github.com/zfarrell/datafusion-ducklake/issues/17#issuecomment-4522682837) |
| #18 UPDATE | `zfarrell/feat/18-update-exec` | [#18](https://github.com/zfarrell/datafusion-ducklake/issues/18#issuecomment-4522792899) |
| #19 MERGE | `zfarrell/feat/19-merge-exec` | [#19](https://github.com/zfarrell/datafusion-ducklake/issues/19#issuecomment-4523057...) |
| #20 DDL | `zfarrell/feat/20-ddl` | [#20](https://github.com/zfarrell/datafusion-ducklake/issues/20#issuecomment-4523860419) |
| #21 CDC | `zfarrell/feat/21-cdc-functions` | [#21](https://github.com/zfarrell/datafusion-ducklake/issues/21#issuecomment-4523465613) |
| #22 virtual columns | `zfarrell/feat/22-virtual-columns` | [#22](https://github.com/zfarrell/datafusion-ducklake/issues/22#issuecomment-4523801800) |
| #23 types | `zfarrell/feat/23-types` | [#23](https://github.com/zfarrell/datafusion-ducklake/issues/23#issuecomment-4523928902) |
| #24 hardening (verify only) | n/a | [#24](https://github.com/zfarrell/datafusion-ducklake/issues/24#issuecomment-4523899034) |
| #25 write path | `zfarrell/feat/25-write-path` (carries broader work — only the footer-size slice is on `pr/25-footer-size`) | [#25](https://github.com/zfarrell/datafusion-ducklake/issues/25#issuecomment-4523337359) |

## Real correctness bugs surfaced during the carry-forward (beyond the original audit)

These all became tests in their respective fork PRs:

1. Concurrent DELETE silently dropped earlier transactions' rows (#17 — fixed via `since_snapshot` trait plumbing)
2. CDC duplicate-column projection collapsed two output slots to one source position; its test asserted the buggy behavior (#21)
3. CDC delete-file schema was passed as the data-file `ParquetSource`, producing `"Non-nullable column 'pos' is missing"` (#21)
4. `DeleteFilterExec` must be ABOVE `RowIdExec` in the scan stack or rowids skew under deletes (#22)
5. `EquivalenceProperties` not propagated through virtual-column execs → `ORDER BY` on virtual columns failed `SanityCheckPlan` (#22)
6. **Parquet footer-size off by 8 bytes** (#25 — single biggest fix; cleared 34 cross-engine tests at once)
7. PG fresh-DB requires `CREATE EXTENSION pgcrypto`; `setval(seq, 0)` rejected (Postgres sequences start at 1) (#14)
8. MySQL `INSERT...SELECT MAX(...)` can't self-reference the target table (error 1093) (#15)
9. **(Trifecta on #25)** Read-side prefetch hint regression — the footer-size fix corrected the catalog value but the 6 read sites that consumed it still needed the +8 trailer for the one-fetch optimization. Caught by independent Sonnet + Codex reviews. Fixed in PR #28's second commit.

## Open follow-ups not yet attacked

- **PG cross-engine footer asymmetry.** #14 saw 4/8 cross-engine tests pass; #15 saw 8/8 on MySQL. The remaining PG failures all hit DuckDB-reads-DF-written-Parquet, despite #25's footer fix being in the same tree. Likely a PG-specific path bypassing the fix — worth tracking as its own issue.
- **SLT triage in #27.** 96 failures categorized into 24 buckets. Largest are nested-type DDL (12), data-inlining hybrid limitations (12), DuckLake-extension macros not supporting functions (9 — upstream blocker, not our problem). Attack as ad-hoc PRs once the carry-forward lands.

## Out-of-scope, explicitly

- `compaction_functions.rs` from the integration tree — dropped per #12. Was a DuckDB pass-through. A native compaction module is a future effort, not this carry-forward.
- The 56-file `docs/` from the integration branch (R1–R11 retrospectives, gap analyses) — agent process artifacts. Lives in `.audit/` on `upstream-port/integration`; does not go to upstream.

## Progress log

- **2026-05-22:** Audit complete (`#26`). Tickets created.
- **2026-05-22 → 2026-05-23:** Workstreams executed on `feat/<NN>` branches, status comments posted to each ticket. Real bugs surfaced (see list above).
- **2026-05-23:** Stale fork PRs `#1`–`#11` closed.
- **2026-05-23:** First focused PR `#28` (footer-size fix) opened against `zfarrell:main`. Trifecta-review found a perf-regression follow-up (read-side `+8`); fixed via 2 additional commits on the same PR.
