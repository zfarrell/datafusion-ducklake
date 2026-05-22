# Phase 4: Integration, Review & Testing Plan

This plan executes after all Phase 3 implementation agents report complete. It covers merging, code review, and multi-angle testing before declaring the work ready for upstream PRs.

---

## PR Strategy

**Goal**: Produce independent, reviewable PRs that tell a coherent story of iterative development — not one giant merge.

Each PR should:
- Be self-contained (builds, tests pass, no dangling references)
- Have a clear scope (one feature or closely related group)
- Build on prior PRs in a logical sequence
- Include both implementation and tests in the same PR

### Proposed PR Sequence

The order matters — later PRs may depend on earlier ones.

```
PR 1: DROP TABLE + DROP SCHEMA + NOT NULL constraints
       (impl-drop's work from ducklake-features/drop-and-constraints + tests)

PR 2: Complex types (LIST, STRUCT, MAP)
       (read-path only, no write dependency)

PR 3: DELETE support
       (MetadataWriter.register_delete_file + DuckLakeDeleteExec + tests)

PR 4: UPDATE support
       (depends on PR 3's delete infrastructure)

PR 5: Transaction conflict detection
       (ducklake_snapshot_changes table + checked write methods)

PR 6: ALTER TABLE (ADD/DROP/RENAME COLUMN, type changes)
       (depends on PR 5 for change tracking)

PR 7: Views (CREATE VIEW, DROP VIEW, persistent view metadata)

PR 8: CREATE SCHEMA

PR 9: Column statistics write + read
       (depends on PR 1+ for MetadataWriter patterns)

PR 10: SQL DML support (DuckLakeQueryPlanner for DELETE/UPDATE via SQL)
        (depends on PRs 3+4)

PR 11: Ported sqllogic tests
        (can go with any PR or standalone)
```

This sequence can be adjusted after review. Some PRs may be combined if they're small. The key constraint is that each PR must compile and pass tests independently when applied on top of the previous ones.

### Integration Branch

We merge all worktree work into `ducklake-features` as a shared integration branch for testing and review. This branch is NOT pushed directly — it exists to verify everything works together. Individual PRs are then constructed by cherry-picking or replaying commits from this branch onto feature branches off `main`.

---

## Step 1: Integration

Each implementation agent worked on an isolated git worktree. Before review or testing, all work must be merged onto a single branch.

### Integrator Agent

**Goal**: Produce a single `ducklake-features` integration branch with all work merged, building cleanly, all tests passing. Then prepare independent PR branches.

**Approach**:
1. Inventory all worktree branches and their commits
2. Determine merge order based on the PR sequence above (this is also the dependency order)
3. Merge branches one at a time, resolving conflicts
4. After each merge: `cargo build --all-features && cargo test --all-features`
5. Final verification: full test suite green, no warnings, clippy clean
6. For each planned PR, create a branch off `main` (or the previous PR branch) with just that PR's changes
7. Verify each PR branch builds and tests pass independently

**Shared file conflict hotspots** (multiple agents modified these):
- `src/lib.rs` — module registrations, re-exports
- `src/metadata_writer.rs` — trait method additions
- `src/metadata_provider.rs` — trait method additions, struct changes
- `src/table.rs` — new methods (delete, update)
- `src/schema.rs` — view handling, deregister changes
- `src/metadata_writer_sqlite.rs` — new method implementations

**This is the critical path** — nothing else starts until the integration branch is green.

---

## Step 2: Code Review (parallel)

All reviewers work read-only on the merged branch. They file findings as structured reports, not code changes. A separate fix-it agent addresses blocking issues.

### arch-reviewer — Architecture & Design
- Module boundaries and separation of concerns
- Consistency of patterns across features (do DELETE, UPDATE, ALTER all follow the same structural pattern?)
- MetadataWriter trait surface area — is it getting too large? Should it be split?
- MetadataProvider trait — same question
- Error type design — is DuckLakeError covering all cases cleanly?
- Abstraction quality — are there premature abstractions or missing ones?
- Dependency direction — does the module graph make sense?

### datafusion-reviewer — DataFusion Idioms
- Correct implementation of TableProvider, CatalogProvider, SchemaProvider contracts
- QueryPlanner integration — is DuckLakeQueryPlanner the right extension point?
- Async patterns — are we blocking the runtime anywhere? Unnecessary `.await`?
- ExecutionPlan implementations — do DuckLakeDeleteExec, UpdateExec, InsertExec follow DataFusion's execution model correctly?
- Schema handling — proper use of Arrow schemas, field metadata
- Filter pushdown — are we correctly reporting Inexact and handling reapplication?
- Session/state management — proper use of SessionState, SessionContext
- Are we fighting DataFusion's APIs anywhere? Could something be simpler?

### rust-reviewer — Rust Quality
- Ownership and borrowing — unnecessary clones, Arc usage
- Error handling — proper propagation, no swallowed errors, descriptive messages
- Clippy compliance (`cargo clippy --all-features`)
- Code style consistency with existing codebase
- Thread safety — Mutex usage, Send/Sync bounds
- Unsafe code (should be zero)
- Test quality — are tests actually testing what they claim?
- Feature gate correctness — is all write code properly gated?

### security-reviewer — Safety & Correctness
- SQL injection in metadata queries (parameterized queries vs string formatting)
- Path traversal in file path resolution
- Error message information leakage (do errors expose internal paths or metadata?)
- Transaction safety — can concurrent operations corrupt metadata?
- Race conditions in conflict detection
- Input validation — table names, schema names, column names, type strings
- Denial of service — can a malicious catalog cause unbounded memory/CPU?

---

## Step 3: Testing (parallel, after integration)

### parity-tester — DuckDB Equivalence (Ground Truth)
Run identical SQL sequences against both DuckDB+DuckLake and DataFusion+DuckLake, diff results.

**Test categories**:
- INSERT → SELECT (various types, NULL handling, multiple rows)
- DELETE → SELECT (with WHERE, without WHERE, no matching rows)
- UPDATE → SELECT (SET single column, multiple columns, with/without WHERE)
- CREATE/DROP TABLE (basic, IF EXISTS, recreate after drop)
- CREATE/DROP SCHEMA (empty, CASCADE, non-empty without CASCADE)
- Type handling (all supported types, edge values, precision)
- NULL behavior (NULL in various contexts, IS NULL, COALESCE)
- Views (CREATE VIEW, SELECT from view, DROP VIEW)
- ALTER TABLE (ADD/DROP/RENAME COLUMN, type changes)
- Information schema queries

**Approach**: For each test, run against DuckDB first to get expected output, then run against DataFusion and compare. Document any intentional divergences (DataFusion-specific behavior that's correct but different from DuckDB).

### edge-case-tester — Boundary Conditions
- Empty tables (SELECT, DELETE, UPDATE on zero-row table)
- All-NULL columns
- Unicode strings (emoji, CJK, RTL, zero-width chars)
- Very large values (max int, very long strings, high-precision decimals)
- Zero-row DELETE (WHERE matches nothing)
- UPDATE with no actual changes (SET col = col)
- DROP then recreate with same name
- DROP then recreate with different schema
- Deeply nested complex types (LIST(LIST(STRUCT(...))))
- Tables with many columns (100+)
- Tables with many files
- Concurrent operations (parallel INSERTs, DELETE during SELECT)
- Transaction conflict scenarios

### e2e-scenario-tester — Realistic Workflows
Multi-step scenarios simulating real usage:

**Scenario 1: Data Pipeline**
```
CREATE SCHEMA analytics
CREATE TABLE analytics.events (id INT, type VARCHAR, ts TIMESTAMP)
INSERT INTO analytics.events VALUES (...)  -- multiple batches
CREATE VIEW analytics.recent_events AS SELECT * FROM analytics.events WHERE ts > '2024-01-01'
SELECT * FROM analytics.recent_events
UPDATE analytics.events SET type = 'processed' WHERE type = 'raw'
SELECT COUNT(*) FROM analytics.events WHERE type = 'processed'
ALTER TABLE analytics.events ADD COLUMN source VARCHAR
INSERT INTO analytics.events VALUES (..., 'api')
SELECT * FROM analytics.events
```

**Scenario 2: Schema Evolution**
```
CREATE TABLE users (id INT NOT NULL, name VARCHAR)
INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')
ALTER TABLE users ADD COLUMN email VARCHAR
INSERT INTO users VALUES (3, 'Charlie', 'charlie@example.com')
SELECT * FROM users  -- Alice and Bob should have NULL email
ALTER TABLE users RENAME COLUMN name TO full_name
SELECT full_name FROM users
```

**Scenario 3: Cleanup Operations**
```
CREATE SCHEMA temp
CREATE TABLE temp.staging (data VARCHAR)
INSERT INTO temp.staging VALUES ('row1'), ('row2'), ('row3')
DELETE FROM temp.staging WHERE data = 'row2'
SELECT * FROM temp.staging
DROP TABLE temp.staging
DROP SCHEMA temp
-- Verify clean state
```

### regression-tester — Nothing Broke
- Run full test suite on merged branch: `cargo test --all-features`
- Run clippy: `cargo clippy --all-features`
- Run rustfmt check: `cargo fmt -- --check`
- Compare test count before and after (should only increase)
- Run any benchmarks to check for performance regressions
- Verify all pre-existing 115 tests pass with identical behavior
- Check for new warnings

---

## Step 4: Findings Consolidation

After all review and test agents report:

1. **Triage findings by severity**:
   - **Blocking**: Correctness bugs, data corruption risks, security issues → must fix before merge
   - **Important**: API design issues, missing error handling, test gaps → fix if time permits
   - **Nit**: Style, naming, documentation → note for future cleanup

2. **Fix blocking issues**: Dispatch a fix-it agent for each blocking finding

3. **Re-test**: After fixes, run regression-tester again

4. **Produce final summary document** (`docs/phase3-summary.md`):
   - What was built (features, test counts, line counts)
   - What works via SQL vs. API-only
   - Known limitations and future work
   - Architecture decisions and rationale
   - How to set up and use the extension

---

## Agent Summary

| Phase | Agent | Type | Access |
|-------|-------|------|--------|
| Step 1 | integrator | general-purpose | Read/write, main worktree |
| Step 2 | arch-reviewer | general-purpose | Read-only |
| Step 2 | datafusion-reviewer | general-purpose | Read-only |
| Step 2 | rust-reviewer | general-purpose | Read-only (+ cargo clippy) |
| Step 2 | security-reviewer | general-purpose | Read-only |
| Step 3 | parity-tester | general-purpose | Read-only + DuckDB CLI |
| Step 3 | edge-case-tester | general-purpose | Read/write (temp test files) |
| Step 3 | e2e-scenario-tester | general-purpose | Read/write (temp test files) |
| Step 3 | regression-tester | general-purpose | Read-only + cargo test |
| Step 4 | fix-it (as needed) | general-purpose | Read/write |

**Parallelism**: Steps 2 and 3 can run in parallel (reviewers and testers simultaneously) since all work off the same frozen merged branch. Step 1 must complete first. Step 4 is sequential after 2+3.
