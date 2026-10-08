# Strobe (`skystrobe`)

> Period search over the ZTF light-curve archive, in a browser tab. DuckDB-Wasm reads a zero-copy Iceberg index over IRSA's public HATS Parquet on S3; a Rust periodogram compiled to WASM runs on every core. No server.

RDD type: **A — Spec**. A coding agent implements from this file. Status: pre-Gate-0.

---

## Purpose

**Problem.** Finding periodic variable stars in ZTF needs a Python environment, lsdb/astropy, and familiarity with HATS. Existing web viewers (e.g. SNAD) are server-backed and work one object at a time.

**Solution.** A static page. The user picks a sky cone. The tab fetches every light curve in that cone from the public archive, runs a generalized Lomb-Scargle (GLS) periodogram on all of them in parallel WASM workers, ranks candidates, and shows folded curves. Results are a URL pinned to an Iceberg snapshot and the algorithm parameters.

**Who benefits.** Amateur variable-star observers, students, and astronomers who want a quick look without a pipeline. Secondary: data engineers interested in zero-backend Iceberg.

---

## Architecture

```
OFFLINE (once per data release, run by Emmanuel)
  strobe-index build  ──reads Parquet footers only──▶  s3://ipac-irsa-ztf/.../lc/hats/**.parquet
        │                                              (data is NOT copied)
        └─writes Iceberg metadata (json + avro)──▶  <metadata host: own S3 bucket or local dir>
                                                    + strobe.json (pointer file)

ONLINE (browser tab, static page on GitHub Pages)
  index.html ─▶ app.js ─▶ query.js (DuckDB-Wasm + iceberg ext)
                             │  iceberg_scan(metadata) + cone predicate
                             │  → range-reads only needed row groups from IRSA S3
                             ▼
                        Arrow batches (t, mag, magerr per object)
                             ▼
                        pool.js ─▶ N × worker.js ─▶ strobe_kernel.wasm (gls_batch)
                             ▼
                        results ─▶ ui.js (table, cone scatter, folded plot, URL hash, CSV)
```

| Component | Responsibility |
|---|---|
| `kernel/` (Rust → WASM) | GLS periodogram, batch API, folding helper |
| `src/strobe_index/` (Python CLI) | Build Iceberg metadata over existing HATS files via `add_files`; native verify; export for validation |
| `web/query.js` | DuckDB-Wasm init, load `iceberg`, run cone query, return typed arrays |
| `web/pool.js` + `web/worker.js` | Worker pool sized to `navigator.hardwareConcurrency`; one kernel instance per worker |
| `web/ui.js` | Inputs, progress, results table, canvas plots, URL state, CSV export |
| `sql/cone.sql` | Single canonical query shared by web and Python verify |

> **Design rationale.** Iceberg is the browser's index: manifest min/max statistics let stock DuckDB-Wasm skip files without HATS-aware JavaScript, and the snapshot ID makes result URLs reproducible. Gate-0 includes a kill test: if Iceberg gives no pruning benefit over plain `read_parquet`, drop it from the MVP.

---

## Recommended Stack

Evidence collected 2026-10-02. Popularity stats (stars, downloads) were not collected; decisions rest on capability and maintenance.

| Layer | Chosen | Why | Rejected |
|---|---|---|---|
| Browser query engine | DuckDB-Wasm + `iceberg` extension | Iceberg extension supported in DuckDB-Wasm since Dec 2025 ([DuckDB blog](https://duckdb.org/2025/12/16/iceberg-in-the-browser)); `iceberg_scan` reads a table from its metadata without a catalog ([docs](https://duckdb.org/docs/current/core_extensions/iceberg/overview)) | Hand-rolled Parquet reader in JS (no Iceberg, no SQL pushdown) |
| Iceberg metadata writer | PyIceberg `add_files` | Registers existing Parquet without rewriting; reads only footers ([PR #2662 docs](https://github.com/apache/iceberg-python/pull/2662/files)) | DuckDB `COPY … FORMAT iceberg` (copies data); Spark `add_files` (heavy runtime) |
| Kernel language | Rust + wasm-bindgen | Near-native numeric speed; mature tooling | AssemblyScript (weaker numeric ecosystem); plain JS (slower, no SIMD path) |
| Kernel build | wasm-pack 0.15.0 | Maintained under the `wasm-bindgen` org after the `rustwasm` org sunset ([Rust blog](https://blog.rust-lang.org/inside-rust/2025/07/21/sunsetting-the-rustwasm-github-org)) | Raw `wasm-bindgen-cli` (more manual steps) |
| Periodogram code | Own fast GLS (Press–Rybicki + `rustfft`), MIT | O(N log N); direct GLS is ~10× too slow for the 60 s target; keeps the project MIT | `light-curve-feature` Rust crate: GPL-3.0 ([repo](https://github.com/light-curve/light-curve-feature)) |
| Hosting | GitHub Pages + one S3 bucket (metadata only) | Static; metadata is KB–MB | Any server |
| Data | ZTF DR24 Lightcurves, HATS Parquet, `s3://ipac-irsa-ztf/ztf/enhanced/dr24/lc/hats` ([IRSA](https://irsa.ipac.caltech.edu/cloud_access/)) | Public, no egress charges, nested light-curve column | Copying data to own bucket (cost; only if CORS blocks) |

---

## Repository Layout

```
skystrobe/
├── README.md                 # this spec
├── LICENSE                   # MIT
├── pyproject.toml            # strobe-index package; [project.scripts] for uvx
├── Makefile
├── sql/
│   └── cone.sql              # canonical cone query (web + Python)
├── src/strobe_index/
│   ├── __init__.py
│   ├── cli.py                # Typer app: build, verify, export, check-cors
│   ├── build.py              # list HATS files, create table, add_files, write strobe.json
│   ├── verify.py             # run sql/cone.sql natively with duckdb
│   ├── export.py             # dump cone light curves to Parquet for validation
│   └── errors.py
├── tests/
│   ├── conftest.py           # builds tiny nested Parquet fixtures locally
│   ├── test_build.py
│   └── test_verify.py
├── kernel/
│   ├── Cargo.toml            # crate strobe_kernel, crate-type cdylib + rlib
│   └── src/lib.rs            # gls_batch, gls_power, fold
├── web/
│   ├── index.html
│   ├── app.js                # wiring, URL state
│   ├── query.js              # DuckDB-Wasm
│   ├── pool.js               # worker pool
│   ├── worker.js             # loads pkg/strobe_kernel.js
│   ├── ui.js                 # table + canvas plots
│   └── pkg/                  # wasm-pack output (gitignored; built in CI)
├── spike/                    # Gate-0 only; kept for reproducibility
│   ├── check_cors.sh
│   ├── iceberg_vs_parquet.sql
│   └── validate_periods.py
├── .github/workflows/
│   ├── ci.yml                # cargo test/clippy, ruff, mypy, pytest
│   └── pages.yml             # build kernel, deploy web/
├── DEVELOPER.md              # local dev loop, release steps
└── CHANGELOG.md
```

---

## Prerequisites

| Requirement | Version | Notes |
|---|---|---|
| macOS or Linux | — | |
| uv | latest | `curl -LsSf https://astral.sh/uv/install.sh \| sh` |
| Python | 3.12 | via uv |
| Rust | stable | `rustup target add wasm32-unknown-unknown` |
| wasm-pack | 0.15.0 | `cargo install wasm-pack --version 0.15.0` |
| AWS CLI | v2 | Public bucket reads use `--no-sign-request` |
| DuckDB CLI | ≥ 1.4 | Optional; native checks |
| Browser | current Chrome or Firefox | |

**Accounts and permissions**

- None to read ZTF data.
- To publish metadata: one S3 bucket you own with public read on the `strobe/` prefix and a CORS rule (see Deployment). Required permissions: `s3:PutObject`, `s3:PutBucketCors`, `s3:PutBucketPolicy` on that bucket.

---

## Quick Start

```bash
# 1. Index (dev mode: local warehouse, a few HATS pixels)
uvx --from git+https://github.com/ejoliet/skystrobe strobe-index build \
  s3://ipac-irsa-ztf/ztf/enhanced/dr24/lc/hats \
  --warehouse file://$PWD/wh --table ztf.lc_dr24 --limit-files 5

# 2. Verify natively (no browser)
uvx --from git+https://github.com/ejoliet/skystrobe strobe-index verify \
  --strobe-json wh/strobe.json --ra 280.0 --dec 0.0 --radius 0.3

# 3. Run the web app locally
git clone https://github.com/ejoliet/skystrobe && cd skystrobe
make kernel && make serve        # http://localhost:8000/?catalog=/wh/strobe.json
```

---

## Configuration Reference

No secrets anywhere. AWS credentials, when needed for publishing, come from the standard AWS credential chain.

### Indexer (environment)

| Variable | Type | Default | Required | Description |
|---|---|---|---|---|
| `AWS_PROFILE` | str | — | no | Profile for writing to your metadata bucket |
| `STROBE_LOG_LEVEL` | str | `INFO` | no | `DEBUG`, `INFO`, `WARNING`, `ERROR` |

### Web app (URL query and hash)

| Param | Where | Default | Description |
|---|---|---|---|
| `catalog` | query | `./strobe.json` | URL of the pointer file |
| `ra`, `dec` | hash | preset 1 | Cone center, degrees |
| `r` | hash | `0.3` | Cone radius, degrees; max `1.0` |
| `fmin`, `fmax` | hash | `0.01`, `10` | Frequency range, cycles/day |
| `os` | hash | `3` | Oversampling factor |
| `nmin` | hash | `50` | Min good points per light curve |
| `band` | hash | `r` | `g`, `r`, or `i` |
| `snap` | hash | from `strobe.json` | Iceberg snapshot ID (pinned in shared URLs) |
| `v` | hash | `1` | URL schema version |

---

## API / Interface Contract

### `strobe.json` (pointer file written by `build`)

```json
{
  "version": 1,
  "table": "ztf.lc_dr24",
  "metadata_url": "https://<host>/strobe/ztf/lc_dr24/metadata/00001-<uuid>.metadata.json",
  "snapshot_id": 1234567890123456789,
  "source_prefix": "s3://ipac-irsa-ztf/ztf/enhanced/dr24/lc/hats",
  "data_region": "us-east-1",
  "files": 5,
  "created_utc": "2026-10-02T12:00:00Z",
  "builder": "strobe-index 0.1.0"
}
```

### CLI

```
strobe-index build SOURCE_PREFIX --warehouse URI --table NS.NAME
    [--limit-files N] [--pixel-glob GLOB] [--metadata-base-url URL] [--dry-run]
  Lists HATS data files under SOURCE_PREFIX (excludes margin and index catalogs),
  reads one footer for the schema, creates the table, calls add_files, validates the
  resulting metadata.json (format-version 2, current-snapshot-id present and listed,
  no self-referencing metadata-log entries, every manifest readable with pyiceberg),
  then writes strobe.json next to the warehouse root. Prints metadata location and
  snapshot ID. Rationale: the DuckDB Iceberg extension is marked experimental and a
  recent issue reports hard crashes on malformed v2 metadata; never ship a file the
  browser cannot parse.

strobe-index verify --strobe-json PATH_OR_URL --ra DEG --dec DEG --radius DEG
    [--band r] [--nmin 50]
  Runs sql/cone.sql with native duckdb. Prints object count, point count, wall time.
  With --compare-parquet also runs the same cone via read_parquet on SOURCE_PREFIX
  and prints both timings (Iceberg kill test).

strobe-index export --strobe-json ... --ra --dec --radius --out lcs.parquet
  Writes objectid, t[], mag[], magerr[] for the cone (validation input).

strobe-index check-cors URL [--origin http://localhost:8000]
  HEAD request with Origin header; exits 0 if Access-Control-Allow-Origin matches.
```

### Canonical SQL (`sql/cone.sql`)

Column names are assumed until Phase 0 confirms them (see Open Questions Q3). `$`-params are bound by the caller. RA wrap-around: the caller splits into two queries when the box crosses 0/360.

```sql
-- AIDEV-NOTE: single source of truth for web/query.js and strobe_index/verify.py
WITH box AS (
  SELECT objectid, ra, dec, lightcurve
  FROM iceberg_scan($metadata_url)
  WHERE dec BETWEEN $dec - $r AND $dec + $r
    AND ra  BETWEEN $ra_lo AND $ra_hi
    AND filterid = $filterid
)
SELECT
  objectid, ra, dec,
  [x.hmjd   FOR x IN lightcurve IF x.catflags = 0] AS t,
  [x.mag    FOR x IN lightcurve IF x.catflags = 0] AS mag,
  [x.magerr FOR x IN lightcurve IF x.catflags = 0] AS magerr
FROM box
WHERE degrees(acos(least(1.0,
        sin(radians(dec)) * sin(radians($dec)) +
        cos(radians(dec)) * cos(radians($dec)) * cos(radians(ra - $ra))))) <= $r
  AND len([1 FOR x IN lightcurve IF x.catflags = 0]) >= $nmin
LIMIT $max_objects;
```

### Kernel (Rust, wasm-bindgen exports)

```rust
/// Batch GLS over many light curves packed into flat arrays.
/// offsets.len() == n_objects + 1; object i uses [offsets[i], offsets[i+1]).
/// Returns flat Vec: per object [best_freq, best_power, n_points, alias_flag].
#[wasm_bindgen]
pub fn gls_batch(t: &[f64], y: &[f64], dy: &[f64], offsets: &[u32],
                 fmin: f64, fmax: f64, oversample: f64) -> Vec<f64>;

/// Full periodogram for one object (for the detail view). Returns power per freq.
#[wasm_bindgen]
pub fn gls_power(t: &[f64], y: &[f64], dy: &[f64],
                 fmin: f64, fmax: f64, oversample: f64) -> Vec<f32>;

/// Phase in [0, 1) for each t at frequency f, zero point t0.
#[wasm_bindgen]
pub fn fold(t: &[f64], f: f64, t0: f64) -> Vec<f64>;
```

Algorithm requirements:

- Generalized (floating-mean, error-weighted) Lomb-Scargle, Zechmeister and Kürster (2009) normalization, power in [0, 1].
- Frequency grid: uniform in frequency, `df = 1 / (oversample * baseline)`, from `fmin` to `fmax`. Cap at 200,000 frequencies; if exceeded, raise `df` and set a flag bit.
- Use the Press–Rybicki fast method (extirpolation onto a regular grid + FFT, O(N log N) per object, with the floating-mean terms as in astropy's `fast` implementation). Use `rustfft` (MIT/Apache-2.0). Direct O(N·M) evaluation is far too slow for the target: 60k frequencies × 500 points ≈ 6×10⁸ flops ≈ 0.2–0.6 s per object in scalar WASM, which breaks the 60 s budget.
- Keep a direct GLS implementation behind a `#[cfg(test)]` flag as the oracle: fast and direct must agree on the best frequency within one grid step on the test set.
- `alias_flag` bit 0 is set when the best period is within 1 % of 1, 1/2, 1/3 days or 29.53 days.
- Objects with fewer than 10 points or zero variance return `NaN` for frequency and power.
- `dy <= 0` is replaced by the median positive `dy` of that object.

### Worker protocol

```
main → worker: { id, t: Float64Array, y, dy, offsets: Uint32Array, fmin, fmax, os }  (transferable)
worker → main: { id, result: Float64Array, ms }
worker → main: { id, error: { name, message } }
```

---

## Error Handling

### Indexer (Python)

| Error class | When | Exit code | Retry |
|---|---|---|---|
| `SourceNotFoundError` | No HATS data files under prefix | 2 | No |
| `SchemaMismatchError` | A file's schema differs from the first | 3 | No; list offending files |
| `S3AccessError` | Footer read fails | 4 | Yes, 3× exponential backoff per file |
| `WarehouseWriteError` | Cannot write metadata | 5 | No |

Logs: structured JSON to stderr.

### Web

| Error name | Detection | User message |
|---|---|---|
| `CorsError` | Fetch `TypeError` on a data or metadata URL | "The data host blocked this browser request (CORS)." Link to Troubleshooting |
| `IcebergMetadataError` | `iceberg_scan` fails | Show the SQL error text |
| `EmptyRegionError` | 0 rows | "No light curves here. Try a larger radius or another band." |
| `RegionTooLargeError` | Object count > `max_objects` (20,000) | Ask to shrink the radius |
| `KernelError` | Worker reports error | Show object batch ID; other batches continue |

---

## Testing

```bash
make test     # cargo test + pytest
make lint     # cargo clippy -D warnings, cargo fmt --check, ruff, mypy --strict
```

| Suite | Covers | Network |
|---|---|---|
| `kernel` `cargo test` | Recovers a sinusoid period within 0.1 % from 200 irregular points with noise; eclipse-like box signal finds a harmonic; `<10` points → NaN; zero variance → NaN; `dy<=0` handling; alias flag; grid cap | No |
| `tests/test_build.py` | Builds nested Parquet fixtures locally, runs `build` to a `file://` warehouse, checks `strobe.json` fields and file count | No |
| `tests/test_verify.py` | Runs `sql/cone.sql` on the fixture table; checks cone filter and `catflags` filter | No |
| Browser end-to-end | Human-run (see Human Checks) | Yes |

---

## Gate-0 (Phase 0 — do first)

Time box: 4 hours. Results go in `spike/RESULTS.md`.

| Step | Who | Command or action | Pass |
|---|---|---|---|
| 0.0 Static Iceberg in Wasm | Emmanuel | In https://shell.duckdb.org run `LOAD iceberg; SELECT count(*) FROM iceberg_scan('https://<any public>/metadata/v1.metadata.json');` against a small public Iceberg table you own or one you upload to your bucket | Returns a count; resolves Q2 before any code |
| 0.1 Find files | Emmanuel | `aws s3 ls --no-sign-request --recursive s3://ipac-irsa-ztf/ztf/enhanced/dr24/lc/hats/ \| grep 'Npix=' \| grep -v -i -E 'margin\|index' \| head` | Paths listed |
| 0.2 CORS | Emmanuel | `bash spike/check_cors.sh "https://ipac-irsa-ztf.s3.us-east-1.amazonaws.com/<path>"` | `access-control-allow-origin` present |
| 0.3 Schema | Agent | `duckdb -c "DESCRIBE SELECT * FROM 's3://ipac-irsa-ztf/<path>'"` then update `sql/cone.sql` names | Q3 resolved |
| 0.4 Index 1 file | Agent | `strobe-index build … --limit-files 1` | `strobe.json` written |
| 0.5 Kill test | Agent | `strobe-index verify … --compare-parquet` | Record both timings and row groups read |
| 0.6 Kernel speed | Agent | `cargo bench` or a timed test: 2,000 synthetic curves × 500 points, 60k frequencies, fast method | ≤ 10 s single-thread native (≈ 5 ms per object) |
| 0.7 Browser | Emmanuel | `make serve`, open the app, run preset 1; note DevTools Network total for `amazonaws` | ≥ 2,000 curves end to end ≤ 60 s; record MB transferred |
| 0.8 Correctness | Emmanuel | Export CSV from the app; `uv run --with astropy --with pandas spike/validate_periods.py results.csv lcs.parquet` | ≥ 95 % within 0.1 % of astropy for power > 0.3 |

**Decisions after Gate-0**

- 0.2 fails: blocker. Ask IRSA to add a GET/HEAD CORS rule for the public bucket. Interim: mirror showcase pixels to your own bucket.
- 0.5 shows no benefit: remove Iceberg from the MVP; switch `query.js` to `read_parquet` over a file list in `strobe.json`.
- 0.7 transfers more than ~150 MB for a 0.3° cone: nested row groups are large. Set the default radius from the measured MB-per-degree² figure; do not promise a bytes-per-file ratio.
- 0.0 fails: build the warehouse in your own S3 bucket and retry with the `s3://` metadata path; if that also fails, Iceberg-in-browser is NO-GO and the Iceberg index becomes a native-only artifact.
- 0.7 or 0.8 fails: NO-GO. Stop.

---

## Human Checks (Emmanuel runs these; agent does not spend tokens on them)

```bash
# Local browser run
make kernel && make serve           # open http://localhost:8000/?catalog=/wh/strobe.json

# Correctness
uv run --with astropy --with pandas --with pyarrow spike/validate_periods.py results.csv lcs.parquet

# Bytes transferred: DevTools → Network → filter "amazonaws" → read "transferred" total
```

Publishing, pushing, and opening PRs are also manual (see Deployment).

---

## Deployment

### Metadata bucket (once)

```bash
BUCKET=<your-bucket>
cat > cors.json <<'EOF'
{"CORSRules":[{"AllowedOrigins":["https://ejoliet.github.io","http://localhost:8000"],
 "AllowedMethods":["GET","HEAD"],"AllowedHeaders":["*"],
 "ExposeHeaders":["Content-Length","Content-Range","ETag"],"MaxAgeSeconds":3600}]}
EOF
aws s3api put-bucket-cors --bucket "$BUCKET" --cors-configuration file://cors.json
```

### Build the full index straight into the bucket

```bash
uvx --from git+https://github.com/ejoliet/skystrobe strobe-index build \
  s3://ipac-irsa-ztf/ztf/enhanced/dr24/lc/hats \
  --warehouse s3://$BUCKET/strobe --table ztf.lc_dr24 \
  --metadata-base-url https://$BUCKET.s3.us-east-1.amazonaws.com/strobe
```

Writing the warehouse directly to S3 avoids relocated `file://` paths in metadata.

### Web

`pages.yml` builds the kernel with wasm-pack 0.15.0 and deploys `web/` to GitHub Pages on push to `main`. Set Pages source to "GitHub Actions" once in repo settings.

---

## Non-Goals (v1)

- Box least squares, conditional entropy, or multiband periodograms.
- All-sky map or drag-to-select on a sky projection (v1 uses cone inputs and a local scatter).
- Cross-match with known-variable catalogs.
- Formal false-alarm probabilities (v1 reports GLS power and an alias flag).
- Rubin, Gaia, or TESS data.
- Writing to Iceberg from the browser; any REST catalog.
- Accounts, saved searches, or server-side storage.
- WebGPU.
- Mobile performance tuning.

---

## Open Questions

- [ ] **Q1** — Does `ipac-irsa-ztf` return CORS headers for GET/HEAD from arbitrary origins? Owner: Emmanuel (ask IRSA if not). Blocks the zero-copy MVP.
- [ ] **Q2** — Does DuckDB-Wasm `iceberg_scan` accept an `https://` `metadata.json` whose data files are `s3://` paths, with anonymous access and `s3_region='us-east-1'`? Owner: agent, Gate-0 step 0.7.
- [ ] **Q3** — Exact DR24 lightcurve HATS schema: names of object ID, RA, Dec, and band columns; whether `lightcurve` matches `list<struct<hmjd, mag, magerr, clrcoeff, catflags>>`. Owner: agent, step 0.3.
- [ ] **Q4** — PyIceberg setting for unsigned S3 reads of the source bucket. Fallback: use any valid AWS credentials (signed reads of a public bucket work). Owner: agent.
- [ ] **Q5** — Does DuckDB's Iceberg reader prune on manifest min/max for `ra`/`dec`? A third-party fork references bounds matching in the extension, so likely yes; measure at step 0.5. Owner: agent.
- [ ] **Q5b** — DR24 row-group size and whether DuckDB pushes projection into the nested `lightcurve` struct (skipping `clrcoeff`). A 0.3° cone may pull 100–300 MB if row groups hold ~10k objects. Owner: agent, step 0.7 numbers.
- [ ] **Q8** — Do the HATS Parquet files carry Parquet field IDs? `add_files` requires either none (then name mapping) or IDs matching the table. Check: `duckdb -c "SELECT name, field_id FROM parquet_schema('s3://…/<file>')"`. Owner: agent, step 0.3.
- [ ] **Q6** — Can the page report "MB fetched"? Cross-origin resource timing needs `Timing-Allow-Origin`. If not available, drop the counter. Owner: agent.
- [ ] **Q7** — Name collision check for "Strobe" / `skystrobe` on GitHub and npm. Owner: Emmanuel.

---

## Agent Build Instructions

> Implement end to end from this README. Do not guess on Open Questions: resolve them in Gate-0 or record the result in `spike/RESULTS.md`.

### Build Order

| Phase | Deliverable | Done when |
|---|---|---|
| 0 | Gate-0 scripts in `spike/`, results file | Steps 0.3–0.6 recorded; Emmanuel confirms 0.2, 0.7, 0.8 |
| 1 | Scaffold: `pyproject.toml`, `Cargo.toml`, `Makefile`, CI | `make lint` passes on empty stubs |
| 2 | Kernel + tests | `cargo test` passes all cases listed in Testing |
| 3 | Indexer `build`, `verify`, `export`, `check-cors` | `pytest` passes offline; `uvx --from . strobe-index --help` works |
| 4 | `query.js`, `pool.js`, `worker.js` | Preset query returns arrays; worker pool returns results (manual check by Emmanuel) |
| 5 | `ui.js`: inputs, progress, results table sorted by power, cone scatter colored by power, folded plot (phase 0–2), periodogram on click, URL hash state, CSV export | Shared URL reproduces the same top 10 |
| 6 | `pages.yml`, `DEVELOPER.md`, `CHANGELOG.md` | Workflow file lints; docs list exact commands |

### File Map

| File | Purpose | Key symbols |
|---|---|---|
| `kernel/src/lib.rs` | GLS + exports | `gls_batch`, `gls_power`, `fold`, `freq_grid`, `AliasFlag` |
| `src/strobe_index/cli.py` | Typer CLI | `app`, `build`, `verify`, `export`, `check_cors` |
| `src/strobe_index/build.py` | Index creation | `list_hats_files`, `create_table`, `register_files`, `validate_metadata`, `write_pointer` |
| `src/strobe_index/verify.py` | Native query | `load_cone_sql`, `run_cone`, `compare_parquet` |
| `src/strobe_index/errors.py` | Error classes | as in Error Handling |
| `web/query.js` | DuckDB-Wasm | `initDb`, `runCone`, `splitRaWrap` |
| `web/pool.js` | Workers | `createPool`, `runBatches` |
| `web/worker.js` | Kernel host | message handler |
| `web/ui.js` | UI | `renderTable`, `drawScatter`, `drawFolded`, `drawPeriodogram`, `readHash`, `writeHash`, `exportCsv` |
| `spike/validate_periods.py` | Astropy comparison | `main` |

### Constraints

- No backend, no bundler, no framework. Plain ES modules; load DuckDB-Wasm from jsDelivr pinned to an exact version, recorded in `DEVELOPER.md`.
- No browser storage APIs. State lives in the URL hash.
- MIT only. No GPL dependencies (excludes `light-curve-feature`).
- Python: 3.12, typed, `ruff` and `mypy --strict` clean, packaged for `uvx --from git+https://github.com/ejoliet/skystrobe strobe-index`.
- Rust: `cargo clippy -D warnings` clean; no `unsafe`. Allowed numeric deps: `rustfft` only.
- DuckDB-Wasm runs single-threaded on GitHub Pages (no COOP/COEP headers, so no SharedArrayBuffer). Do not rely on DuckDB parallelism; keep the kernel in your own Workers using transferable buffers.
- Pin `@duckdb/duckdb-wasm` to an exact version and keep a smoke test (`iceberg_scan` on the fixture table) in `DEVELOPER.md` to rerun before any version bump.
- Unit tests never touch the network. Use local fixtures.
- Batch objects into chunks of about 250 per worker message to limit JS↔WASM crossings.
- Main thread never runs the kernel.
- Use `AIDEV-NOTE:` / `AIDEV-TODO:` / `AIDEV-QUESTION:` anchors for non-obvious decisions. No comments that restate code.
- Never commit credentials, bucket policies with account IDs, or `.env` files.

### Acceptance Criteria

- [ ] `make test` and `make lint` pass.
- [ ] Kernel recovers injected periods within 0.1 % in unit tests.
- [ ] `strobe-index build` on 1 real HATS file produces a readable table (`verify` returns rows).
- [ ] `spike/RESULTS.md` records Gate-0 outcomes and the Iceberg kill-test numbers.
- [ ] Human-run: ≥ 2,000 light curves searched end to end in ≤ 60 s on an M-series laptop, at the default radius chosen after step 0.7.
- [ ] `spike/RESULTS.md` records MB transferred per degree² for the chosen preset; the UI default radius is derived from it.
- [ ] Kernel: fast and direct methods agree within one grid step on all unit-test signals.
- [ ] Human-run: ≥ 95 % period agreement with astropy (power > 0.3).
- [ ] A shared URL reproduces the same top-10 candidates and the same snapshot ID.
- [ ] Every Open Question is resolved or moved to v2 in `CHANGELOG.md`.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Fetch fails instantly, console mentions CORS | Data or metadata host lacks CORS | Run `strobe-index check-cors`; see Q1 |
| `iceberg_scan` cannot open `file:///…` | Dev warehouse paths are absolute | Build the warehouse directly in S3, or try `allow_moved_paths = true` |
| Query downloads hundreds of MB | Row groups too large or no pruning | Smaller radius; check Q5 results |
| Many periods near 1 day | Sampling alias | Alias flag is set; inspect periodogram |
| Search takes minutes | Direct periodogram path used, or oversample too high | Confirm fast path is active; lower `os` or `fmax` |

---

## Next Steps

1. [ ] Emmanuel: run Gate-0 steps 0.0–0.2 (15 minutes). Stop if 0.0 or CORS fails and there is no fallback.
2. [ ] Agent: Phase 0 (steps 0.3–0.6) and Phase 1.
3. [ ] Agent: Phases 2–3.
4. [ ] Emmanuel: Gate-0 steps 0.7–0.8; GO/NO-GO.
5. [ ] Agent: Phases 4–6.
6. [ ] Emmanuel: create bucket CORS, build full index, push, enable Pages.
7. [ ] Emmanuel: launch (Show HN, AAVSO forums, r/dataengineering, r/rust).

## References

- ZTF on the AWS Open Data registry: https://registry.opendata.aws/ztf/
- IRSA cloud access: https://irsa.ipac.caltech.edu/cloud_access/
- IRSA ZTF HATS light-curve tutorial: https://caltech-ipac.github.io/irsa-tutorials/ztf-lightcurves/
- DuckDB Iceberg in the browser: https://duckdb.org/2025/12/16/iceberg-in-the-browser
- DuckDB-Wasm remote reads and CORS: https://duckdb.org/docs/current/clients/wasm/known_issues
- PyIceberg configuration: https://py.iceberg.apache.org/configuration
- Zechmeister and Kürster 2009, "The generalised Lomb-Scargle periodogram" (A&A 496, 577)
