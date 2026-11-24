# opteryx-iceberg

Read-only Apache Iceberg `Metastore`/`FileIO` backend for [opteryx-catalog](https://github.com/mabel-dev/opteryx-catalog), letting an Opteryx workspace query tables from an external Iceberg catalog (REST, SQL, Hive, Glue - whatever [pyiceberg](https://py.iceberg.apache.org/)'s own catalog loader supports) side by side with native Firestore/GCS-backed tables.

Tier 1 is reads. Tier 2 is writes: `CREATE SCHEMA`, `CREATE TABLE` and `INSERT INTO ... SELECT` (see [Writing](#writing)). Serving Opteryx's own catalog as an Iceberg REST endpoint (Tier 3) is separate, later work.

Kept as its own package - not merged into `opteryx-catalog` or `opteryx-core` - because it depends on `pyiceberg`, which pulls in `pydantic`. Both of those repos are deliberately free of that dependency chain; Iceberg support is optional, the same way `opteryx-access` is.

Iceberg metadata files (manifest lists, manifests, `metadata.json`) are read through opteryx-core's own storage clients (`opteryx_iceberg.fileio.OpteryxFileIO`, installed as the catalog's `py-io-impl`), not pyiceberg's `PyArrowFileIO`: one GET per file with no preceding HEAD, over the engine's warm connections, and no `pyarrow` at runtime. Credentials and the S3 endpoint therefore come from opteryx-core's configuration; a catalog whose properties name a different S3 endpoint or access key, or vend a GCS token, is refused rather than silently read under another identity. This holds whatever FileIO the catalog serves: a REST catalog may serve its own `py-io-impl` in a table load (Lakekeeper's default serves `FsspecFileIO`), and pyiceberg lets that replace the catalog's, so manifests are read through an `OpteryxFileIO` built per table rather than through pyiceberg's `table.io`. A `py-io-impl` passed to `IcebergMetastore` is refused for the same reason.

Manifest lists and manifests are decoded by rugo's native Avro reader (bundled in opteryx-core), not pyiceberg: `opteryx_iceberg.manifests` replaces `table.scan().plan_files()`, making the same decisions pyiceberg's local planner makes with no row filter (live entries, sequence-number inheritance, delete-file matching), and is tested against it. pyiceberg remains the catalog client - `metadata.json`, snapshots and schemas. Planning is always local: a REST catalog configured for server-side scan planning is not asked for a plan.

## Usage

Register a workspace against an external Iceberg catalog using Opteryx's existing connector-registration API:

```python
from opteryx.connectors import register_workspace
from opteryx.connectors.opteryx_connector import OpteryxConnector
from opteryx_iceberg import IcebergMetastore

register_workspace(
    "my_iceberg_workspace",
    OpteryxConnector,
    catalog=IcebergMetastore,
    catalog_type="rest",       # IcebergMetastore: "rest" only - see "Catalog properties" below
    uri="https://...",
    warehouse="s3://...",
)
```

Do **not** pass `workspace=` yourself — `OpteryxConnector` injects it automatically (as the registered prefix) when it instantiates `IcebergMetastore`; passing it explicitly raises a duplicate-keyword-argument error.

Native (Firestore/GCS-backed) workspaces are entirely unaffected - this only applies to workspaces explicitly registered with `catalog=IcebergMetastore`.

**Catalog properties are an allow-list.** `IcebergMetastore` is for configuration a *tenant* supplies (a workspace's catalog binding), so it accepts only `catalog_type="rest"` and the properties `uri` (https), `warehouse`, `token`, `credential`, `scope`, `oauth2-server-uri` (https), `header.X-Iceberg-Access-Delegation`, `header.x-goog-user-project`, and `auth={"type": "google", "google": {"scopes": [...]}}` with a `uri` under `https://biglake.googleapis.com/`. Anything else raises `ValueError` before pyiceberg sees it: pyiceberg imports and calls the class an `auth.impl` or `py-*-impl` names, and its Google auth manager sends the process's own token to whatever `uri` says. `check_tenant_properties(catalog_type, properties)` is the same check, for a binding writer to refuse a config before storing it. A REST catalog's server cannot widen this either: a `/v1/config` response that changes `uri`, `auth`, `ssl`, the token/credential/OAuth keys, or any `header.*`, `rest.*`, `client.*` or `py-*` key is refused, and a table's FileIO is always the metastore's own (`TenantFileIO` here, `OpteryxFileIO` for the operator class) whatever `py-io-impl` the table's properties or load-table config name.

**Locations are restricted too.** Every Iceberg file - metadata, manifests, and the data files opteryx-core scans - is opened with Opteryx's own identity, so a tenant's catalog could otherwise name the worker's local disk, or another workspace's data in a bucket Opteryx owns, and have it read for them. `IcebergMetastore` reads, writes and hands the engine only `gs://` and `s3://` objects whose bucket is a canonical name (lowercase; no percent-escapes, ports or userinfo) and is not one the deployment reserves. Anything else raises `PermissionError` before storage is touched. The reserved buckets are **required** configuration for a tenant catalog to be read at all - `OPTERYX_ICEBERG_RESERVED_BUCKETS`, a comma-separated list of `gs://<bucket>` / `s3://<bucket>` naming every bucket Opteryx's identity can read that is not a customer's lake (its native catalog's data, query results, scratch). Unset, every tenant read raises `RuntimeError` rather than protecting nothing. This is a stopgap: a customer bucket granted to Opteryx's shared identity stays reachable from any tenant's catalog until each workspace reads under an identity of its own. `UnrestrictedIcebergMetastore` keeps every location the reader supports, local paths included.

**So are the addresses it connects to.** A tenant's REST catalog would otherwise make the worker an HTTP client inside Opteryx's network. pyiceberg's REST session follows a redirect wherever it points (https to http included), resolves the name again for each connection, and quotes an unparseable response body in its error. `IcebergMetastore` builds its REST catalog with every session hardened (`opteryx_iceberg.egress`), from the first request on, including the `oauth2-server-uri` token request that pyiceberg makes while it builds a session:
- each connection goes only to an address that is public (`ipaddress.is_global`, not multicast), and every address the name resolves to must be public;
- it connects to exactly the address that was checked, with no second lookup to rebind, while TLS still verifies the certificate against the hostname;
- a redirect is refused, never followed;
- plain http is refused;
- the process's proxy and CA environment (`HTTPS_PROXY`, `REQUESTS_CA_BUNDLE`, ...) is ignored.

`UnrestrictedIcebergMetastore` is not hardened: an operator's catalog may legitimately be a local or VPC service.

`UnrestrictedIcebergMetastore` takes any catalog type (`sql`, `hive`, `glue`, ...) and any pyiceberg property, passed through as given, and is only for configuration the deployment's *operator* writes - a self-hosted box's environment, a script, this repo's tests - never a binding a workspace owner can edit. (Flat `auth_type`/`google_auth_scopes` kwargs, retired when opteryx-core's connector cache stopped hashing registration kwargs, raise a clear `ValueError` on both classes.)

### Google auth (BigLake and other Google-fronted REST catalogs)

```python
register_workspace(
    "tarchia",
    OpteryxConnector,
    catalog=IcebergMetastore,
    catalog_type="rest",
    uri="https://biglake.googleapis.com/iceberg/v1/restcatalog",
    warehouse="bl://projects/<project>/catalogs/<catalog>",
    auth={"type": "google", "google": {"scopes": ["https://www.googleapis.com/auth/cloud-platform"]}},
    **{"header.x-goog-user-project": "<project>"},
)
```

`auth={"type": "google", ...}` selects pyiceberg's built-in `GoogleAuthManager`, which authenticates via Application Default Credentials and **refreshes the token on every request** — safe for a long-lived server (a manually fetched `gcloud auth print-access-token` bearer token, by contrast, expires within the hour and is only good for one-off scripts/tests). In production this picks up Cloud Run's attached service account automatically, the same way the rest of the deployment already does — no explicit `credentials_path` needed. Stored-credential catalogs need no code at all: pass pyiceberg's `token=` or `credential=` the same way.

This is wired into [`worker.opteryx`](../worker.opteryx/app/worker.py) as the `tarchia` workspace, alongside the native `mabel_data` registration - reads from it go through the exact same query path as any native table (verified with a real `SELECT ... FROM tarchia.interop_ns.people`).

**Published on PyPI**: `worker.opteryx` depends on `opteryx-iceberg>=0.1.2` as a real dependency in its `pyproject.toml`, alongside `opteryx-core`, `opteryx-catalog[kms]` and `opteryx-access` - no `sys.path` sibling-checkout shim, no vendoring. The `tarchia` registration therefore works in a production Cloud Run deploy the same way it works locally. (The `sys.path` convention still applies to this repo's *own* tests, which resolve sibling `opteryx-catalog`/`opteryx-core` checkouts - see [Local development](#local-development).)

### SQL catalogs: the pyiceberg catalog name must equal the workspace prefix

`IcebergMetastore` passes the Opteryx workspace prefix straight through as pyiceberg's catalog *name*: `load_catalog(workspace, ...)`. For REST/Hive/Glue that name is a local label and nothing on the wire depends on it. **For `catalog_type="sql"` it is part of the data.** pyiceberg's `SqlCatalog` stores its name in the `catalog_name` column of its metadata tables and filters every lookup on it, so a table written under catalog name `warehouse` is simply not there when read back under the name `my_workspace`.

The failure is silent and unhelpful: `load_dataset` gets `NoSuchTableError` and raises a bare `DatasetNotFound`, exactly as if the table had never been created. There is no hint that the metadata row exists under a different catalog name.

So when pointing Opteryx at a local/SQL Iceberg catalog, whoever wrote the tables must have used the same catalog name as the workspace prefix you register:

```python
# writer
SqlCatalog("my_workspace", uri="sqlite:///.../catalog.db", warehouse="file:///...")

# reader - the prefix here becomes the pyiceberg catalog name
register_workspace("my_workspace", OpteryxConnector, catalog=UnrestrictedIcebergMetastore, catalog_type="sql", ...)
```

If you already have a SQL catalog written under a different name, either register the Opteryx workspace under that name or rewrite the `catalog_name` values in the catalog's metadata table.

## What's supported

- `SELECT` queries against existing Iceberg tables, including predicate pushdown/pruning via standard Iceberg manifest bounds (`min_values`/`max_values`/`null_counts`).
- Schema introspection (`SHOW COLUMNS`, information_schema).
- Time travel: `VERSION AS OF <snapshot-id>`, `VERSION AS OF PREVIOUS` (walks Iceberg's `parent_snapshot_id`), and `TIMESTAMP AS OF '<ts>'` (point-in-time, resolved against the commit history; a timestamp before the first commit is an error, not an empty result).
- `SHOW MANIFEST FOR <table>` — one row per live data file, with the real decoded Iceberg bounds.
- `SHOW SNAPSHOTS FOR <table>` — the commit history, newest first.
- `information_schema.tables` / `.columns`, and `SHOW COLUMNS`. (`DESCRIBE` is not supported by opteryx-core for any table - it is rejected as an `EXPLAIN TABLE` query.)
- Partitioned tables, including predicates over the partition column.
- Schema evolution: a time-travel read resolves the *historical* schema the snapshot was written under, so a snapshot taken before an `ADD COLUMN` does not report the column that did not exist yet.

### What `SHOW SNAPSHOTS` can and cannot tell you about an Iceberg table

opteryx-core's snapshot output was defined against opteryx-catalog's own commit records, which are richer than Iceberg's. `IcebergSnapshot` (in `dataset.py`) adapts a pyiceberg snapshot into that shape; three columns are **always `NULL`** for an Iceberg-backed table, because the Iceberg spec has nowhere to record them:

| Column | Why it is null |
|---|---|
| `author` | Iceberg records no committer identity |
| `commit_message` | Iceberg records no commit message |
| `user_created` | opteryx's user-vs-system commit distinction has no Iceberg equivalent |

`operation_type` is the Iceberg spec's own lowercase name (`append`, `overwrite`, `delete`, `replace`). Note that a row-level **delete is reported as `overwrite`** — that is what pyiceberg commits, not a mistranslation here.

The counter columns come from the snapshot summary, which Iceberg holds as strings and populates *per operation*: an `append` records no `deleted-*` counters at all. Those arrive as `NULL` — meaning "this commit does not report it", not zero, which would claim the commit deleted nothing.

## What's not (yet)

- **Merge-on-read delete files.** Iceberg v2 can express a delete either by rewriting the data file (copy-on-write) or by committing a *delete file* beside it (merge-on-read) that the reader must subtract at scan time. This package does not do that subtraction, so a table carrying delete files is **refused** with a `NotImplementedError` naming the cause, rather than read — reading it without applying them would return deleted rows as live data, silently.

  Copy-on-write tables are unaffected, and that is everything `pyiceberg` itself writes (it has no merge-on-read write path). **Spark, Flink and Trino do write merge-on-read deletes**, so this is the limitation most likely to be met on a remote catalog written by something other than pyiceberg. Lifting it means implementing positional and equality delete application.
- Any write path: `CREATE`/`DROP`/`ALTER`/`INSERT`/`rename` all raise `NotImplementedError` — that's Tier 2.
- Iceberg views (Iceberg's view spec has no equivalent here yet).
- Opteryx's own sketch-based pruning stats (`min_k_hashes`/histograms) — standard Iceberg manifests don't carry them; queries fall back to standard bounds-based pruning.
- Some nested Iceberg types. A struct, a map with string keys, and a list with a struct or map at the bottom read as JSON text; a list of a plain scalar reads as an ARRAY. A list of lists of a scalar, a map whose keys are not strings, and a list of a scalar with no ARRAY element type are refused by `IcebergDataset.schema()` with the cause named, rather than misrepresented. Note this makes a table with *any* such column unreadable, including for a query that touches only its other columns.
- Iceberg's own triggers/views listings (`list_views`/`list_triggers` return empty, which is the truthful answer for a Tier 1 reader rather than a stub).

## Writing

`CREATE SCHEMA ws.ns`, `CREATE TABLE ws.ns.t (...)` and `INSERT INTO ws.ns.t SELECT ...` (`opteryx_iceberg/writes.py`):

- Every file is written by rugo: Parquet data files (with the Iceberg field-ids in their schema) under `<table>/data/`, and the Avro manifest and manifest list. pyiceberg is only the catalog client: `create_namespace`, `create_table`, and one `commit_table` per INSERT.
- An INSERT is an Iceberg fast append: one new manifest of ADDED entries, plus the parent snapshot's manifests carried over. The commit asserts `main` still points at the parent, so a concurrent commit fails the INSERT (`ConcurrentModificationError`), never overwrites.
- Created tables are format-version 2, unpartitioned, unsorted, default properties. Types: BOOLEAN, INTEGER, BIGINT, REAL, DOUBLE, DATE, microsecond TIMESTAMP, VARCHAR, VARBINARY, UUID, DECIMAL(p ≤ 18); anything else is refused at CREATE TABLE.
- Bounds are written for numeric, date, timestamp and decimal columns. String, binary and UUID columns get null and value counts but no bounds: the engine's string statistic is an 8-byte prefix key, from which a valid upper bound cannot be rebuilt.
- Writes go to local files directly and to S3/GCS through opteryx-catalog's FileIO, built with opteryx-core's endpoint and credentials, under the same identity check as reads.
- Refused by name: CTAS, DROP/RENAME, partitioned tables, nested columns, non-v2 tables, and tables setting `write.data.path`, object-storage layout, or a non-Parquet `write.format.default`.

## Local development

Sibling `opteryx-catalog`/`opteryx-core` checkouts are referenced via `sys.path` insertion in test files (see `tests/`), never `pip install -e` - see those repos' own conventions.

## Testing

```bash
python -m pytest tests/ -v
```

Tests run against pyiceberg's own local `SqlCatalog` (SQLite metadata + local-disk `FileIO`) — no server, no Docker required.

### Lakekeeper (REST catalog over S3, in CI)

Four modules read through a real Iceberg REST catalog — [Lakekeeper](https://docs.lakekeeper.io), backed by Postgres and Silo (S3) — started by `scripts/lakekeeper.sh`:

- `tests/test_lakekeeper_rest.py`: a basic two-commit table — rows, filters, `information_schema`, time travel, `SHOW SNAPSHOTS` — and that manifests are read through opteryx-core.
- `tests/test_lakekeeper_queries.py`: a query battery. opteryx-core's planets, satellites, missions, TPC-H sf0.01, TPC-DS sf0.01 and `clickbench_tiny` data is loaded into Lakekeeper. Handwritten queries plus core's 22 TPC-H, 99 TPC-DS and 43 ClickBench queries must each give the same outcome as the same query over the original parquet on local disk: the same rows, or the same error. `OPTERYX_CORE_ROOT` names the opteryx-core checkout the data and SQL come from.
- `tests/test_lakekeeper_surface.py`: table shapes and metadata — a partitioned table (identity and day transforms), schema evolution, a copy-on-write delete, a tag, `TIMESTAMP AS OF`, several namespaces, the refusal for a missing table, a nested (list) column, and that data files take the native parquet scan.
- `tests/test_lakekeeper_refusals.py`: the identity check against a catalog that trips it — a warehouse serving a different S3 endpoint string, and one vending STS credentials. Both reads must be refused with the cause named.

CI runs all four as the `lakekeeper` job, and opteryx-core's regression suite runs them against its own build. Locally they need Docker and skip without the stack:

```bash
set -a; source scripts/lakekeeper.env; set +a
scripts/lakekeeper.sh up
OPTERYX_CORE_ROOT=../opteryx-core python -m pytest tests/test_lakekeeper_*.py -v
scripts/lakekeeper.sh down
```

The read tests run against two warehouses: one with STS and remote signing off (Lakekeeper serves only the S3 endpoint) and one on Lakekeeper's default (remote signing on, which also serves `py-io-impl: FsspecFileIO`). The other two warehouses exist for the refusal tests. Test tables' files are always written with pyiceberg's `PyArrowFileIO` and the stack's static key; commits still go through Lakekeeper. Writing through what the default warehouse serves (`FsspecFileIO` behind the remote signer) was intermittently refused by S3. Partitioned writes need `pyiceberg-core`, which the `test` extra includes. See the script header for why the stack runs on the host network.

### Real REST-catalog interop check

Snowflake Open Catalog is closed to new signups as of 2026 (Snowflake now points new customers at Horizon Catalog, which needs a full paid-account trial). Instead, real wire-protocol compatibility is verified against **Google Lakehouse for Apache Iceberg (BigLake)**, reusing the existing `mabeldev` GCP project:

- Catalog: `projects/mabeldev/catalogs/opteryx-iceberg-tier1-test` (type `biglake`, credential-mode `end-user`), storing data under `gs://tarchia/iceberg-tier1-test`.
- Verified manually (not in CI - needs a live GCP access token): `dataset_exists`, `load_dataset`, `schema()` type mapping, `scan()` including real Iceberg bounds-byte decoding (`min_values`/`max_values`/`field_ids`), and `get_relation` for both hit and miss, all through `opteryx_iceberg.IcebergMetastore` against a table (`interop_ns.people`) written independently via plain `pyiceberg.catalog.rest.RestCatalog`.
- Connecting needed `GOOGLE_APPLICATION_CREDENTIALS` set in-process (not just `gcloud auth activate-service-account`) — at the time metadata was read through `PyArrowFileIO`, whose GCS backend otherwise hung trying to reach the GCE metadata server for ADC. Metadata now reads through opteryx-core's GCS client; this check has not been re-run since. Warehouse URI format is `bl://projects/<project>/catalogs/<catalog>` (not a bare `projects/...` path).
- This catalog/table is being kept around (not torn down) for reuse in future Tier 1/Tier 2 verification.
