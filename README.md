# opteryx-iceberg

Read-only Apache Iceberg `Metastore`/`FileIO` backend for [opteryx-catalog](https://github.com/mabel-dev/opteryx-catalog), letting an Opteryx workspace query tables from an external Iceberg catalog (REST, SQL, Hive, Glue - whatever [pyiceberg](https://py.iceberg.apache.org/)'s own catalog loader supports) side by side with native Firestore/GCS-backed tables.

This is Tier 1 of Opteryx's Iceberg support: **reads only**. Writing real Iceberg tables from Opteryx (Tier 2) and serving Opteryx's own catalog as an Iceberg REST endpoint (Tier 3) are separate, later work.

Kept as its own package - not merged into `opteryx-catalog` or `opteryx-core` - because it depends on `pyiceberg`, which pulls in `pyarrow`/`pydantic`. Both of those repos are deliberately free of that dependency chain; Iceberg support is optional, the same way `opteryx-access` is.

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
    catalog_type="rest",       # or "sql", "hive", "glue" - anything pyiceberg's loader supports
    uri="https://...",
    warehouse="s3://...",
)
```

Do **not** pass `workspace=` yourself — `OpteryxConnector` injects it automatically (as the registered prefix) when it instantiates `IcebergMetastore`; passing it explicitly raises a duplicate-keyword-argument error.

Native (Firestore/GCS-backed) workspaces are entirely unaffected - this only applies to workspaces explicitly registered with `catalog=IcebergMetastore`.

**Config passes through to pyiceberg verbatim — nesting included.** Every kwarg after `catalog=` is forwarded untouched to `pyiceberg.catalog.load_catalog`, so pyiceberg's own config shapes (`auth={...}`, `token=`, `credential=`) are used directly. (Earlier versions required flat `auth_type`/`google_auth_scopes` kwargs because opteryx-core's connector cache hashed registration kwargs and a `dict` value broke it; that cache is now keyed by workspace name in opteryx-core's resolution-first connector layer, the flattening is retired, and passing the old flat kwargs raises a clear `ValueError`.)

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
register_workspace("my_workspace", OpteryxConnector, catalog=IcebergMetastore, catalog_type="sql", ...)
```

If you already have a SQL catalog written under a different name, either register the Opteryx workspace under that name or rewrite the `catalog_name` values in the catalog's metadata table.

## What's supported

- `SELECT` queries against existing Iceberg tables, including predicate pushdown/pruning via standard Iceberg manifest bounds (`min_values`/`max_values`/`null_counts`).
- Schema introspection (`DESCRIBE`, information_schema).
- Time travel: `VERSION AS OF <snapshot-id>`, `VERSION AS OF PREVIOUS` (walks Iceberg's `parent_snapshot_id`), and `TIMESTAMP AS OF '<ts>'` (point-in-time, resolved against the commit history; a timestamp before the first commit is an error, not an empty result).
- `SHOW MANIFEST FOR <table>` — one row per live data file, with the real decoded Iceberg bounds.
- `SHOW SNAPSHOTS FOR <table>` — the commit history, newest first.
- `information_schema.tables` / `.columns`, and `SHOW COLUMNS` / `DESCRIBE`.
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
- Nested Iceberg types (struct/map/list) — `IcebergDataset.schema()` raises rather than silently misrepresenting them. Note this makes a table with *any* nested column unreadable, including for a query that touches only its scalar columns.
- Iceberg's own triggers/views listings (`list_views`/`list_triggers` return empty, which is the truthful answer for a Tier 1 reader rather than a stub).

## Local development

Sibling `opteryx-catalog`/`opteryx-core` checkouts are referenced via `sys.path` insertion in test files (see `tests/`), never `pip install -e` - see those repos' own conventions.

## Testing

```bash
python -m pytest tests/ -v
```

Tests run against pyiceberg's own local `SqlCatalog` (SQLite metadata + local-disk `FileIO`) — no server, no Docker required.

### Real REST-catalog interop check

Snowflake Open Catalog is closed to new signups as of 2026 (Snowflake now points new customers at Horizon Catalog, which needs a full paid-account trial). Instead, real wire-protocol compatibility is verified against **Google Lakehouse for Apache Iceberg (BigLake)**, reusing the existing `mabeldev` GCP project:

- Catalog: `projects/mabeldev/catalogs/opteryx-iceberg-tier1-test` (type `biglake`, credential-mode `end-user`), storing data under `gs://tarchia/iceberg-tier1-test`.
- Verified manually (not in CI - needs a live GCP access token): `dataset_exists`, `load_dataset`, `schema()` type mapping, `scan()` including real Iceberg bounds-byte decoding (`min_values`/`max_values`/`field_ids`), and `get_relation` for both hit and miss, all through `opteryx_iceberg.IcebergMetastore` against a table (`interop_ns.people`) written independently via plain `pyiceberg.catalog.rest.RestCatalog`.
- Connecting needs `GOOGLE_APPLICATION_CREDENTIALS` set in-process (not just `gcloud auth activate-service-account`) — `PyArrowFileIO`'s GCS backend otherwise hangs trying to reach the GCE metadata server for ADC. Warehouse URI format is `bl://projects/<project>/catalogs/<catalog>` (not a bare `projects/...` path).
- This catalog/table is being kept around (not torn down) for reuse in future Tier 1/Tier 2 verification.
