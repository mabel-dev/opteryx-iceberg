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

**Local-dev only for now**: `worker.opteryx` reaches `opteryx_iceberg` via the same `sys.path` sibling-checkout convention as `opteryx-core`/`opteryx-catalog`/`opteryx-access` (never `pip install -e`) - see its `pyproject.toml`, which does *not* yet list `opteryx-iceberg` as a real dependency, since it isn't published anywhere yet. A production Cloud Run deploy of `worker.opteryx` would need that resolved first (publish `opteryx-iceberg` somewhere installable, or vendor it) - the `tarchia` registration works today for local runs only.

## What's supported

- `SELECT` queries against existing Iceberg tables, including predicate pushdown/pruning via standard Iceberg manifest bounds (`min_values`/`max_values`/`null_counts`).
- Schema introspection (`DESCRIBE`, information_schema).

## What's not (yet)

- Any write path: `CREATE`/`DROP`/`ALTER`/`INSERT`/`rename` all raise `NotImplementedError` — that's Tier 2.
- Iceberg views (Iceberg's view spec has no equivalent here yet).
- Opteryx's own sketch-based pruning stats (`min_k_hashes`/histograms) — standard Iceberg manifests don't carry them; queries fall back to standard bounds-based pruning.
- Nested Iceberg types (struct/map/list) — `IcebergDataset.schema()` raises rather than silently misrepresenting them.

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
