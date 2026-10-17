#!/usr/bin/env bash
# A throwaway Lakekeeper Iceberg REST catalog for tests/test_lakekeeper_rest.py.
#
#   set -a; source scripts/lakekeeper.env; set +a
#   scripts/lakekeeper.sh up      # Postgres + Silo (S3) + Lakekeeper, bootstrapped
#   scripts/lakekeeper.sh down    # remove the containers
#
# CI (.github/workflows/tests.yaml, job `lakekeeper`) runs exactly this, with
# scripts/lakekeeper.env loaded into the job environment, so the stack the
# tests see locally and in CI cannot drift.
#
# Every container is on the HOST network. That is load-bearing, not a
# convenience: Lakekeeper hands its storage profile's S3 endpoint to every
# client in the load-table response, and opteryx_iceberg.fileio refuses a read
# whose properties name a different endpoint from opteryx-core's
# AWS_S3_ENDPOINT. On a bridge network Lakekeeper would reach S3 as
# http://<service>:9000 while the tests reach it as http://localhost:9000, and
# every read would be (correctly) refused. On the host network both are
# localhost:9000, so the profile endpoint and AWS_S3_ENDPOINT are one string.
#
# Two warehouses, both with STS off (every client reads with its own key - here
# the same static key the warehouses hold):
#   LAKEKEEPER_WAREHOUSE          remote signing off: a table load serves only
#                                 the S3 endpoint/region/path-style.
#   LAKEKEEPER_DEFAULT_WAREHOUSE  Lakekeeper's default, remote signing on: a
#                                 table load also serves a signer and
#                                 `py-io-impl: FsspecFileIO`, which pyiceberg
#                                 lets override the catalog's own FileIO.

set -euo pipefail

LAKEKEEPER_IMAGE="quay.io/lakekeeper/catalog:v0.14.0"
POSTGRES_IMAGE="postgres:17"
# Silo is the maintained MinIO fork Lakekeeper's own examples run against.
SILO_IMAGE="pgsty/silo:RELEASE.2026-09-03T13-18-01Z"

DB=opteryx-iceberg-lk-db
S3=opteryx-iceberg-lk-s3
CATALOG=opteryx-iceberg-lk-catalog

PG_URL="postgresql://postgres:postgres@localhost:5432/postgres"
# Lakekeeper encrypts stored warehouse credentials with this; the stack is
# disposable, so a fixed key is fine.
PG_ENCRYPTION_KEY="opteryx-iceberg-ci-not-a-secret"

wait_for() {
    local what="$1"
    shift
    for _ in $(seq 1 60); do
        if "$@" > /dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done
    echo "ERROR: $what was not ready after 60s" >&2
    return 1
}

up() {
    : "${LAKEKEEPER_URI:?load scripts/lakekeeper.env first}"
    : "${LAKEKEEPER_WAREHOUSE:?}" "${LAKEKEEPER_DEFAULT_WAREHOUSE:?}" "${LAKEKEEPER_BUCKET:?}"
    : "${AWS_S3_ENDPOINT:?}"
    : "${AWS_ACCESS_KEY_ID:?}" "${AWS_SECRET_ACCESS_KEY:?}" "${AWS_REGION:?}"

    docker run -d --name "$DB" --network host \
        -e POSTGRES_PASSWORD=postgres \
        "$POSTGRES_IMAGE"
    # Over TCP, not the socket: initdb's temporary server answers on the
    # socket only, then restarts - a socket probe can pass before the real one.
    wait_for "Postgres" docker exec "$DB" pg_isready -h 127.0.0.1 -U postgres

    docker run -d --name "$S3" --network host \
        -e MINIO_ROOT_USER="$AWS_ACCESS_KEY_ID" \
        -e MINIO_ROOT_PASSWORD="$AWS_SECRET_ACCESS_KEY" \
        "$SILO_IMAGE" server /data
    wait_for "Silo" curl -sf "$AWS_S3_ENDPOINT/minio/health/ready"
    docker exec "$S3" mc alias set ci "$AWS_S3_ENDPOINT" "$AWS_ACCESS_KEY_ID" "$AWS_SECRET_ACCESS_KEY"
    docker exec "$S3" mc mb "ci/$LAKEKEEPER_BUCKET"

    local lk_env=(
        -e LAKEKEEPER__PG_ENCRYPTION_KEY="$PG_ENCRYPTION_KEY"
        -e LAKEKEEPER__PG_DATABASE_URL_READ="$PG_URL"
        -e LAKEKEEPER__PG_DATABASE_URL_WRITE="$PG_URL"
        -e LAKEKEEPER__BASE_URI="$LAKEKEEPER_URI"
        # Its Prometheus endpoint defaults to 9000 - Silo's S3 port, on a
        # shared host network.
        -e LAKEKEEPER__METRICS__PORT=9091
    )
    docker run --rm --network host "${lk_env[@]}" "$LAKEKEEPER_IMAGE" migrate
    docker run -d --name "$CATALOG" --network host "${lk_env[@]}" "$LAKEKEEPER_IMAGE" serve
    wait_for "Lakekeeper" curl -sf "$LAKEKEEPER_URI/health"

    curl -sS --fail-with-body -X POST "$LAKEKEEPER_URI/management/v1/bootstrap" \
        -H "Content-Type: application/json" \
        --data '{"accept-terms-of-use": true}'
    echo

    # Remote signing off: a table load serves the endpoint only.
    create_warehouse "$LAKEKEEPER_WAREHOUSE" lakekeeper ', "remote-signing-enabled": false'
    # Lakekeeper's DEFAULT (remote signing on): a table load also serves a
    # signer and `py-io-impl: FsspecFileIO` - the case opteryx-iceberg must
    # not let replace its own FileIO.
    create_warehouse "$LAKEKEEPER_DEFAULT_WAREHOUSE" lakekeeper-default ''
}

# create_warehouse NAME KEY_PREFIX EXTRA_PROFILE_FIELDS
create_warehouse() {
    curl -sS --fail-with-body -X POST "$LAKEKEEPER_URI/management/v1/warehouse" \
        -H "Content-Type: application/json" \
        --data @- <<EOF
{
  "warehouse-name": "$1",
  "project-id": "00000000-0000-0000-0000-000000000000",
  "storage-profile": {
    "type": "s3",
    "bucket": "$LAKEKEEPER_BUCKET",
    "key-prefix": "$2",
    "endpoint": "$AWS_S3_ENDPOINT",
    "region": "$AWS_REGION",
    "path-style-access": true,
    "flavor": "s3-compat",
    "sts-enabled": false$3
  },
  "storage-credential": {
    "type": "s3",
    "credential-type": "access-key",
    "access-key-id": "$AWS_ACCESS_KEY_ID",
    "secret-access-key": "$AWS_SECRET_ACCESS_KEY"
  }
}
EOF
    echo
}

down() {
    docker rm -f "$CATALOG" "$S3" "$DB"
}

logs() {
    for container in "$CATALOG" "$S3" "$DB"; do
        echo "===== $container ====="
        docker logs "$container" 2>&1 | tail -n 200
    done
}

case "${1:-}" in
    up) up ;;
    down) down ;;
    logs) logs ;;
    *)
        echo "usage: $0 up|down|logs" >&2
        exit 2
        ;;
esac
