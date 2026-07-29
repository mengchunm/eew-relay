#!/bin/sh

set -eu
umask 077

BACKUP_ROOT=${BACKUP_ROOT:-/var/backups/eew-relay}
EEW_ROOT=${EEW_ROOT:-/opt/eew-relay}
SCALE_ROOT=${SCALE_ROOT:-/opt/eew-relay-scale}
BARK_ROOT=${BARK_ROOT:-/opt/bark-server}
BARK_DB=${BARK_DB:-/opt/bark-server/data/bark.db}
BBOLT=${BBOLT:-/usr/local/bin/bbolt}
DOCKER=${DOCKER:-/usr/bin/docker}
RETENTION_DAYS=${RETENTION_DAYS:-14}

case "$BACKUP_ROOT" in
  /*) ;;
  *)
    printf 'backup root must be an absolute path\n' >&2
    exit 1
    ;;
esac
if [ "$BACKUP_ROOT" = / ]; then
  printf 'refusing to use the filesystem root as backup root\n' >&2
  exit 1
fi
case "$RETENTION_DAYS" in
  '' | *[!0-9]*)
    printf 'retention days must be a non-negative integer\n' >&2
    exit 1
    ;;
esac

stamp=$(date -u +%Y%m%dT%H%M%SZ)
tmp="$BACKUP_ROOT/.$stamp.tmp.$$"
final="$BACKUP_ROOT/$stamp"
if [ -e "$final" ]; then
  printf 'backup destination already exists: %s\n' "$final" >&2
  exit 1
fi

cleanup() {
  if [ -n "${tmp:-}" ] && [ -d "$tmp" ]; then
    rm -rf -- "$tmp"
  fi
}
trap cleanup EXIT HUP INT TERM

install -d -m 700 "$BACKUP_ROOT" "$tmp"
install -m 600 "$EEW_ROOT/config.yaml" "$tmp/config.yaml"
install -m 600 "$EEW_ROOT/docker-compose.yml" "$tmp/docker-compose.yml"
if [ -f "$EEW_ROOT/relay.env" ]; then
  install -m 600 "$EEW_ROOT/relay.env" "$tmp/relay.env"
fi
if [ -f "$EEW_ROOT/.env" ]; then
  install -m 600 "$EEW_ROOT/.env" "$tmp/app.env"
fi
install -m 600 "$SCALE_ROOT/docker-compose.yml" "$tmp/docker-compose.scale.yml"
install -m 600 "$SCALE_ROOT/nats.conf" "$tmp/nats.conf"
install -m 600 "$SCALE_ROOT/.env" "$tmp/scale.env"
install -m 600 "$BARK_ROOT/docker-compose.yml" "$tmp/docker-compose.bark.yml"
if [ -f "$BARK_ROOT/.env" ]; then
  install -m 600 "$BARK_ROOT/.env" "$tmp/bark.env"
fi

"$DOCKER" exec eew-postgres pg_isready -U eew -d eew >/dev/null
"$DOCKER" exec eew-postgres pg_dump -U eew -d eew --format=custom --compress=6 >"$tmp/eew-postgres.dump"
chmod 600 "$tmp/eew-postgres.dump"
"$DOCKER" exec -i eew-postgres pg_restore --list <"$tmp/eew-postgres.dump" >/dev/null
subscriptions=$("$DOCKER" exec eew-postgres psql -U eew -d eew -tAc 'SELECT count(*) FROM eew_subscriptions')
locations=$("$DOCKER" exec eew-postgres psql -U eew -d eew -tAc 'SELECT count(*) FROM eew_subscription_locations')

"$DOCKER" exec bark-mysql sh -lc 'MYSQL_PWD="$MYSQL_PASSWORD" mysqladmin ping -h 127.0.0.1 -ubark --silent' >/dev/null
"$DOCKER" exec bark-mysql sh -lc 'MYSQL_PWD="$MYSQL_PASSWORD" mysqldump -ubark --single-transaction --quick --skip-lock-tables --no-tablespaces bark' >"$tmp/bark-mysql.sql"
chmod 600 "$tmp/bark-mysql.sql"
grep -q 'CREATE TABLE.*devices' "$tmp/bark-mysql.sql"
bark_devices=$("$DOCKER" exec bark-mysql sh -lc 'MYSQL_PWD="$MYSQL_PASSWORD" mysql -N -ubark bark -e "SELECT count(*) FROM devices"')

legacy_subscriptions=0
if [ -f "$EEW_ROOT/data/subscriptions.json" ]; then
  install -m 600 "$EEW_ROOT/data/subscriptions.json" "$tmp/subscriptions.legacy.json"
  jq -e 'type == "array"' "$tmp/subscriptions.legacy.json" >/dev/null
  legacy_subscriptions=$(jq 'length' "$tmp/subscriptions.legacy.json")
fi
if [ -f "$EEW_ROOT/data/history.json" ]; then
  install -m 600 "$EEW_ROOT/data/history.json" "$tmp/history.json"
fi
service_health_included=0
if [ -f "$EEW_ROOT/data/service-health.jsonl" ]; then
  install -m 600 "$EEW_ROOT/data/service-health.jsonl" "$tmp/service-health.jsonl"
  service_health_included=1
fi

bark_ok=0
if [ -f "$BARK_DB" ] && [ -x "$BBOLT" ]; then
  attempt=1
  while [ "$attempt" -le 3 ]; do
    cp --sparse=always -- "$BARK_DB" "$tmp/bark.legacy.db"
    chmod 600 "$tmp/bark.legacy.db"
    if "$BBOLT" check "$tmp/bark.legacy.db" >/dev/null 2>&1; then
      bark_ok=1
      break
    fi
    attempt=$((attempt + 1))
    sleep 1
  done
  if [ "$bark_ok" -ne 1 ]; then
    printf 'legacy Bark bbolt snapshot failed integrity check\n' >&2
    exit 1
  fi
fi

cat >"$tmp/MANIFEST" <<EOF
created_at=$stamp
postgres_subscriptions=$subscriptions
postgres_locations=$locations
mysql_bark_devices=$bark_devices
legacy_json_subscriptions=$legacy_subscriptions
legacy_bbolt_included=$bark_ok
service_health_included=$service_health_included
EOF
chmod 600 "$tmp/MANIFEST"

(
  cd "$tmp"
  find . -maxdepth 1 -type f ! -name SHA256SUMS -printf '%P\n' \
    | LC_ALL=C sort \
    | xargs sha256sum
) >"$tmp/SHA256SUMS"
chmod 600 "$tmp/SHA256SUMS"

mv -- "$tmp" "$final"
tmp=

find "$BACKUP_ROOT" -mindepth 1 -maxdepth 1 -type d \
  -name '20??????T??????Z' -mtime "+$RETENTION_DAYS" -exec rm -rf -- {} +

printf 'backup complete: %s subscriptions=%s bark_devices=%s\n' "$final" "$subscriptions" "$bark_devices"
