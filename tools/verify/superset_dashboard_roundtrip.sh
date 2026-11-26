#!/bin/bash
# Verification V4 of decision 0089: dashboard export and import are
# deterministic in Superset 6.1.0. A bundle in Superset's export format
# ("assets", imported by Superset's assets importer, which overwrites by
# UUID; one database, one dataset, two charts, one dashboard, UUIDs fixed:
# superset_verify_init.py's definition, standing in for step 9's generator)
# is imported into a fresh Superset, exported, normalised and compared with
# its source; the archive is built twice and compared by SHA-256; it is
# imported twice, and again after an author edited a shipped chart and
# saved a copy of it; the database template is rendered with another
# endpoint; the embedded record keeps its fixed UUID. Trino 484 and RustFS
# as V2: importing a new database connects to it (Superset lists its
# catalogs and schemas for its permissions), as superset-init will, with the
# module's client token. Only names, UUIDs, counts and digests are printed.
#
# Containers, volumes and the network qd-verify-v4-…, removed on exit. Run
# alone, from the repository root:
#
#   bash tools/verify/superset_dashboard_roundtrip.sh
set -u
cd "$(dirname "$0")/../.."
P=qd-verify-v4
. tools/verify/superset_lib.sh
superset_wait_quiet
superset_image || exit 1
superset_lake || exit 1
superset_files
rules
chmod 777 "$D/bundle"
superset_edge_up || exit 1
superset_trino || exit 1
superset_gold
superset_stores_up || exit 1
EMBEDDED=1 superset_up || exit 1
state() { sinit state 2>/dev/null | tail -1; }
C1=8f6a3c1e-0089-4c10-9000-0000000000c1

echo "== The source, its archive twice"
sinit bundle /bundle/src trino.internal:8443
sinit archive /bundle/src /bundle/a1.zip
sleep 2
sinit archive /bundle/src /bundle/a2.zip
expect "the two archives are identical (SHA-256)" "$(sha256sum <"$D/bundle/a1.zip" | cut -c1-64)" \
  "$(sha256sum <"$D/bundle/a2.zip" | cut -c1-64)"
note "archive: $(python3 -I -c 'import sys, zipfile; print(*zipfile.ZipFile(sys.argv[1]).namelist())' "$D/bundle/a1.zip")"

echo "== Import into a fresh Superset, export, normalise, compare"
sinit import /bundle/a1.zip | tail -1
sinit embed "$DASH_UUID" "$EMBED_UUID" "https://$APP" | tail -1
S1=$(state)
expect "counts after the first import (databases datasets charts dashboards embedded)" "1 1 2 1 1" \
  "$(jq -r '[.databases, .datasets, .charts, .dashboards, .embedded | length] | join(" ")' <<<"$S1")"
expect "the dashboard's UUID is the definition's" "$DASH_UUID" "$(jq -r '.dashboards[0][0]' <<<"$S1")"
sinit export /bundle/out | tail -1
if D1=$(diff -r "$D/bundle/src" "$D/bundle/out"); then ok "the normalised export equals the source"; else
  ko "the normalised export differs from the source:"
  printf '%s\n' "$D1" | head -60
fi

echo "== A second import changes nothing"
sinit import /bundle/a1.zip | tail -1
expect "UUIDs, ids and counts after the second import" "$S1" "$(state)"

echo "== An author edits a shipped chart and saves a copy; an update imports again"
sinit edit-chart "$C1" "Records by cut, edited by an author"
sinit copy-chart "$C1" "Records by cut, an author's copy"
sinit import /bundle/a1.zip | tail -1
S2=$(state)
expect "the shipped chart is overwritten, the copy is kept" \
  '["Conformity by cut","Records by cut","Records by cut, an author'"'"'s copy"]' "$(jq -c .chart_titles <<<"$S2")"
expect "the shipped charts keep their ids" "$(jq -c '.charts' <<<"$S1")" \
  "$(jq -c '[.charts[] | select(.[0] | startswith("8f6a3c1e-0089"))]' <<<"$S2")"
expect "the embedded record keeps its fixed UUID" "$(jq -c .embedded <<<"$S1")" "$(jq -c .embedded <<<"$S2")"

echo "== The database template rendered with another endpoint, imported over the installed one"
sinit bundle /bundle/other trino-b.internal:8443
sinit archive /bundle/other /bundle/b.zip
sinit import /bundle/b.zip | tail -1
S3=$(state)
expect "the database's URI follows the template, its UUID and id stay" \
  "$(jq -c '.databases' <<<"$S1") trino://bi-superset@trino-b.internal:8443/gold" \
  "$(jq -c '.databases' <<<"$S3") $(jq -r '.uri[0]' <<<"$S3")"
expect "the embedded record still the same" "$(jq -c .embedded <<<"$S1")" "$(jq -c .embedded <<<"$S3")"

echo
echo "passed $PASSED, failed $FAILED"
[ "$FAILED" = 0 ] && echo "ALL OK" || exit 1
