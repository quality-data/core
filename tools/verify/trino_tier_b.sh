#!/usr/bin/env bash
# The contract tier B on Trino (decision 0090, step 15): Trino 484 over
# RustFS 1.0.5's Iceberg warehouse (trino_engine_lib.sh), its HTTP port on a
# random port of 127.0.0.1 only, and the compiled bundles of one synthetic
# dataset's chain run by quality-jobs' stand-in job runner
# (crates/quality-jobs/tests/tier_b.rs). The test binary is built first,
# under the build lock, and handed here, so nothing builds while the
# containers run:
#
#   cargo test -p quality-jobs --test tier_b --no-run   # prints the binary
#   tools/verify/trino_tier_b.sh target/debug/deps/tier_b-<hash>
#
# Containers, network and volumes are named qd-verify-tierb-… and removed
# on exit, also on failure.
set -u
export LC_ALL=C
TEST=${1:?the tier_b test binary}
TEST=$(realpath "$TEST")
cd "$(dirname "$0")/../.."
P=qd-verify-tierb
. tools/verify/trino_engine_lib.sh

te_rustfs || exit 1
TRINO_PUBLISH=1 te_trino || exit 1
URL=$(te_trino_url)
info "Trino at $URL"
if QD_TIER_B_URL=$URL "$TEST" --test-threads=1 --nocapture; then
  ok "tier B: the chain's bundles on Trino over RustFS"
else
  ko "tier B failed"
fi
[ "$FAILED" = 0 ] && echo "ALL OK" || echo "SOME CHECKS FAILED"
exit "$FAILED"
