#!/bin/bash
# benchmark/ceb_1a, the plugins of common.sh
# output: $DATA/ceb_1a/ (report.txt, per_query.tsv, ds/), log: $DATA/ceb_1a.log
set -o pipefail
source "$(dirname "$0")/common.sh"
run_bench ceb_1a ceb_1a
