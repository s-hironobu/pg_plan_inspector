#!/bin/bash
# benchmark/stats_ceb, the plugins of common.sh
# output: $DATA/stats_ceb/ (report.txt, per_query.tsv, ds/), log: $DATA/stats_ceb.log
set -o pipefail
source "$(dirname "$0")/common.sh"
run_bench stats_ceb stats_ceb
