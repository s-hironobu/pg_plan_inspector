#!/bin/bash
# benchmark/job_light, the plugins of common.sh
# output: $DATA/job_light/ (report.txt, per_query.tsv, ds/), log: $DATA/job_light.log
set -o pipefail
source "$(dirname "$0")/common.sh"
run_bench job_light job_light
