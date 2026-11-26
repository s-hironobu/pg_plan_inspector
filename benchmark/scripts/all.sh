#!/bin/bash
# stats_ceb, job_light and ceb_1a, one after another (a failure does not stop the next one)
here=$(dirname "$0")
for b in stats_ceb job_light ceb_1a; do
    bash "$here/$b.sh" || echo "== $b failed" >&2
done
