#!/bin/bash
# benchmark/ceb_1a_continual (experimental), 5 cycles, its own plugins (feedback, litecard, nngp, hres)
# output: $DATA/ceb_1a_continual/, log: $DATA/ceb_1a_continual.log
set -o pipefail
source "$(dirname "$0")/common.sh"
mkdir -p "$DATA"
name=ceb_1a_continual
if [ -e "$DATA/$name" ]; then echo "$DATA/$name already exists: move it away first" >&2; exit 1; fi
cd "$REPO"
echo "== $name ($(date))" | tee "$DATA/$name.log"
python3 benchmark/ceb_1a_continual/run.py --dsn "$DSN" --out "$DATA/$name" "$@" 2>&1 | tee -a "$DATA/$name.log"
echo "== $name finished ($(date))" | tee -a "$DATA/$name.log"
