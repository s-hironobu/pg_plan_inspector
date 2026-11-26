# Shared settings of the benchmark scripts (sourced by each of them).
# Override any of them from the environment, e.g. DATA=/somewhere bash bench-all.sh
REPO=${REPO:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}
DATA=${DATA:-$REPO/benchmark/data}
DSN=${DSN:-"dbname=testdb"}
PLUGINS=${PLUGINS:-"feedback,litecard,nngp,grasp,matcomp,hres"}
COMMON="--temp-limit 3GB --plugins $PLUGINS"

run_bench() {    # run_bench BENCHMARK NAME [extra options...]
    local bench=$1 name=$2; shift 2
    mkdir -p "$DATA"
    if [ -e "$DATA/$name" ]; then
        echo "$DATA/$name already exists: move it away first" >&2
        return 1
    fi
    cd "$REPO"
    echo "== $name: python3 benchmark/$bench/run.py $COMMON $*  ($(date))" | tee "$DATA/$name.log"
    python3 benchmark/$bench/run.py --dsn "$DSN" --out "$DATA/$name" $COMMON "$@" 2>&1 | tee -a "$DATA/$name.log"
    echo "== $name finished ($(date))" | tee -a "$DATA/$name.log"
}
