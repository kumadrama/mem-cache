#!/usr/bin/env bash
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
source_dir=$(cd "$here/.." && pwd)
commit=${REV:-HEAD}
original=git.gametaptap.com/neutron/gotap/caches
version=v0.0.0-20230220083620-ebf728076160
out=$(mktemp -d)
echo "Benchmark output: $out"
mkdir -p "$out/source"
out=$(cd "$out" && pwd)
git -C "$source_dir" archive "$commit" | tar -x -C "$out/source"
cp "$here/testdata/cache/pressure_test.go" "$out/source/pressure_test.go"
cd "$out/source"
export GOWORK=off GOTOOLCHAIN=local
export GOMAXPROCS=8
if [[ "${WITH_GOTAP:-0}" == 1 ]]; then
    old="$(go env GOMODCACHE)/$original@$version"
    if ! test -d "$old"; then
        go mod download "$original@$version"
    fi
    go mod edit "-require=$original@$version" "-replace=$original=$old"
    cp "$here/testdata/cache/original_test.go" original_test.go
fi
go test -c -o "$out/pressure.test" .
go test -race -run '^TestPressure(Fanout|Cancel)$' -count=3 -timeout=3m -v . > "$out/fanout.log" 2>&1
"$out/pressure.test" -test.v -test.run '^TestPressure(Env|Matrix)$' -test.timeout=12m > "$out/matrix.log" 2>&1
"$out/pressure.test" -test.v -test.run '^TestPressureSoak$' -test.timeout=2m > "$out/soak.log" 2>&1
"$out/pressure.test" -test.v -test.run '^TestPressureMemory$' > "$out/heap.log" 2>&1
"$out/pressure.test" -test.v -test.run '^TestPressureProfile$' -test.mutexprofile="$out/mutex.pprof" -test.mutexprofilefraction=20 -test.cpuprofile="$out/cpu.pprof" > "$out/profile.log" 2>&1
go tool pprof -top -cum -nodecount=25 "$out/pressure.test" "$out/mutex.pprof" > "$out/mutex.txt"
go tool pprof -top -nodecount=25 "$out/pressure.test" "$out/cpu.pprof" > "$out/cpu.txt"
go test -race -run "^TestPressureState$" -count=3 -timeout=5m -v . > "$out/state.log" 2>&1
"$here/external.sh" "$out"
echo "$out"
