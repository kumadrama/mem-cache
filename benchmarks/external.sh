#!/usr/bin/env bash
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
out=$(cd "$1" && pwd)
mkdir -p "$out/caller"
cp "$here/testdata/caller/main.go" "$out/caller/main.go"
cd "$out/caller"
export GOWORK=off GOTOOLCHAIN=local GOMAXPROCS=8
if ! test -f go.mod; then go mod init local.test/mem-cache-caller; fi
go mod edit -require=github.com/kumadrama/mem-cache@v0.0.0 "-replace=github.com/kumadrama/mem-cache=$out/source"
cp "$out/source/go.sum" go.sum
go build -mod=mod -o "$out/caller.bin" .
"$out/caller.bin" logs > "$out/dlog.raw" 2>&1
python3 - "$out/dlog.raw" <<'PY' > "$out/error-burst.log"
import collections, json, sys
traces=collections.Counter()
found=leaks=bad=0
summary=None
for line in open(sys.argv[1]):
    if line.startswith('ROW '):
        summary=json.loads(line[4:])
        continue
    try: event=json.loads(line)
    except ValueError: continue
    if event.get('event')!='mem_cache.load_failed': continue
    found+=1
    leaks+=int('synthetic-private-key-' in line)
    traces[event.get('trace_id')]+=1
    bad+=int(any(event.get(k)!=v for k,v in {'span_id':'synthetic-span','operation':'get','cache':'log-pressure','policy':'lru','error':'synthetic backend unavailable','key_count':1}.items()))
if summary is None or found!=3200 or bad or leaks or sorted(traces.values())!=[50]*64:
    raise SystemExit(f"logs={found} traces={len(traces)} bad={bad} leaks={leaks}")
print('ROW',json.dumps(dict(summary,Logs=found,TraceIDs=len(traces),BadFields=bad,KeyLeaks=leaks)))
PY
"$out/caller.bin" > "$out/paced.log" 2>&1
"$out/caller.bin" steady > "$out/steady.log" 2>&1
