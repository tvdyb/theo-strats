#!/bin/bash
set -e
ARCHIVE=/Users/wilsonw/mm-setup/auto_theo/archive/kalshi/markets
UA="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"

for et in KXTRUEV-26APR15 KXTRUEV-26APR16 KXTRUEV-26APR17 KXTRUEV-26APR18; do
    # Skip if already have this event archived
    cnt=$(ls $ARCHIVE/${et}-T*.json 2>/dev/null | wc -l)
    if [ "$cnt" -ge 10 ]; then
        echo "skip $et (already $cnt markets)"
        continue
    fi
    out=/tmp/__${et}.json
    echo "fetch $et"
    curl -sA "$UA" -H "Accept: application/json" "https://api.elections.kalshi.com/trade-api/v2/markets?event_ticker=$et&limit=200" -o $out
    n=$(python3 -c "import json; d=json.load(open('$out')); print(len(d.get('markets',[])))")
    echo "  -> $n markets"
    python3 -c "
import json, datetime as dt, os
with open('$out') as f: d=json.load(f)
for m in d.get('markets',[]):
    p = os.path.join('$ARCHIVE', m['ticker']+'.json')
    with open(p,'w') as f:
        json.dump({
            'fetched_at': dt.datetime.now(dt.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
            'source_url': 'https://api.elections.kalshi.com/trade-api/v2/markets?event_ticker=$et',
            'raw': m,
        }, f, indent=2)
print('saved')
"
    sleep 30
done
echo "done"
