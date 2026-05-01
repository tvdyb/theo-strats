#!/bin/bash
# Fetch one event with a single curl, retry up to 5x with 60s sleeps.
ET=$1
ARCHIVE=/Users/wilsonw/mm-setup/auto_theo/archive/kalshi/markets
UA="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
for i in 1 2 3 4 5 6 7 8; do
    out=/tmp/__retry_${ET}_$i.json
    sleep 60
    curl -sA "$UA" -H "Accept: application/json" "https://api.elections.kalshi.com/trade-api/v2/markets?event_ticker=$ET&limit=200" -o $out
    n=$(python3 -c "import json; d=json.load(open('$out')); print(len(d.get('markets',[])))" 2>/dev/null)
    echo "$ET attempt $i -> $n markets"
    if [ "$n" -ge 10 ]; then
        python3 -c "
import json, datetime as dt, os
with open('$out') as f: d=json.load(f)
for m in d.get('markets',[]):
    p = os.path.join('$ARCHIVE', m['ticker']+'.json')
    with open(p,'w') as f:
        json.dump({
            'fetched_at': dt.datetime.now(dt.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
            'source_url': 'https://api.elections.kalshi.com/trade-api/v2/markets?event_ticker=$ET',
            'raw': m,
        }, f, indent=2)
print('saved $ET')
"
        exit 0
    fi
done
echo "GIVE UP $ET"
exit 1
