#!/bin/bash
# Fetch candlesticks for boundary markets, slowly.
ARCHIVE=/Users/wilsonw/mm-setup/auto_theo/archive/kalshi/candlesticks
UA="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
mkdir -p "$ARCHIVE"

python3 -c "
import json
with open('/Users/wilsonw/mm-setup/auto_theo/research/ev_basket/kalshi_history.json') as f: h=json.load(f)
import datetime as dt
for ev in h:
    if ev['midpoint'] is None: continue
    s=ev['strikes']; r=ev['resolutions']
    yes_t=sorted([(s[t],t) for t,res in r.items() if res=='yes' and s[t] is not None])
    no_t=sorted([(s[t],t) for t,res in r.items() if res=='no' and s[t] is not None])
    if yes_t:
        ct=dt.datetime.fromisoformat(ev['close_time'].replace('Z','+00:00'))
        et=int(ct.timestamp()); st=et-7*86400
        print(yes_t[-1][1], st, et)
    if no_t:
        ct=dt.datetime.fromisoformat(ev['close_time'].replace('Z','+00:00'))
        et=int(ct.timestamp()); st=et-7*86400
        print(no_t[0][1], st, et)
" > /tmp/candle_targets.txt

while IFS=' ' read -r tk start_ts end_ts; do
    out_dir=$ARCHIVE/$tk
    mkdir -p "$out_dir"
    out_file=$out_dir/close_window.json
    if [ -f "$out_file" ]; then
        echo "skip $tk (cached)"
        continue
    fi
    sleep 45
    url="https://api.elections.kalshi.com/trade-api/v2/series/KXTRUEV/markets/$tk/candlesticks?start_ts=$start_ts&end_ts=$end_ts&period_interval=60"
    curl -sA "$UA" -H "Accept: application/json" "$url" -o /tmp/__candles_$tk.json
    n=$(python3 -c "import json; d=json.load(open('/tmp/__candles_$tk.json')); print(len(d.get('candlesticks',[])))" 2>/dev/null)
    echo "$tk -> $n candles"
    if [ "$n" -ge 1 ]; then
        python3 -c "
import json, datetime as dt
with open('/tmp/__candles_$tk.json') as f: d=json.load(f)
out = {
    'fetched_at': dt.datetime.now(dt.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
    'source_url': '$url',
    'raw': d,
}
with open('$out_file','w') as f:
    json.dump(out, f)
print('saved')
"
    fi
done < /tmp/candle_targets.txt
echo "candle fetch done"
