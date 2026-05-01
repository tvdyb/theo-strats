#!/bin/bash
# Fetch candlesticks for boundary markets, slowly. Args: list of tickers
ARCHIVE=/Users/wilsonw/mm-setup/auto_theo/archive/kalshi/candlesticks
UA="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
mkdir -p "$ARCHIVE"

# Compute time range: 7 days ending at the event close
for tk in "$@"; do
    out_dir=$ARCHIVE/$tk
    out_file=$out_dir/close_window.json
    mkdir -p "$out_dir"
    if [ -f "$out_file" ]; then
        echo "skip $tk (cached)"
        continue
    fi
    # Parse event close from kalshi_history
    info=$(python3 -c "
import json, datetime as dt
with open('/Users/wilsonw/mm-setup/auto_theo/research/ev_basket/kalshi_history.json') as f: h=json.load(f)
et = '$tk'.rsplit('-T',1)[0]
ev = next((e for e in h if e['event_ticker']==et), None)
if not ev:
    print('NA NA')
else:
    ct=dt.datetime.fromisoformat(ev['close_time'].replace('Z','+00:00'))
    e=int(ct.timestamp()); s=e-7*86400
    print(s, e)
")
    start_ts=$(echo $info | awk '{print $1}'); end_ts=$(echo $info | awk '{print $2}')
    if [ "$start_ts" = "NA" ]; then echo "skip $tk (no event metadata)"; continue; fi
    sleep 60
    url="https://api.elections.kalshi.com/trade-api/v2/series/KXTRUEV/markets/$tk/candlesticks?start_ts=$start_ts&end_ts=$end_ts&period_interval=60"
    curl -sA "$UA" -H "Accept: application/json" "$url" -o /tmp/__candles_$tk.json
    n=$(python3 -c "import json; d=json.load(open('/tmp/__candles_$tk.json')); print(len(d.get('candlesticks',[])))" 2>/dev/null)
    err=$(python3 -c "import json; d=json.load(open('/tmp/__candles_$tk.json')); print(d.get('error',{}).get('code') or '-')" 2>/dev/null)
    echo "$tk -> $n candles (err=$err)"
    if [ "$n" -ge 1 ]; then
        python3 -c "
import json, datetime as dt
with open('/tmp/__candles_$tk.json') as f: d=json.load(f)
out = {
    'fetched_at': dt.datetime.now(dt.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
    'source_url': '$url',
    'raw': d,
}
with open('$out_file','w') as f: json.dump(out, f)
print('saved')
"
    fi
done
echo "candle fetch done"
