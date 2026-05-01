#!/usr/bin/env python3
"""Parse World Bank Pink Sheet for nickel monthly USD/MT."""
import openpyxl, csv, os, datetime as dt

XL = "/Users/wilsonw/mm-setup/auto_theo/archive/worldbank_cmo_monthly.xlsx"
OUT = "/Users/wilsonw/mm-setup/auto_theo/research/ev_basket/components"

wb = openpyxl.load_workbook(XL, read_only=True, data_only=True)
ws = wb['Monthly Prices']
rows = list(ws.iter_rows(values_only=True))
header = rows[4]

def parse_ym(s):
    # e.g. '2024M01' -> 2024-01-15 (mid-month convention)
    if not isinstance(s,str) or 'M' not in s: return None
    y,m = s.split('M')
    return dt.date(int(y), int(m), 15)

def grab_col(name, alt=None):
    for i,h in enumerate(header):
        if h == name:
            data = []
            for r in rows[6:]:
                d = parse_ym(r[0])
                v = r[i]
                if d and isinstance(v,(int,float)):
                    data.append((d,v))
            return data
    return None

cu = grab_col("Copper")
ni = grab_col("Nickel")
pt = grab_col("Platinum")
pb_pd_au = grab_col("Lead")  # check that Pd is missing

for name, data in [("nickel_wb",ni), ("copper_wb",cu), ("platinum_wb",pt)]:
    if not data:
        print(f"SKIP {name}: not found")
        continue
    # Filter: 2024-01 onwards
    data = [(d,v) for d,v in data if d >= dt.date(2024,1,1)]
    out = os.path.join(OUT, f"{name}.csv")
    with open(out,"w") as f:
        f.write("date,close\n")
        for d,v in data:
            f.write(f"{d.isoformat()},{v}\n")
    print(f"OK  {name}: {len(data)} rows -> {out}")

print("WB Pink Sheet does not contain cobalt or palladium.")
