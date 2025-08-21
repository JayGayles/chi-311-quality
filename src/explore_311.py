import argparse, os, json, sys
from datetime import timezone
import pandas as pd
import requests
from dotenv import load_dotenv

from common import find_col, pct  # NEW: shared helpers

load_dotenv()

API_URL = "https://data.cityofchicago.org/resource/v6vf-nfxy.json"

# Required logical fields
REQUIRED_FIELDS = ["sr_number","type","status","created_date"]

# Thresholds (tune as needed)
REQ_NULL_THRESHOLD = 0.005       # 0.5% missing in required fields -> WARN
COORD_NULL_THRESHOLD = 0.15      # 15% null/zero lat/lon -> WARN
INFO_ADDR_DOMINANCE = 0.40       # "Information Only" dominance >= 40% -> INFO

def load_api(limit, app_token=None):
    params = {"$limit": limit}
    headers = {"X-App-Token": app_token} if app_token else {}
    r = requests.get(API_URL, params=params, headers=headers, timeout=90)
    r.raise_for_status()
    return pd.DataFrame(r.json())

def load_csv(path):
    return pd.read_csv(path, low_memory=False)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", choices=["api","csv"], required=True, help="Where to load data from.")
    ap.add_argument("--limit", type=int, default=50000, help="API row limit (ignored for CSV).")
    ap.add_argument("--path", type=str, help="CSV path when --source=csv.")
    ap.add_argument("--out", default="notes/data_quality_checks.md", help="Markdown output path for Step-2 report.")
    ap.add_argument("--mark-done", action="store_true", help="Write notes/.STEP2_DONE if set.")
    args = ap.parse_args()

    # ---- Load ----
    if args.source == "api":
        df = load_api(args.limit, os.getenv("SOCRATA_APP_TOKEN"))
        src_label = f"API (limit={args.limit})"
    else:
        if not args.path or not os.path.exists(args.path):
            sys.exit("CSV path missing. Use --path data/chi311.csv")
        df = load_csv(args.path)
        src_label = f"CSV ({args.path})"

    n = len(df)
    checks = []

    # Resolve concrete columns (once)
    cols = {k: find_col(df, k) for k in ["sr_number","type","status","created_date","closed_date","lat","lon","legacy","address"]}

    # 1) Required field completeness
    for k in REQUIRED_FIELDS:
        c = cols[k]
        if not c:
            checks.append({"name": f"Required field present: {k}", "status":"FAIL", "detail":"Column missing"})
            continue
        miss = int(df[c].isna().sum()); rate = pct(miss, n)
        status = "PASS" if rate <= REQ_NULL_THRESHOLD else "WARN"
        checks.append({"name": f"Completeness: {k}", "status":status, "detail":f"missing={miss} ({rate:.2%})"})

    # 2) Duplicate SR numbers
    if cols["sr_number"]:
        dups = int(df.duplicated(cols["sr_number"]).sum())
        checks.append({"name":"Duplicate SR numbers", "status":"FAIL" if dups>0 else "PASS", "detail":f"duplicates={dups}"})
    else:
        checks.append({"name":"Duplicate SR numbers", "status":"FAIL", "detail":"sr_number column missing"})

    # 3) Temporal validity
    fut_created = fut_closed = neg_duration = "N/A"
    if cols["created_date"]:
        df["_created_dt"] = pd.to_datetime(df[cols["created_date"]], errors="coerce", utc=True)
        fut_created = int((df["_created_dt"] > pd.Timestamp.now(timezone.utc)).sum())
    if cols["closed_date"]:
        df["_closed_dt"] = pd.to_datetime(df[cols["closed_date"]], errors="coerce", utc=True)
        fut_closed = int((df["_closed_dt"] > pd.Timestamp.now(timezone.utc)).sum())
        if "_created_dt" in df.columns:
            neg_duration = int(((~df["_closed_dt"].isna()) & (df["_closed_dt"] < df["_created_dt"])).sum())

    checks.append({"name":"Future created_date", "status":"FAIL" if fut_created not in ("N/A",0) else "PASS", "detail":f"count={fut_created}"})
    checks.append({"name":"Future closed_date", "status":"FAIL" if fut_closed not in ("N/A",0) else "PASS", "detail":f"count={fut_closed}"})
    checks.append({"name":"Closed before created", "status":"FAIL" if neg_duration not in ("N/A",0) else "PASS", "detail":f"count={neg_duration}"})

    # 4) Coordinate completeness
    coord_anom = "N/A"
    if cols["lat"] and cols["lon"]:
        lat = pd.to_numeric(df[cols["lat"]], errors="coerce")
        lon = pd.to_numeric(df[cols["lon"]], errors="coerce")
        coord_anom = int((lat.isna() | lon.isna() | (lat==0) | (lon==0)).sum())
        rate = pct(coord_anom, n)
        status = "PASS" if rate <= COORD_NULL_THRESHOLD else "WARN"
        checks.append({"name":"Coordinate anomalies (null/zero)", "status":status, "detail":f"count={coord_anom} ({rate:.2%})"})
    else:
        checks.append({"name":"Coordinate anomalies (null/zero)", "status":"WARN", "detail":"No lat/lon columns found"})

    # 5) Legacy records
    legacy_counts = {}
    if cols["legacy"]:
        legacy_counts = df[cols["legacy"]].value_counts(dropna=False).to_dict()
        legacy_true = int(legacy_counts.get(True, 0) or legacy_counts.get("true", 0) or 0)
        checks.append({"name":"Legacy records present", "status":"WARN" if legacy_true>0 else "PASS", "detail":json.dumps(legacy_counts)})
    else:
        checks.append({"name":"Legacy column present", "status":"WARN", "detail":"No legacy column"})

    # 6) “INFORMATION ONLY” heuristic
    info_note = "N/A"
    if cols["type"]:
        info_mask = df[cols["type"]].astype(str).str.contains("information", case=False, na=False) & \
                    df[cols["type"]].astype(str).str.contains("only", case=False, na=False)
        info_df = df[info_mask].copy()
        dom = 0.0
        if len(info_df) and cols["address"]:
            top = info_df[cols["address"]].value_counts(dropna=True).head(1)
            if len(top):
                dom = top.iloc[0] / len(info_df)
                info_note = f"info_calls={len(info_df)}, top_addr='{top.index[0]}', dominance={dom:.2%}"
        status = "INFO" if dom >= INFO_ADDR_DOMINANCE else "PASS"
        checks.append({"name":"Information-only address dominance", "status":status, "detail":info_note})
    else:
        checks.append({"name":"Information-only address dominance", "status":"PASS", "detail":"No type column; skipped"})

    # Overall status
    overall = "PASS"
    if any(c["status"]=="FAIL" for c in checks): overall = "FAIL"
    elif any(c["status"]=="WARN" for c in checks): overall = "WARN"

    # Write report
    from common import ensure_dir  # local import to avoid unused if not writing
    ensure_dir(args.out)
    with open(args.out, "w", encoding="utf-8") as f:
        f.write("# Chicago 311 – Step 2 Quality Checks\n\n")
        f.write(f"- Source: **{src_label}**\n")
        f.write(f"- Rows: **{n:,}**\n")
        f.write(f"- Overall: **{overall}**\n\n")
        f.write("| Check | Status | Details |\n|---|---|---|\n")
        for c in checks:
            f.write(f"| {c['name']} | {c['status']} | {c['detail']} |\n")

    # Marker
    if args.mark_done:
        os.makedirs("notes", exist_ok=True)
        with open("notes/.STEP2_DONE","w") as mf:
            mf.write("done\n")

    print(f"Wrote {args.out}")
    print("STEP2_READY_TO_PROCEED" if overall == "PASS" else "STEP2_ATTENTION_NEEDED")
    sys.exit(0 if overall=="PASS" else 1)

if __name__ == "__main__":
    main()