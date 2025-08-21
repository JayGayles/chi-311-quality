import argparse, os, json, sys
from datetime import timezone
import pandas as pd
import requests

API_URL = "https://data.cityofchicago.org/resource/v6vf-nfxy.json"

# --------------------------------------------------------------------------------------
# Column candidates (handles schema drift)
# --------------------------------------------------------------------------------------
# Many public datasets evolve over time or differ between API/CSV. To be resilient,
# we map a *logical* field name (e.g., "sr_number") to a list of common *physical*
# column name variants. The checker will pick the first that exists in the dataframe.
CANDIDATES = {
    "sr_number": ["sr_number","service_request_number","srnumber","sr_no"],
    "created_date": ["created_date","creation_date","date_created","open_date","sr_created_date","requested_datetime"],
    "closed_date": ["closed_date","completion_date","date_closed","status_date","closed_datetime","closed_date_time"],
    "type": ["service_request_type","sr_type","type_of_service_request","type","sr_short_description"],
    "status": ["status","sr_status","current_status"],
    "legacy": ["legacy_record","is_legacy_record","legacy"],
    "address": ["street_address","address","request_address","location_address"],
    "lat": ["latitude","lat","location_latitude"],
    "lon": ["longitude","lon","location_longitude"],
}

# Logical fields that must exist / be largely populated
REQUIRED_FIELDS = ["sr_number","type","status","created_date"]

# Thresholds (tuned for typical 311 datasets; adjust as needed)
REQ_NULL_THRESHOLD = 0.005       # 0.5% missing in required fields -> WARN
COORD_NULL_THRESHOLD = 0.15      # 15% null/zero lat/lon -> WARN
INFO_ADDR_DOMINANCE = 0.40       # "Information Only" calls dominated by one address >= 40% -> INFO

def find_col(df, key):
    """
    Return the first matching physical column name for a given logical key
    according to CANDIDATES. If none found, return None.
    """
    for c in CANDIDATES.get(key, []):
        if c in df.columns:
            return c
    return None

def load_api(limit, app_token=None):
    """
    Load a sample from the Socrata API.

    Parameters
    ----------
    limit : int
        Number of rows to fetch via $limit.
    app_token : str or None
        Optional Socrata app token (recommended to limit throttling).

    Returns
    -------
    pandas.DataFrame
    """
    params = {"$limit": limit}
    headers = {"X-App-Token": app_token} if app_token else {}
    r = requests.get(API_URL, params=params, headers=headers, timeout=90)
    r.raise_for_status()
    return pd.DataFrame(r.json())

def load_csv(path):
    """
    Load a CSV with safe defaults (no chunked dtype guessing).

    Parameters
    ----------
    path : str

    Returns
    -------
    pandas.DataFrame
    """
    return pd.read_csv(path, low_memory=False)

def pct(n, d):
    """Safe percentage helper (returns 0.0 if denominator is zero)."""
    return (n/d) if d else 0.0

def main():
    """
    Run quality checks for Chicago 311 data (API or CSV source) and
    emit a Markdown report summarizing results plus an overall status.

    Exit codes
    ----------
    0 : PASS (no FAIL or WARN)
    1 : WARN or FAIL present (attention needed)
    """
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", choices=["api","csv"], required=True,
                    help="Where to load data from.")
    ap.add_argument("--limit", type=int, default=50000,
                    help="API row limit (ignored for CSV).")
    ap.add_argument("--path", type=str,
                    help="CSV path when --source=csv.")
    ap.add_argument("--out", default="notes/data_quality_checks.md",
                    help="Markdown output path for the check report.")
    ap.add_argument("--mark-done", action="store_true",
                    help="If set, write notes/.STEP2_DONE marker file.")
    args = ap.parse_args()

    # ------------------------------------------------------------------
    # Load data
    # ------------------------------------------------------------------
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

    # ------------------------------------------------------------------
    # Resolve real columns for all logical keys we care about
    # ------------------------------------------------------------------
    cols = {k: find_col(df, k) for k in ["sr_number","type","status","created_date","closed_date","lat","lon","legacy","address"]}

    # 1) Required field completeness
    #    - Missing column => FAIL
    #    - Present but > threshold missing => WARN
    for k in REQUIRED_FIELDS:
        c = cols[k]
        if not c:
            checks.append({"name": f"Required field present: {k}", "status":"FAIL", "detail":"Column missing"})
            continue
        miss = int(df[c].isna().sum())
        rate = pct(miss, n)
        status = "PASS" if rate <= REQ_NULL_THRESHOLD else "WARN"
        checks.append({"name": f"Completeness: {k}", "status":status, "detail":f"missing={miss} ({rate:.2%})"})

    # 2) Duplicate SR numbers
    #    - Duplicate identifiers indicate merging/ingest issues.
    if cols["sr_number"]:
        dups = int(df.duplicated(cols["sr_number"]).sum())
        checks.append({"name":"Duplicate SR numbers", "status":"FAIL" if dups>0 else "PASS", "detail":f"duplicates={dups}"})
    else:
        checks.append({"name":"Duplicate SR numbers", "status":"FAIL", "detail":"sr_number column missing"})

    # 3) Temporal validity
    #    - created_date in the future
    #    - closed_date in the future
    #    - closed_date earlier than created_date
    fut_created = fut_closed = neg_duration = "N/A"
    if cols["created_date"]:
        df["_created_dt"] = pd.to_datetime(df[cols["created_date"]], errors="coerce", utc=True)
        fut_created = int((df["_created_dt"] > pd.Timestamp.now(timezone.utc)).sum())
    if cols["closed_date"]:
        df["_closed_dt"] = pd.to_datetime(df[cols["closed_date"]], errors="coerce", utc=True)
        # BUGFIX: the original code checked for "._created_dt" (leading dot). Corrected to "_created_dt".
        fut_closed = int((df["_closed_dt"] > pd.Timestamp.now(timezone.utc)).sum())
        if "_created_dt" in df.columns:
            neg_duration = int(((~df["_closed_dt"].isna()) & (df["_closed_dt"] < df["_created_dt"])).sum())

    checks.append({"name":"Future created_date", "status":"FAIL" if fut_created not in ("N/A",0) else "PASS", "detail":f"count={fut_created}"})
    checks.append({"name":"Future closed_date", "status":"FAIL" if fut_closed not in ("N/A",0) else "PASS", "detail":f"count={fut_closed}"})
    checks.append({"name":"Closed before created", "status":"FAIL" if neg_duration not in ("N/A",0) else "PASS", "detail":f"count={neg_duration}"})

    # 4) Coordinate completeness
    #    - Nulls or zeros in lat/lon are tallied. WARN if over threshold.
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
    #    - Some datasets flag historical/legacy rows; we surface counts as INFO/WARN.
    legacy_counts = {}
    if cols["legacy"]:
        legacy_counts = df[cols["legacy"]].value_counts(dropna=False).to_dict()
        # Handle boolean True and string "true"
        legacy_true = int(legacy_counts.get(True, 0) or legacy_counts.get("true", 0) or 0)
        checks.append({"name":"Legacy records present", "status":"WARN" if legacy_true>0 else "PASS", "detail":json.dumps(legacy_counts)})
    else:
        checks.append({"name":"Legacy column present", "status":"WARN", "detail":"No legacy column"})

    # 6) “INFORMATION ONLY” call-center proxy heuristic
    #    - Detects if "information only" SRs are dominated by a single address,
    #      which often indicates call-center address used instead of true location.
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

    # ------------------------------------------------------------------
    # Overall result aggregation
    # ------------------------------------------------------------------
    overall = "PASS"
    if any(c["status"]=="FAIL" for c in checks):
        overall = "FAIL"
    elif any(c["status"]=="WARN" for c in checks):
        overall = "WARN"

    # ------------------------------------------------------------------
    # Write Markdown report
    # ------------------------------------------------------------------
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        f.write("# Chicago 311 – Step 2 Quality Checks\n\n")
        f.write(f"- Source: **{src_label}**\n")
        f.write(f"- Rows: **{n:,}**\n")
        f.write(f"- Overall: **{overall}**\n\n")
        f.write("| Check | Status | Details |\n|---|---|---|\n")
        for c in checks:
            f.write(f"| {c['name']} | {c['status']} | {c['detail']} |\n")

    # Optional "step done" marker for orchestration
    if args.mark_done:
        os.makedirs("notes", exist_ok=True)
        with open("notes/.STEP2_DONE","w") as mf:
            mf.write("done\n")

    print(f"Wrote {args.out}")
    print("STEP2_READY_TO_PROCEED" if overall == "PASS" else "STEP2_ATTENTION_NEEDED")

    # Exit code reflects overall result (0 pass, 1 warn/fail)
    sys.exit(0 if overall=="PASS" else 1)

if __name__ == "__main__":
    main()