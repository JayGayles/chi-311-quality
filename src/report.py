import argparse, os, json
from datetime import timezone
import pandas as pd
import requests

# Socrata API endpoint for Chicago 311 Service Requests (JSON)
API_URL = "https://data.cityofchicago.org/resource/v6vf-nfxy.json"

# --------------------------------------------------------------------------------------
# Candidate column names per logical field (defensive against schema drift)
# The checker will pick the first matching physical column present in the DataFrame.
# --------------------------------------------------------------------------------------
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
    "x": ["x_coordinate","xcoord","x_coordinate_state_plane"],
    "y": ["y_coordinate","ycoord","y_coordinate_state_plane"],
}

def find_col(df, key):
    """
    Return the first concrete column name that maps to the given logical key.

    Parameters
    ----------
    df : pandas.DataFrame
    key : str
        One of the keys in CANDIDATES (e.g., 'sr_number', 'created_date').

    Returns
    -------
    str or None
        The first matching column in df.columns, or None if none found.
    """
    for c in CANDIDATES.get(key, []):
        if c in df.columns:
            return c
    return None

def load_api(limit, app_token=None):
    """
    Fetch a sample from the Socrata API.

    Parameters
    ----------
    limit : int
        Number of rows to request via SoQL $limit (intended for exploration).
    app_token : str or None
        Optional Socrata app token to reduce throttling (X-App-Token header).

    Returns
    -------
    pandas.DataFrame
        DataFrame created from the JSON response.

    Raises
    ------
    requests.HTTPError
        For non-2xx responses; surfaced via raise_for_status().
    """
    params = {"$limit": limit}
    headers = {"X-App-Token": app_token} if app_token else {}
    r = requests.get(API_URL, params=params, headers=headers, timeout=60)
    r.raise_for_status()
    return pd.DataFrame(r.json())

def load_csv(path):
    """
    Load a CSV with stable dtype inference settings.

    Parameters
    ----------
    path : str
        Path to a CSV file.

    Returns
    -------
    pandas.DataFrame
    """
    return pd.read_csv(path, low_memory=False)

def main():
    """
    Generate a lightweight data-quality findings report for Chicago 311 data.

    Reads from API (sampled) or CSV, computes quick signals (missingness,
    duplicates, basic temporal/spatial anomalies, legacy flags, and an
    'INFORMATION ONLY' heuristic), and writes a Markdown report.

    The report is intended as a first pass before deeper normalization/ETL.
    """
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", choices=["api","csv"], required=True,
                    help="Data source to load from.")
    ap.add_argument("--limit", type=int, default=1000,
                    help="Row limit for API mode (ignored for CSV).")
    ap.add_argument("--path", type=str,
                    help="CSV path for --source=csv.")
    ap.add_argument("--out", default="notes/data_quality_findings.md",
                    help="Output Markdown file for findings.")
    ap.add_argument("--mark-done", action="store_true",
                    help="If set, write notes/.STEP1_DONE marker.")
    args = ap.parse_args()

    # ------------------------------------------------------------------
    # Load data
    # ------------------------------------------------------------------
    if args.source == "api":
        df = load_api(args.limit, os.getenv("SOCRATA_APP_TOKEN"))
        source_label = f"API (limit={args.limit})"
    else:
        if not args.path or not os.path.exists(args.path):
            raise SystemExit("CSV path missing. Use --path data/chi311.csv")
        df = load_csv(args.path)
        source_label = f"CSV ({args.path})"

    # ------------------------------------------------------------------
    # Basic stats (missingness by column, descending)
    # ------------------------------------------------------------------
    na = df.isna().sum().sort_values(ascending=False)

    # Uniqueness check on service request numbers
    sr_col = find_col(df,"sr_number")
    dup_count = int(df.duplicated(sr_col).sum()) if sr_col else None

    # Temporal anomalies
    created_col = find_col(df,"created_date")
    closed_col  = find_col(df,"closed_date")

    fut_created = fut_closed = neg_duration = None
    if created_col and df[created_col].notna().any():
        # Coerce to UTC timestamps; invalid parses become NaT
        df["_created_dt"] = pd.to_datetime(df[created_col], errors="coerce", utc=True)
        fut_created = int((df["_created_dt"] > pd.Timestamp.now(timezone.utc)).sum())
    if closed_col and df[closed_col].notna().any():
        df["_closed_dt"] = pd.to_datetime(df[closed_col], errors="coerce", utc=True)
        fut_closed = int((df["_closed_dt"] > pd.Timestamp.now(timezone.utc)).sum())
        # Only compute negative durations if we have both sides parsed
        if "_created_dt" in df.columns:
            neg_duration = int((df["_closed_dt"] < df["_created_dt"]).sum())

    # Spatial anomalies (prefer lat/lon; fall back to projected x/y if needed)
    lat_col, lon_col = find_col(df,"lat"), find_col(df,"lon")
    x_col, y_col = find_col(df,"x"), find_col(df,"y")
    coord_anom = None
    if lat_col and lon_col:
        lat = pd.to_numeric(df[lat_col], errors="coerce")
        lon = pd.to_numeric(df[lon_col], errors="coerce")
        coord_anom = int((lat.isna() | lon.isna() | (lat==0) | (lon==0)).sum())
    elif x_col and y_col:
        x = pd.to_numeric(df[x_col], errors="coerce")
        y = pd.to_numeric(df[y_col], errors="coerce")
        coord_anom = int((x.isna() | y.isna() | (x==0) | (y==0)).sum())

    # Legacy-record flag distribution (if present)
    legacy_col = find_col(df,"legacy")
    legacy_counts = df[legacy_col].value_counts(dropna=False).to_dict() if legacy_col else {}

    # “INFORMATION ONLY” calls: address dominance and coarse geoclusters
    type_col, addr_col = find_col(df,"type"), find_col(df,"address")
    info_count, top_info_addr, top_info_clusters = 0, {}, {}
    if type_col:
        # Heuristic: entries where SR type contains both "information" and "only"
        info_mask = df[type_col].astype(str).str.contains("information", case=False, na=False) & \
                    df[type_col].astype(str).str.contains("only", case=False, na=False)
        info_df = df[info_mask].copy()
        info_count = int(len(info_df))
        # Address dominance (top addresses by count)
        if addr_col and info_count:
            top_info_addr = info_df[addr_col].value_counts(dropna=True).head(5).to_dict()
        # Rough lat/lon clustering by rounding (if coordinates present)
        if lat_col and lon_col and info_count:
            info_df["_lat"] = pd.to_numeric(info_df[lat_col], errors="coerce").round(3)
            info_df["_lon"] = pd.to_numeric(info_df[lon_col], errors="coerce").round(3)
            top_info_clusters = (
                info_df.groupby(["_lat","_lon"]).size()
                .sort_values(ascending=False)
                .head(5).to_dict()
            )

    # ------------------------------------------------------------------
    # Write Markdown report
    # ------------------------------------------------------------------
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        f.write("# Chicago 311 Service Requests – Data Quality Findings\n\n")
        f.write("## 1. Schema & Data Types\n")
        f.write(f"- Dataset source: **{source_label}**\n")
        f.write(f"- Number of rows pulled: **{len(df):,}**\n")
        f.write(f"- Columns present ({len(df.columns)}): `{', '.join(sorted(df.columns))}`\n\n")

        f.write("## 2. Missing Value Summary (top 20)\n\n")
        f.write("| Column | Missing | % Missing |\n|---|---:|---:|\n")
        for col, cnt in na.head(20).items():
            pct = (cnt/len(df))*100 if len(df) else 0.0
            f.write(f"| {col} | {cnt} | {pct:.1f}% |\n")
        f.write("\n")

        f.write("## 3. Uniqueness Check\n")
        f.write(f"- SR number column used: `{sr_col or 'N/A'}`\n")
        f.write(f"- Duplicate count: **{dup_count if dup_count is not None else 'N/A'}**\n\n")

        f.write("## 4. Temporal Anomalies\n")
        f.write(f"- Future created_date: **{fut_created if fut_created is not None else 'N/A'}**\n")
        f.write(f"- Future closed_date: **{fut_closed if fut_closed is not None else 'N/A'}**\n")
        f.write(f"- Closed before created: **{neg_duration if neg_duration is not None else 'N/A'}**\n\n")

        f.write("## 5. Spatial Anomalies\n")
        f.write(f"- Lat/Lon or X/Y anomalies (null/zero): **{coord_anom if coord_anom is not None else 'N/A'}**\n\n")

        f.write("## 6. LEGACY_RECORD Usage\n")
        f.write(f"- Column present: **{'Yes' if legacy_col else 'No'}**\n")
        if legacy_counts:
            f.write(f"- Value counts: `{json.dumps(legacy_counts)}`\n\n")
        else:
            f.write("\n")

        f.write("## 7. “INFORMATION ONLY” Calls\n")
        f.write(f"- Count: **{info_count}**\n")
        if top_info_addr:
            f.write("- Top addresses:\n")
            for a,c in top_info_addr.items():
                f.write(f"  - {a}: {c}\n")
        if top_info_clusters:
            f.write("- Top lat/lon clusters (rounded):\n")
            for (lat,lon),c in top_info_clusters.items():
                f.write(f"  - ({lat}, {lon}): {c}\n")
        f.write("\n")

        f.write("## 8. Initial Quality Check Priorities\n")
        f.write("- Columns to validate in detail: _fill after review_\n")
        f.write("- Columns to standardize: _fill after review_\n")
        f.write("- Potential filters for future analysis: _fill after review_\n")

    # Optional step marker for orchestration/tests
    if args.mark_done:
        open("notes/.STEP1_DONE", "w").write("done\n")

    print(f"Wrote {args.out}")
    if args.mark_done:
        print("READY_TO_PROCEED")  # simple pipeline flag for downstream steps

if __name__ == "__main__":
    main()