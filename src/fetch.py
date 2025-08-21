# src/fetch.py
import os, argparse, sys
from datetime import datetime, timedelta, timezone
import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

API_URL = "https://data.cityofchicago.org/resource/v6vf-nfxy.json"

# Prefer real datetime fields first; include text-y ones too
DATE_FIELD_CANDIDATES = [
    "requested_datetime",
    "last_modified_date",
    "closed_date",
    "created_date",
    "creation_date",
    "open_date",
    "sr_created_date",
    "date_created",
]

def _sample_columns(headers: dict) -> set:
    """Fetch 1 row to learn which columns exist."""
    r = requests.get(API_URL, params={"$limit": 1}, headers=headers, timeout=60)
    r.raise_for_status()
    rows = r.json() or [{}]
    return set(rows[0].keys())

def _try_page(where_expr: str, order_col: str, chunk: int, headers: dict) -> list[dict]:
    """Attempt a single page fetch; raise with helpful context on HTTP errors."""
    params = {"$where": where_expr, "$limit": chunk, "$offset": 0, "$order": order_col}
    r = requests.get(API_URL, headers=headers, params=params, timeout=120)
    try:
        r.raise_for_status()
    except requests.HTTPError as e:
        raise SystemExit(
            f"HTTP {r.status_code} from Socrata.\n"
            f"URL: {r.url}\n"
            f"Message: {r.text[:800]}"
        ) from e
    return r.json()

def _page_iter_no_where(order_col: str | None, chunk: int, headers: dict, start_offset: int = 0):
    """Yield rows across pages without a $where clause (fallback mode)."""
    offset = start_offset
    while True:
        params = {"$limit": chunk, "$offset": offset}
        if order_col:
            params["$order"] = order_col
        r = requests.get(API_URL, headers=headers, params=params, timeout=120)
        r.raise_for_status()
        rows = r.json()
        if not rows:
            break
        yield rows
        offset += len(rows)
        if len(rows) < chunk:
            break

def fetch_api(days_back=90, chunk=50000, app_token=None, created_field: str | None = None, max_pages: int = 30):
    """
    Pull recent rows from the Socrata API in pages, robust to text vs datetime columns.
    Strategy:
      1) Try date filtering server-side:
            a) date_col >= '...'
            b) date_col::floating_timestamp >= '...'
         If both fail for all candidates, fall back to:
      2) Blind pagination (no $where), order if possible, then filter locally by parsed dates.
    """
    headers = {"X-App-Token": app_token} if app_token else {}

    # discover columns
    cols = _sample_columns(headers)

    # choose a date column list to consider
    if created_field:
        if created_field not in cols:
            sys.exit(f"--created-field '{created_field}' not found in dataset columns: {sorted(cols)}")
        date_cols_to_try = [created_field]
    else:
        date_cols_to_try = [c for c in DATE_FIELD_CANDIDATES if c in cols]
        if not date_cols_to_try:
            sys.exit(
                "No suitable date column found. Checked: "
                + ", ".join(DATE_FIELD_CANDIDATES)
                + f". Dataset columns: {sorted(cols)}"
            )

    # UTC cutoff (seconds precision)
    since_dt = datetime.now(timezone.utc) - timedelta(days=days_back)
    since_str = since_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    # ---------- Attempt server-side filtering ----------
    working_where = None
    working_order = None
    first_rows = None

    for dc in date_cols_to_try:
        # 1) raw comparison
        try:
            where_expr = f"{dc} >= '{since_str}'"
            first_rows = _try_page(where_expr, dc, chunk=min(5, chunk), headers=headers)
            working_where, working_order = where_expr, dc
            break
        except SystemExit as e_raw:
            msg = str(e_raw)
            # 2) casted comparison for text dates
            if "type-mismatch" in msg or "Type mismatch" in msg or "op$>=" in msg:
                try:
                    cast_where = f"{dc}::floating_timestamp >= '{since_str}'"
                    first_rows = _try_page(cast_where, f"{dc}::floating_timestamp", chunk=min(5, chunk), headers=headers)
                    working_where, working_order = cast_where, f"{dc}::floating_timestamp"
                    break
                except SystemExit:
                    continue
            else:
                # other error (malformed col etc.) -> try next candidate
                continue

    frames = []
    if working_where is not None:
        # paginate server-side filtered
        if first_rows:
            frames.append(pd.DataFrame(first_rows))
            start_offset = len(first_rows)
        else:
            start_offset = 0

        offset = start_offset
        while True:
            params = {"$where": working_where, "$limit": chunk, "$offset": offset, "$order": working_order}
            r = requests.get(API_URL, headers=headers, params=params, timeout=120)
            r.raise_for_status()
            rows = r.json()
            if not rows:
                break
            frames.append(pd.DataFrame(rows))
            offset += len(rows)
            if len(rows) < chunk:
                break

        df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        return df

    # ---------- Fallback: blind pagination + local filtering ----------
    # pick an order column if we can (prefer a candidate date column, even if text)
    order_col = None
    for dc in date_cols_to_try:
        if dc in cols:
            order_col = dc
            break

    kept_frames = []
    pages_scanned = 0

    for rows in _page_iter_no_where(order_col=order_col, chunk=chunk, headers=headers):
        pages_scanned += 1
        page_df = pd.DataFrame(rows)

        # attempt to parse any date candidate columns
        any_newer = False
        for c in date_cols_to_try:
            if c in page_df.columns:
                # parse as UTC; errors -> NaT (won’t match cutoff)
                page_df[c] = pd.to_datetime(page_df[c], errors="coerce", utc=True)
                # any rows >= since?
                if page_df[c].notna().any() and (page_df[c] >= since_dt).any():
                    any_newer = True

        # keep only rows that are newer in ANY candidate column
        mask_newer = None
        for c in date_cols_to_try:
            if c in page_df.columns and page_df[c].dtype.kind == 'M':
                cond = page_df[c] >= since_dt
                mask_newer = cond if mask_newer is None else (mask_newer | cond)

        if mask_newer is not None:
            kept = page_df[mask_newer]
        else:
            # no parseable dates at all on this page; conservatively keep it (rare)
            kept = page_df

        if not kept.empty:
            kept_frames.append(kept)

        # stop if we’ve clearly paged past the window:
        # heuristic: if the page has NO rows newer than cutoff in ANY candidate column,
        # and we already accumulated some newer rows, we can break.
        if not any_newer and kept_frames:
            break

        # hard cap to prevent runaway pulls
        if pages_scanned >= max_pages:
            break

    return pd.concat(kept_frames, ignore_index=True) if kept_frames else pd.DataFrame()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=90, help="Days back from now (UTC) to fetch via API.")
    ap.add_argument("--out", default="data/raw_311.parquet", help="Output Parquet path.")
    ap.add_argument("--summary", default="notes/data_ingest_summary.md", help="Summary Markdown path.")
    ap.add_argument("--source", choices=["api", "csv"], default="api", help="Read from API or local CSV.")
    ap.add_argument("--path", help="CSV path if --source csv")
    ap.add_argument("--created-field", help="Override the created-date field name (e.g., requested_datetime).")
    ap.add_argument("--max-pages", type=int, default=30, help="Max pages to scan in fallback mode.")
    args = ap.parse_args()

    os.makedirs("data", exist_ok=True)
    os.makedirs("notes", exist_ok=True)

    if args.source == "api":
        df = fetch_api(
            days_back=args.days,
            chunk=50000,
            app_token=os.getenv("SOCRATA_APP_TOKEN"),
            created_field=args.created_field,
            max_pages=args.max_pages,
        )
        src = f"API (last {args.days} days)"
    else:
        if not args.path or not os.path.exists(args.path):
            sys.exit("CSV path missing. Use --path data/chi311.csv")
        df = pd.read_csv(args.path, low_memory=False)
        src = f"CSV ({args.path})"

    # Normalize common datetime columns if present
    for c in DATE_FIELD_CANDIDATES:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce", utc=True)

    df.to_parquet(args.out, index=False)

    with open(args.summary, "w", encoding="utf-8") as f:
        f.write("# Data Ingest Summary\n\n")
        f.write(f"- Source: **{src}**\n")
        f.write(f"- Rows: **{len(df):,}**\n")
        for c in DATE_FIELD_CANDIDATES:
            if c in df.columns and df[c].notna().any():
                f.write(f"- Date range: **{df[c].min()} → {df[c].max()}**\n")
                break

    with open("notes/.INGEST_DONE", "w", encoding="utf-8") as mk:
        mk.write("done\n")

    print(f"Wrote {args.out} and {args.summary}")
    print("DATA_READY_TO_PROCEED")

if __name__ == "__main__":
    main()