import argparse, os, sys
import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()
API_URL = "https://data.cityofchicago.org/resource/v6vf-nfxy.json"
APP_TOKEN = os.getenv("SOCRATA_APP_TOKEN")

def load_from_api(limit=1000):
    params = {"$limit": limit}
    headers = {"X-App-Token": APP_TOKEN} if APP_TOKEN else {}
    r = requests.get(API_URL, params=params, headers=headers, timeout=60)
    r.raise_for_status()
    return pd.DataFrame(r.json())

def load_from_csv(path):
    return pd.read_csv(path, low_memory=False)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", choices=["api", "csv"], required=True)
    ap.add_argument("--limit", type=int, default=1000)
    ap.add_argument("--path", type=str)
    args = ap.parse_args()

    if args.source == "api":
        df = load_from_api(limit=args.limit)
        print(f"Loaded {len(df):,} rows from API")
    else:
        if not args.path:
            print("Need --path for CSV mode"); sys.exit(1)
        df = load_from_csv(args.path)
        print(f"Loaded {len(df):,} rows from CSV")

    print("\nColumns:")
    print(df.columns.tolist())

    print("\nMissing values per column:")
    print(df.isna().sum())

if __name__ == "__main__":
    main()