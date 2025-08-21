import os
import pandas as pd

# Column candidates (handles schema drift)
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

def find_col(df: pd.DataFrame, key: str):
    """Return the first DataFrame column that matches the logical key."""
    for c in CANDIDATES.get(key, []):
        if c in df.columns:
            return c
    return None

def ensure_dir(path: str):
    """Create parent folder for a file path if needed."""
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)

def pct(n: int, d: int) -> float:
    """Safe percentage helper (0 if denominator is 0)."""
    return (n/d) if d else 0.0