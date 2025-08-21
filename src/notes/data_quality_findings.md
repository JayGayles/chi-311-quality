# Chicago 311 Service Requests – Data Quality Findings

## 1. Schema & Data Types
- Dataset source: **API (limit=1000)**
- Number of rows pulled: **1,000**
- Columns present (45): `:@computed_region_43wa_7qmu, :@computed_region_6mkv_f3dw, :@computed_region_bdys_3d7i, :@computed_region_du4m_ji7t, :@computed_region_rpca_8um6, :@computed_region_vrxf_vc4k, _closed_dt, _created_dt, city, closed_date, community_area, created_date, created_day_of_week, created_department, created_hour, created_month, duplicate, electrical_district, electricity_grid, last_modified_date, latitude, legacy_record, location, longitude, origin, owner_department, parent_sr_number, police_beat, police_district, police_sector, precinct, sr_number, sr_short_code, sr_type, state, status, street_address, street_direction, street_name, street_number, street_type, ward, x_coordinate, y_coordinate, zip_code`

## 2. Missing Value Summary (top 20)

| Column | Missing | % Missing |
|---|---:|---:|
| parent_sr_number | 969 | 96.9% |
| closed_date | 513 | 51.3% |
| created_department | 318 | 31.8% |
| electrical_district | 211 | 21.1% |
| electricity_grid | 211 | 21.1% |
| police_beat | 170 | 17.0% |
| ward | 170 | 17.0% |
| precinct | 170 | 17.0% |
| police_district | 170 | 17.0% |
| police_sector | 170 | 17.0% |
| community_area | 170 | 17.0% |
| :@computed_region_bdys_3d7i | 163 | 16.3% |
| longitude | 163 | 16.3% |
| location | 163 | 16.3% |
| :@computed_region_rpca_8um6 | 163 | 16.3% |
| :@computed_region_vrxf_vc4k | 163 | 16.3% |
| :@computed_region_6mkv_f3dw | 163 | 16.3% |
| :@computed_region_43wa_7qmu | 163 | 16.3% |
| latitude | 163 | 16.3% |
| :@computed_region_du4m_ji7t | 163 | 16.3% |

## 3. Uniqueness Check
- SR number column used: `sr_number`
- Duplicate count: **0**

## 4. Temporal Anomalies
- Future created_date: **0**
- Future closed_date: **0**
- Closed before created: **0**

## 5. Spatial Anomalies
- Lat/Lon or X/Y anomalies (null/zero): **163**

## 6. LEGACY_RECORD Usage
- Column present: **Yes**
- Value counts: `{"false": 1000}`

## 7. “INFORMATION ONLY” Calls
- Count: **370**
- Top addresses:
  - 2111 W Lexington ST: 370
- Top lat/lon clusters (rounded):
  - (41.872, -87.68): 236
  - (41.887, -87.773): 2
  - (41.957, -87.754): 2
  - (41.947, -87.827): 2
  - (41.672, -87.638): 1

## 8. Initial Quality Check Priorities
- Columns to validate in detail: _fill after review_
- Columns to standardize: _fill after review_
- Potential filters for future analysis: _fill after review_
