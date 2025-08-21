# Chicago 311 – Step 2 Quality Checks

- Source: **API (limit=50000)**
- Rows: **50,000**
- Overall: **PASS**

| Check | Status | Details |
|---|---|---|
| Completeness: sr_number | PASS | missing=0 (0.00%) |
| Completeness: type | PASS | missing=0 (0.00%) |
| Completeness: status | PASS | missing=0 (0.00%) |
| Completeness: created_date | PASS | missing=0 (0.00%) |
| Duplicate SR numbers | PASS | duplicates=0 |
| Future created_date | PASS | count=0 |
| Future closed_date | PASS | count=0 |
| Closed before created | PASS | count=0 |
| Coordinate anomalies (null/zero) | PASS | count=190 (0.38%) |
| Legacy records present | PASS | {"false": 50000} |
| Information-only address dominance | INFO | info_calls=16874, top_addr='2111 W Lexington ST', dominance=100.00% |
