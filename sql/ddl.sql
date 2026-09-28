-- Append-only: every version of every interval, forever.
CREATE TABLE grid.raw_demand_versions (
  report          STRING,     -- 'RealtimeTotals', 'PredispTotals', 'ICIDemand'
  interval_start  TIMESTAMP,  -- UTC, normalised
  zone            STRING,     -- 'ONTARIO' for totals
  version         INT64,      -- from _vNN; base file resolved to its highest version
  value_mw        FLOAT64,
  published_at    TIMESTAMP,  -- IESO Last-Modified
  ingested_at     TIMESTAMP,  -- when we saw it
  source_uri      STRING,     -- gs:// path of the raw file (lineage)
  content_sha256  STRING      -- hash of the parsed values, not the raw bytes (see docs/data-notes.md)
)
PARTITION BY DATE(interval_start)
CLUSTER BY report, zone;

-- Latest known value per interval, maintained by MERGE.
CREATE TABLE grid.current_demand (
  report          STRING,
  interval_start  TIMESTAMP,
  zone            STRING,
  version         INT64,
  value_mw        FLOAT64,
  published_at    TIMESTAMP,
  ingested_at     TIMESTAMP,
  source_uri      STRING,
  content_sha256  STRING
)
PARTITION BY DATE(interval_start)
CLUSTER BY report, zone;
