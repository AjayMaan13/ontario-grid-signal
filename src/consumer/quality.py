"""Data-quality checks on the BigQuery tables. Each returns the number of violations: 0 is good."""

CHECKS = {
    # one row per interval in current_demand
    "duplicate_keys": """
        SELECT COUNT(*) FROM (SELECT 1 FROM `{current}` GROUP BY report, interval_start, zone HAVING COUNT(*) > 1)""",

    # current_demand holds the highest version that raw_demand_versions has, for every interval
    "current_matches_raw": """
        SELECT COUNT(*) FROM (
          SELECT report, interval_start, zone, MAX(version) AS v FROM `{raw}` GROUP BY report, interval_start, zone) R
        LEFT JOIN `{current}` C USING (report, interval_start, zone)
        WHERE C.report IS NULL OR C.version != R.v""",

    # values inside 80% of the lowest and 120% of the highest ICIDemand ever seen (no history counts as a violation)
    "values_in_range": """
        WITH h AS (SELECT MIN(value_mw) AS lo, MAX(value_mw) AS hi FROM `{current}` WHERE report = 'ICIDemand')
        SELECT COUNT(*) FROM `{current}`, h WHERE h.lo IS NULL OR value_mw < 0.8 * h.lo OR value_mw > 1.2 * h.hi""",

    # a day with data on both sides must have all 288 five-minute readings (days are EST, like IESO's)
    "realtime_gaps": """
        WITH d AS (
          SELECT DATE(TIMESTAMP_SUB(interval_start, INTERVAL 5 HOUR)) AS day, COUNT(*) AS n
          FROM `{current}` WHERE report = 'RealtimeTotals' GROUP BY day)
        SELECT COUNT(*) FROM d
        WHERE n != 288
          AND DATE_SUB(day, INTERVAL 1 DAY) IN (SELECT day FROM d)
          AND DATE_ADD(day, INTERVAL 1 DAY) IN (SELECT day FROM d)""",
}

# BIT_XOR of per-row fingerprints does not depend on row order, so equal data gives an equal checksum.
SUMMARY = """
    SELECT COUNT(*) AS n, BIT_XOR(FARM_FINGERPRINT(CONCAT(
      report, '|', CAST(interval_start AS STRING), '|', zone, '|', CAST(version AS STRING), '|', CAST(value_mw AS STRING)))) AS checksum
    FROM `{table}`"""


def _scalar(client, sql):
    return list(client.query(sql).result())[0][0]


def run_all(client, dataset):
    names = {"raw": f"{client.project}.{dataset}.raw_demand_versions", "current": f"{client.project}.{dataset}.current_demand"}
    return {name: _scalar(client, sql.format(**names)) for name, sql in CHECKS.items()}


def summary(client, dataset):
    out = {}
    for table in ("raw_demand_versions", "current_demand"):
        row = list(client.query(SUMMARY.format(table=f"{client.project}.{dataset}.{table}")).result())[0]
        out[table] = {"rows": row["n"], "checksum": row["checksum"]}
    return out
