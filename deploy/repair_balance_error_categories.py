"""Generate bounded SQL to repair the v1 quota recognition bug.

Default output is a read-only preview. --apply emits a transaction for psql;
this program never connects to a database. It uses the gateway's exact final
request deduplication/classifier, and replaces only buckets whose complete old
category counts can still be reproduced from retained source logs.
"""
import argparse
import datetime as dt
from pathlib import Path

SOURCE = Path(__file__).resolve().parents[1] / "backend/internal/repository/channel_monitor_v2_aggregation.go"
ADDED_NEEDLES = (
    "insufficient account balance", "insufficient_balance",
    "insufficient_quota", "api_key_quota_exhausted",
)
TABLES = (
    ("channel_monitor_v2_error_metrics_1m", "60"),
    ("channel_monitor_v2_error_metrics_rollup", "bucket_seconds"),
)


def timestamp(value):
    parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timestamps must include a timezone")
    return parsed.astimezone(dt.timezone.utc).replace(second=0, microsecond=0)


def build_plan(start, end, source=None):
    if not dt.timedelta() < end - start <= dt.timedelta(days=7):
        raise ValueError("repair windows must be positive and at most 7 days")
    source = SOURCE.read_text(encoding="utf-8") if source is None else source
    marker = "const channelMonitorV2ErrorAggregationSQL = `"
    query = source.split(marker, 1)[1].split("`", 1)[0]
    query = query.split(", metric_rows AS (", 1)[0]
    if "classified AS (" not in query or "INSERT INTO" in query:
        raise ValueError("unexpected aggregation SQL layout")
    # ISO timestamps come only from datetime objects, never raw SQL input.
    query = query.replace("$1", f"TIMESTAMPTZ '{start.isoformat()}'")
    query = query.replace("$2", f"TIMESTAMPTZ '{end.isoformat()}'")
    legacy = query
    for needle in ADDED_NEEDLES:
        pattern = f",'%{needle}%'"
        if pattern not in legacy:
            raise ValueError(f"missing expected classifier needle: {needle}")
        legacy = legacy.replace(pattern, "")
    recorded = " UNION ALL ".join(
        f"SELECT {seconds} AS seconds, bucket_start, platform, group_id, model, "
        f"error_category, error_requests FROM {table} WHERE taxonomy_version=1 "
        f"AND bucket_start >= TIMESTAMPTZ '{start.isoformat()}' "
        f"AND bucket_start + ({seconds} * INTERVAL '1 second') <= TIMESTAMPTZ '{end.isoformat()}'"
        for table, seconds in TABLES
    )
    return f"""
WITH source_rows AS (
    SELECT 'old' AS kind, * FROM ({legacy} SELECT bucket_start, platform, group_id, model, category FROM classified) old_source
    UNION ALL
    SELECT 'new' AS kind, * FROM ({query} SELECT bucket_start, platform, group_id, model, category FROM classified) new_source
), source_counts AS (
    SELECT kind, seconds,
      to_timestamp(floor(extract(epoch FROM bucket_start)/seconds)*seconds) AS bucket_start,
      platform, group_id, model, category, COUNT(*) AS n
    FROM source_rows CROSS JOIN (VALUES (60),(300),(3600),(43200),(86400)) tiers(seconds)
    GROUP BY 1,2,3,4,5,6,7
), expected AS (
    SELECT kind, seconds, bucket_start, platform, group_id, model,
      jsonb_object_agg(category, n) AS counts
    FROM source_counts GROUP BY 1,2,3,4,5,6
), recorded_rows AS ({recorded}), recorded AS (
    SELECT seconds, bucket_start, platform, group_id, model,
      jsonb_object_agg(error_category,error_requests) FILTER (WHERE error_requests>0) AS counts
    FROM recorded_rows GROUP BY 1,2,3,4,5
)
SELECT r.seconds, r.bucket_start, r.platform, r.group_id, r.model,
    r.counts AS recorded_counts, o.counts AS old_counts, n.counts AS new_counts,
    CASE WHEN r.counts=n.counts THEN 'already_correct'
         WHEN r.counts=o.counts THEN 'repair'
         ELSE 'skip_source_mismatch' END AS action
FROM recorded r
JOIN expected o USING (seconds,bucket_start,platform,group_id,model)
JOIN expected n USING (seconds,bucket_start,platform,group_id,model)
WHERE o.kind='old' AND n.kind='new' AND o.counts<>n.counts
""".strip()


def build_sql(start, end, apply=False):
    plan = build_plan(start, end)
    if not apply:
        return "BEGIN READ ONLY;\nSET LOCAL statement_timeout='30s';\n" + plan + ";\nCOMMIT;\n"
    statements = [
        "BEGIN;", "SET LOCAL lock_timeout='3s';", "SET LOCAL statement_timeout='30s';",
        "LOCK TABLE channel_monitor_v2_error_metrics_1m, channel_monitor_v2_error_metrics_rollup IN SHARE ROW EXCLUSIVE MODE;",
        "CREATE TEMP TABLE quota_category_repair ON COMMIT DROP AS " + plan + ";",
    ]
    for table, seconds in TABLES:
        tier = "p.seconds=60" if seconds == "60" else "p.seconds<>60 AND t.bucket_seconds=p.seconds"
        statements.append(f"""DELETE FROM {table} t USING quota_category_repair p
WHERE p.action='repair' AND {tier} AND t.taxonomy_version=1
  AND t.bucket_start=p.bucket_start AND t.platform=p.platform
  AND t.group_id=p.group_id AND t.model=p.model;""")
        extra_col, extra_val = ("", "") if seconds == "60" else ("bucket_seconds,", "p.seconds,")
        tier = "p.seconds=60" if seconds == "60" else "p.seconds<>60"
        statements.append(f"""INSERT INTO {table}
({extra_col}bucket_start,platform,group_id,model,error_category,taxonomy_version,error_requests)
SELECT {extra_val}p.bucket_start,p.platform,p.group_id,p.model,c.key,1,c.value::bigint
FROM quota_category_repair p CROSS JOIN LATERAL jsonb_each_text(p.new_counts) c
WHERE p.action='repair' AND {tier};""")
    statements.extend([
        "SELECT action,seconds,COUNT(*) AS buckets FROM quota_category_repair GROUP BY 1,2 ORDER BY 1,2;",
        "COMMIT;",
    ])
    return "\n".join(statements) + "\n"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", required=True, type=timestamp)
    parser.add_argument("--end", required=True, type=timestamp)
    parser.add_argument("--apply", action="store_true", help="emit modifying SQL, without executing it")
    args = parser.parse_args()
    print(build_sql(args.start, args.end, args.apply), end="")
