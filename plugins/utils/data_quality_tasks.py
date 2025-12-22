import json

from plugins.hooks import MinioHook, PostgresDataHook


# ------------------
# size task
# ------------------

def get_latest_timestamp():
    # create hook
    hook = MinioHook()
    bucket = "raw-data"

    # get clan info
    clan_tag = hook.list_prefixes(bucket=bucket)[0]
    collections = hook.list_prefixes(bucket=bucket, prefix=f"{clan_tag}/")
    if not collections:
        return None

    # get latest timestamp
    collections_only = [c.split("/", 1)[1] for c in collections]
    latest = max(collections_only)

    return latest

def get_raw_data_size(
        latest_timestamp: str
):
    # create hook
    hook = MinioHook()
    bucket = "raw-data"

    # iterate over all clans
    total_size = 0.0
    clans = hook.list_prefixes(bucket=bucket)
    for clan_tag in clans:
        # list all collections under clan
        collections = hook.list_prefixes(bucket=bucket, prefix=f"{clan_tag}/")
        if not collections:
            continue

        # iterate over all files
        prefix = f"{clan_tag}/{latest_timestamp}/"
        total_size += hook.calculate_objects_size(bucket=bucket, prefix=prefix)

    return total_size

def get_normalized_data_size():
    # create hook
    hook = PostgresDataHook()
    return hook.calculate_db_size()

def compare_data_size(**kwargs):
    # create hook
    hook = MinioHook()
    bucket = "raw-data"

    # get latest timestamp
    latest_timestamp = get_latest_timestamp()
    
    # calculate data size before/after normalization
    raw_size = get_raw_data_size(latest_timestamp)
    norm_size_after = get_normalized_data_size()

    norm_size_before_bytes = hook.download_to_bytes(
        bucket=bucket, 
        object_name=f"data_quality_metrics{latest_timestamp}.json"
    )
    norm_size_before = json.loads(norm_size_before_bytes.decode("utf-8"))

    total_size = 0.0
    for key in norm_size_after.items():
        total_size += norm_size_after[key] - norm_size_before[key]

    return {"before_norm": raw_size, "after_norm": total_size}

# ------------------
# find anomalies task
# ------------------

def check_anomalies(**kwargs):
    # create hook
    hook = PostgresDataHook()

    # Set anomaly queries
    anomaly_queries = [
        "SELECT COUNT(*) FROM clan WHERE members_count < 0;",
        "SELECT COUNT(*) FROM clan WHERE war_wins < 0;",
        "SELECT COUNT(*) FROM player WHERE townhall_level < 0;"
    ]

    # Try to find anomalies
    for q in anomaly_queries:
        cnt = hook.execute_query(q).fetchone()[0]
        if cnt > 0:
            raise ValueError(f"Anomaly detected: {cnt} rows match query '{q}'")

# ------------------
# scd validation task
# ------------------

def scd_validation(**kwargs):
    # create hook
    hook = PostgresDataHook()
    # set SCD2 table names
    scd2_tables = ['clan', 'player']

    for table in scd2_tables:
        query_unique = f"""
            SELECT tag, start_date, COUNT(*)
            FROM {table}
            GROUP BY tag, start_date
            HAVING COUNT(*) > 1;
        """
        duplicates = hook.execute_query(query_unique).fetchall()
        if duplicates:
            raise ValueError(f"SCD2 uniqueness violation in table {table}: {duplicates}")

        query_dates = f"""
            SELECT tag, start_date, end_date
            FROM {table}
            WHERE end_date IS NOT NULL AND start_date >= end_date;
        """
        invalid_dates = hook.execute_query(query_dates).fetchall()
        if invalid_dates:
            raise ValueError(f"SCD2 date violation in table {table}: {invalid_dates}")
