from datetime import timedelta, timezone
import pandas as pd
import tempfile
import json
import os

from plugins.hooks import AirflowAPIHook, MinioHook

# useful methods

def save_tmp_file(
        hook,
        data,
        object_name
):
    with tempfile.NamedTemporaryFile(suffix=".json", 
                                        delete=False, 
                                        mode="w", 
                                        encoding="utf-8") as f:
        tmp_file = f.name
        json.dump(data, f, indent=2)

    hook.upload_file("metrics", object_name, tmp_file)

    if os.path.exists(tmp_file):
        os.remove(tmp_file)

def fetch_metrics(**context):
    # Create hooks
    api_hook = AirflowAPIHook()
    minio_hook = MinioHook()

    # Get data
    dag_start_date = context["dag"].start_date.replace(tzinfo=timezone.utc)
    start_date = (dag_start_date - timedelta(days=1)).isoformat()
    items = api_hook.get_task_instances(start_date)

    if len(items) == 0:
        return 

    # Postprocess data
    df = pd.DataFrame(items)

    # Convert to datetime and normalize timezone to UTC
    df["start_date"] = pd.to_datetime(df["start_date"], utc=True)
    df["end_date"] = pd.to_datetime(df["end_date"], utc=True)

    # Compute stats
    df = df.dropna(subset=["duration"])
    stats = (
        df.groupby(["dag_id", "task_id"])["duration"]
        .agg(
            p25=lambda s: s.quantile(0.25),
            p50=lambda s: s.quantile(0.50),
            p75=lambda s: s.quantile(0.75),
            p90=lambda s: s.quantile(0.9),
            p95=lambda s: s.quantile(0.95),
            p99=lambda s: s.quantile(0.99),
        )
        .reset_index()
    )

    stats_dict = stats.to_dict(orient="records")
    save_tmp_file(minio_hook, stats_dict, f"tasks_metric_{dag_start_date}.json")
