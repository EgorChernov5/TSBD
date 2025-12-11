import logging

import pandas as pd

from plugins.hooks import PostgresDataHook

# ------------------
# tasks before postgres
# ------------------

def save_postgres_raw_data(**context):
    hook = PostgresDataHook()

    # Get data from previous task
    top_clans_info, top_clans_member_info = context["ti"].xcom_pull(task_ids="preprocess_raw_data")
    dag_run_date = context['dag_run'].start_date.date()

    for clan_tag in top_clans_info:
        for member in top_clans_member_info[clan_tag]:
            member_tag, member_info = next(iter(member.items()))
            member_info.pop('legendStatistics', None)

    # Create tables dynamically based on first record
    first_clan_tag, first_clan_info = next(iter(top_clans_info.items()))
    first_clan_info["clan_tag"] = first_clan_tag
    hook.create_table_from_record("clan", first_clan_info, primary_keys=["clan_tag"])

    first_member_info = next(iter(top_clans_member_info[first_clan_tag][0].values()))
    first_member_info.update({"clan_tag": first_clan_tag, "member_tag": "dummy"})
    hook.create_table_from_record("clan_member", first_member_info, primary_keys=["clan_tag","member_tag"])

    # Insert data
    for clan_tag, clan_info in top_clans_info.items():
        clan_info["clan_tag"] = clan_tag
        hook.insert("clan", clan_info, run_date=dag_run_date)

        member_records = []
        for member in top_clans_member_info[clan_tag]:
            member_tag, member_info = next(iter(member.items()))
            member_info.update({"clan_tag": clan_tag, "member_tag": member_tag})
            member_records.append(member_info)
        if member_records:
            hook.insert("clan_member", member_records, run_date=dag_run_date)

        logging.info(f"Successfully processed data for clan {clan_tag}")

# ------------------
# tasks after postgres
# ------------------

def load_postgres_raw_data(**context):
    hook = PostgresDataHook()
    out = {}

    # Get all clans
    all_clans = hook.find("clan", run_date=None)
    clan_tags = list({c["clan_tag"] for c in all_clans})

    for clan_tag in clan_tags:
        logging.info(f"Loading data for clan {clan_tag}")

        # Find latest run_date for this clan
        clan_rows = hook.find("clan", where="clan_tag=%(tag)s", params={"tag": clan_tag})
        if not clan_rows:
            continue
        latest_date = max(row["run_date"] for row in clan_rows)

        # Load members for latest date
        members = hook.find(
            "clan_member",
            run_date=latest_date,
            where="clan_tag=%(tag)s",
            params={"tag": clan_tag}
        )
        if not members:
            continue

        # Extract all member columns except fixed ones
        member_data = []
        for m in members:
            d = {k: v for k, v in m.items() if k not in ("clan_tag", "member_tag", "run_date")}
            member_data.append(d)

        df = pd.DataFrame(member_data)
        out[clan_tag] = df

    return out

def postprocess_postgres_raw_data(**context):
    raw_dict = context["ti"].xcom_pull(task_ids="load_postgres_raw_data")
    results = {}

    for clan_tag, df in raw_dict.items():
        best = df.sort_values("trophies", ascending=False).iloc[0]
        results[clan_tag] = best[["name", "trophies"]].to_dict()
        logging.info(f"Postprocessed clan {clan_tag}")

    return results