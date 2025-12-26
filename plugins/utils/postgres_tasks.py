from plugins.utils.constants import END_DATE
import logging
from typing import List
from datetime import datetime, timezone

import pandas as pd

from plugins.hooks import PostgresDataHook
from plugins.utils.tools import apply_scd, convert_types

# ------------------
# tasks before postgres
# ------------------

def save_postgres_raw_data(**context):
    hook = PostgresDataHook()

    # Get data from previous task
    top_clans_info, top_clans_member_info = context["ti"].xcom_pull(task_ids="preprocess_raw_data")
    dag_run_date = context['dag_run'].start_date.date()

    # Create tables dynamically based on first record
    first_clan_tag, first_clan_info = next(iter(top_clans_info.items()))
    first_clan_info["clan_tag"] = first_clan_tag
    hook.create_table_from_record("clan", first_clan_info, primary_keys=["clan_tag"])

    member_tag, member_info = next(iter(top_clans_member_info[first_clan_tag][0].items()))
    member_info.update({"clan_tag": first_clan_tag, "member_tag": member_tag})
    hook.create_table_from_record("clan_member", member_info, primary_keys=["clan_tag","member_tag"])

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

# ------------------
# tasks after minio
# ------------------

def load_postgres_sqd_data(**context):
    # Create hook
    hook = PostgresDataHook()
    # Get metadata
    table_names = context["ti"].xcom_pull(task_ids="presettup", key="table_names")
    tables_target_fields = [
        context["ti"].xcom_pull(task_ids="presettup", key=f"{table_name}_target_fields")
        for table_name in table_names
    ]
    # Iterate over all tables
    tables_data = []
    for table_name, target_fields in zip(table_names, tables_target_fields):
        actual_fields = None
        if table_name in ['clan', 'player']:
            target_fields += ['start_date', 'end_date']
            actual_fields = 'end_date'
        
        cols, data = hook.find_actual(table_name, actual_fields)

        df = pd.DataFrame(data=data, columns=cols) if len(cols) else pd.DataFrame(data=data, columns=target_fields)
        tables_data.append(df[target_fields])
    
    # clan, player, league, achievement, player_achievement, player_camp, item
    return tables_data

def compare_scd_data(**context):
    # Get metadata
    table_names = context["ti"].xcom_pull(task_ids="presettup", key="table_names")
    tables_keys = [
        context["ti"].xcom_pull(task_ids="presettup", key=f"{table_name}_keys")
        for table_name in table_names
    ]
    # Get old and new data
    mid_data: List[pd.DataFrame] = context["ti"].xcom_pull(task_ids="load_minio_norm_data")
    old_data: List[pd.DataFrame] = context["ti"].xcom_pull(task_ids="load_postgres_sqd_data")
    # Compare data
    new_data = []
    changed_data = []
    for table_name, keys, new_df, old_df in zip(table_names, tables_keys, mid_data, old_data):
        old_df = convert_types(new_df, old_df)
        merged_df = old_df.merge(new_df, how='right', on=keys, suffixes=('_old', '_new'))

        merged_cols = [c.replace('_old', '') for c in merged_df.columns if '_old' in c]
        old_changed_cols = {c + '_old': c for c in merged_cols}
        new_changed_cols = {c + '_new': c for c in merged_cols}

        merged_df = merged_df[(
            merged_df[list(old_changed_cols.keys())].fillna('__NA__').values !=
            merged_df[list(new_changed_cols.keys())].fillna('__NA__').values
        ).any(axis=1)]

        changed_data.append(merged_df.dropna().drop(columns=old_changed_cols).rename(columns=new_changed_cols)[old_df.columns])
        new_data.append(merged_df[merged_df.isna().any(axis=1)].drop(columns=old_changed_cols).rename(columns=new_changed_cols)[old_df.columns])
        if table_name == 'league':
            print(merged_df)

    # tuple of [clan, player, league, achievement, player_achievement, player_camp, item]
    return new_data, changed_data

def save_postgres_scd_data(**context):
    hook = PostgresDataHook()

    # Get metadata
    dag_run_timestamp = context['logical_date']
    table_names = context["ti"].xcom_pull(task_ids="presettup", key="table_names")
    tables_keys = [
        context["ti"].xcom_pull(task_ids="presettup", key=f"{table_name}_keys")
        for table_name in table_names
    ]
    # Get data
    new_data, changed_data = context["ti"].xcom_pull(task_ids="compare_scd_data")

    # Close changed data
    for table_name, table_keys, df in zip(table_names, tables_keys, changed_data):
        if df.empty:
            continue

        if table_name in ['clan', 'player']:
            # SCD-2
            # Update df
            df = df.assign(start_date=dag_run_timestamp).drop(columns=['end_date'])
            # Send request
            hook.update_scd2_data(
                df=df,
                table_name=table_name,
                change_ts=dag_run_timestamp,
                pk_cols=table_keys
            )
        else:
            # SCD-1
            hook.update_scd1_data(
                df=df,
                table_name=table_name,
                pk_cols=table_keys
            )

    # Add new data
    for table_name, table_keys, df in zip(table_names, tables_keys, new_data):
        if df.empty:
            continue

        if table_name in ['clan', 'player']:
            # SCD-2
            # Update df
            df = df.assign(start_date=dag_run_timestamp).drop(columns=['end_date'])
            # Send requests
            hook.create_table_if_not_exists(
                table_name,
                tuple(df.columns) + ('end_date',),
                tuple(df.iloc[0]) + (END_DATE,),
                table_keys,
                'id',
                [(*table_keys, 'end_date')]
            )
            hook.insert_scd2_data(
                df=df,
                table_name=table_name,
                change_ts=dag_run_timestamp
            )
        else:
            # SCD-1
            hook.create_table_if_not_exists(
                table_name,
                df.columns,
                tuple(df.values[0]),
                table_keys,
                unique_constraints=[tuple(table_keys)]
            )
            hook.insert_scd1_data(
                df=df,
                table_name=table_name,
                pk_cols=table_keys
            )
