import logging

import pandas as pd

from plugins.hooks import PostgresDataHook
from plugins.utils.tools import apply_scd

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

def presettup(**context):
    context["ti"].xcom_push(
        key='table_names',
        value=['clans', 'players', 'leagues', 'achievements', 'player_achievements', 'player_camps', 'items']
    )

    # Table clan
    context["ti"].xcom_push(
        key="clans_target_fields",
        value=['tag', 'name', 'members_count', 'war_wins_count', 'clan_level', 'clan_points']
    )
    context["ti"].xcom_push(key="clans_keys", value=['tag'])
    # Table player
    context["ti"].xcom_push(
        key="players_target_fields",
        value=['tag', 'tag_clan', 'name', 'town_hall_level', 'id_league']
    )
    context["ti"].xcom_push(key="players_keys", value=['tag'])
    # Table league
    context["ti"].xcom_push(
        key="leagues_target_fields",
        value=['id_leagues', 'league_name']
    )
    context["ti"].xcom_push(key="leagues_keys", value=['id_league'])
    # Table achievement
    context["ti"].xcom_push(
        key="achievements_target_fields",
        value=['id_achievement', 'name', 'max_starts']
    )
    context["ti"].xcom_push(key="achievements_keys", value=['id_achievement'])
    # Table player_achievement
    context["ti"].xcom_push(
        key="player_achievements_target_fields",
        value=['tag_player', 'id_achievement', 'stars']
    )
    context["ti"].xcom_push(key="player_achievements_keys", value=['tag_player', 'id_achievement'])
    # Table player_camp
    context["ti"].xcom_push(
        key="player_camps_target_fields",
        value=['tag_player', 'id_item', 'level']  # TODO: ['tag_player', 'id_item', 'level', 'icon_link']
    )
    context["ti"].xcom_push(key="player_camps_keys", value=['tag_player', 'id_item'])
    # Table item
    context["ti"].xcom_push(
        key="items_target_fields",
        value=['id_item', 'name', 'item_type', 'village', 'max_level']
    )
    context["ti"].xcom_push(key="items_keys", value=['id_item'])

def load_postgres_sqd_data(**context):
    pass

def compare_scd_data(**context):
    # Get metadata
    table_names = context["ti"].xcom_pull(task_ids="presettup", key="table_names")
    tables_target_fields = [
        context["ti"].xcom_pull(task_ids="presettup", key=f"{table_name}_target_fields")
        for table_name in table_names
    ]
    tables_keys = [
        context["ti"].xcom_pull(task_ids="presettup", key=f"{table_name}_keys")
        for table_name in table_names
    ]
    # Get old and new data
    mid_data = context["ti"].xcom_pull(task_ids="load_minio_norm_data")
    old_data = load_postgres_sqd_data()
    # Compare data
    new_data = []
    for keys, target_fields, new_df, old_df in zip(tables_keys, tables_target_fields, mid_data, old_data):
        merged_df= old_df.merge(
            new_df,
            on=keys,
            how="outer",
            suffixes=("_old", "_new"),
            indicator=True
        )
        cols = [c for c in merged_df.columns if ('_old' in c) or ('_new' in c)]

        new_rows = merged_df[merged_df["_merge"] == "right_only"]
        both = merged_df[merged_df["_merge"] == "both"]
        changed = both[
            (both[[c for c in cols if '_old' in c]].values !=
            both[[c for c in cols if '_new' in c]].values).any(axis=1)
        ]
        # Union
        non_key_cols = [c for c in target_fields if c not in keys]
        new_clean = new_rows[
            keys + [c + "_new" for c in non_key_cols]
        ].copy()
        new_clean.columns = keys + list(non_key_cols)

        changed_clean = changed[
            keys + [c + "_new" for c in non_key_cols]
        ].copy()
        changed_clean.columns = keys + list(non_key_cols)
        result = pd.concat([new_clean, changed_clean], ignore_index=True)
        # Save
        new_data.append([tuple(v) for v in result.values])

    return new_data  # [clans, players, leagues, achievements, player_achievements, player_camps, items]

# TODO: add SCD-2
def scd_postgres_norm_data(**context):
    hook = PostgresDataHook()

    clans, players, leagues, achievements, player_achievements, player_camps, items = context["ti"].xcom_pull(task_ids="norm_minio_raw_data")

    apply_scd(
        hook,
        'dds_clan',
        clans[0],
        clans[1:],
        ['tag'],
        ['members_count', 'war_wins_count', 'clan_level', 'clan_points']
    )

def save_postgres_norm_data(**context):
    hook = PostgresDataHook()

    # Get data from previous task
    clans, players, leagues, achievements, player_achievements, player_camps, items = context["ti"].xcom_pull(task_ids="norm_minio_raw_data")
    
    # Init tables
    clan_table = "dds_clan"
    clan_columns = clans.pop(0)
    hook.create_table_if_not_exists(clan_table, clan_columns, clans[0], primary_keys=["tag"], surrogate_key='id_sk')

    # player_table = "dds_player"
    # player_columns = players.pop(0)
    # hook.create_table_if_not_exists(player_table, player_columns, players[0], primary_keys=["tag"])

    # league_table = "dds_league"
    # league_columns = leagues.pop(0)
    # hook.create_table_if_not_exists(league_table, league_columns, leagues[0], primary_keys=["id_leagues"])

    # achievement_table = "dds_achievement"
    # achievement_columns = achievements.pop(0)
    # hook.create_table_if_not_exists(achievement_table, achievement_columns, achievements[0], primary_keys=["id_achievement"])

    # player_achievement_table = "dds_player_achievement"
    # player_achievement_columns = player_achievements.pop(0)
    # hook.create_table_if_not_exists(player_achievement_table, player_achievement_columns, player_achievements[0], primary_keys=["tag", "id_achievement"])

    # player_camp_table = "dds_player_camp"
    # player_camp_columns = player_camps.pop(0)
    # hook.create_table_if_not_exists(player_camp_table, player_camp_columns, player_camps[0], primary_keys=["tag", "id_item"])

    # item_table = "dds_item"
    # item_columns = items.pop(0)
    # hook.create_table_if_not_exists(item_table, item_columns, items[0], primary_keys=["id_item"])

    # Insert data
    hook.insert_norm_rows(clan_table, clans, clan_columns)
    # hook.insert_norm_rows(player_table, players, player_columns)
    # hook.insert_norm_rows(league_table, leagues, league_columns)
    # hook.insert_norm_rows(achievement_table, achievements, achievement_columns)
    # hook.insert_norm_rows(player_achievement_table, player_achievements, player_achievement_columns)
    # hook.insert_norm_rows(player_camp_table, player_camps, player_camp_columns)
    # hook.insert_norm_rows(item_table, items, item_columns)
