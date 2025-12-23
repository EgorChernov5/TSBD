from typing import List, Dict
import pyarrow.parquet as pq
from io import BytesIO
import pyarrow as pa
import tempfile
import logging
import os

import pandas as pd

from plugins.hooks import MinioHook
from plugins.utils import tools

# ------------------
# tasks before minio
# ------------------

def save_tmp_file(hook, bucket, data, object_name):
    table = pa.Table.from_pylist([data]) if isinstance(data, dict) else  pa.Table.from_pylist(data)
    tmp = tempfile.NamedTemporaryFile(delete=False)
    pq.write_table(table, tmp.name)
    tmp.close()

    hook.upload_file(bucket, object_name, tmp.name)

    if os.path.exists(tmp.name):
        os.remove(tmp.name)

def save_minio_raw_data(**context):
    # create hook
    hook = MinioHook()
    bucket = "raw-data"
    # Get data from previous task
    top_clans_info, top_clans_member_info = context["ti"].xcom_pull(task_ids="preprocess_raw_data")

    for clan_tag, clan_info in top_clans_info.items():       
        try:
            filename = f"{clan_tag}/{context['dag_run'].start_date}/clan_info.parquet"
            save_tmp_file(hook, bucket, clan_info, filename)

            for member in top_clans_member_info[clan_tag]:
                member_tag, member_info = next(iter(member.items()))
                filename = f"{clan_tag}/{context['dag_run'].start_date}/member_{member_tag}_info.parquet"
                save_tmp_file(hook, bucket, member_info, filename)

            logging.info(f"sucessfully process data for clan with tag {clan_tag}")
        except Exception as e:
            raise RuntimeError(f"cannot upload data to minio! error: {e}")

# ------------------
# tasks after minio
# ------------------

def load_minio_raw_data(**context):
    # create hook
    hook = MinioHook()
    bucket = "raw-data"

    # iterate over all clans
    out = {}
    clans = hook.list_prefixes(bucket=bucket)
    for clan_tag in clans:
        logging.info(f"load data for clan with tag {clan_tag}")
        # list all collections under clan
        collections = hook.list_prefixes(bucket=bucket, prefix=f"{clan_tag}/")
        if not collections:
            continue

        # get latest collection
        collections_only = [c.split("/", 1)[1] for c in collections]
        latest = max(collections_only)

        # iterate over latest collection files
        dfs = []
        prefix = f"{clan_tag}/{latest}/"
        files = hook.list_objects(bucket=bucket, prefix=prefix)
        for file_key in files:
            filename = file_key.split("/")[-1]
            if filename.startswith("member"):
                data = hook.download_to_bytes(bucket=bucket, object_name=file_key)
                table = pq.read_table(BytesIO(data))
                dfs.append(table.to_pandas())

        if dfs:
            out[clan_tag] = pd.concat(dfs, ignore_index=True)

    return out

def load_minio_raw_clan_data(**context):
    # create hook
    hook = MinioHook()
    bucket = "raw-data"

    # iterate over all clans
    out = {}
    clans = hook.list_prefixes(bucket=bucket)
    for clan_tag in clans:
        logging.info(f"load data for clan with tag {clan_tag}")
        # list all collections under clan
        collections = hook.list_prefixes(bucket=bucket, prefix=f"{clan_tag}/")
        if not collections:
            continue

        # get latest collection
        collections_only = [c.split("/", 1)[1] for c in collections]
        latest = max(collections_only)

        # iterate over latest collection files
        dfs = []
        prefix = f"{clan_tag}/{latest}/"
        files = hook.list_objects(bucket=bucket, prefix=prefix)
        for file_key in files:
            filename = file_key.split("/")[-1]
            if filename.startswith("clan_info"):
                data = hook.download_to_bytes(bucket=bucket, object_name=file_key)
                table = pq.read_table(BytesIO(data))
                dfs.append(table.to_pandas())

        if dfs:
            out[clan_tag] = pd.concat(dfs, ignore_index=True)

    return out

def postprocess_minio_raw_data(**context):
    # get data from minio
    raw_dict = context["ti"].xcom_pull(task_ids="load_minio_raw_data")

    results = {}
    for clan_tag, df in raw_dict.items():
        # change the sorting logic depending on your metric
        best = df.sort_values("trophies", ascending=False).iloc[0]
        results[clan_tag] = best[["name", "trophies"]].to_dict()
        logging.info(f"successfully postprocess data for clan with tag {clan_tag}")

    return results

# ------------------
# tasks norm minio
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

def split_minio_raw_data(**context):
    # Get data from minio
    raw_clans_dict = context["ti"].xcom_pull(task_ids="load_minio_raw_clan_data")
    raw_players_dict = context["ti"].xcom_pull(task_ids="load_minio_raw_data")

    # Clans info
    clans: List[Dict] = []                  # tag, name, members_count, war_wins_count, clan_level, clan_points
    if raw_clans_dict is not None:
        for clan_tag, raw_df in raw_clans_dict.items():
            for _, clan in raw_df.iterrows():
                # Table clan
                clans.append({
                    'tag': clan_tag,
                    'name': clan['name'],
                    'members_count': clan['members'],
                    'war_wins_count': clan['warWins'],
                    'clan_level': clan['clanLevel'],
                    'clan_points': clan['clanPoints']
                })
    
    # Players info
    players: List[Dict] = []                # tag, tag_clan, name, town_hall_level, id_league
    leagues: List[Dict] = []                # id_league, name
    achievements: List[Dict] = []           # id_achievement, name, max_starts=3
    player_achievements: List[Dict] = []    # tag_player, id_achievement, stars
    player_camps: List[Dict] = []           # tag_player, id_item, level, icon_link
    items: List[Dict] = []                  # id_item, name, item_type, village, max_level
    if raw_players_dict is not None:
        for clan_tag, raw_df in raw_players_dict.items():
            for _, player in raw_df.iterrows():
                # Table player
                tag = player['tag']
                id_league = player['league']['id'] if player['league'] is not None else None

                players.append({
                    'tag': tag,
                    'tag_clan': clan_tag,
                    'name': player['name'],
                    'town_hall_level': player['townHallLevel'],
                    'id_league': id_league
                })

                # Table league
                if id_league is not None:
                    leagues.append({
                        'id_league': id_league,
                        'name': player['league']['name']
                    })
                
                # Table player_achievement, achievement
                tools.parse_achievements(player, player_achievements, achievements)

                # TODO: add icon_link
                # Table player_camp, item (troops, heroes, spells)
                tools.parse_items(player, 'troops', items, player_camps)
                tools.parse_items(player, 'heroes', items, player_camps)
                tools.parse_items(player, 'spells', items, player_camps)

    return clans, players, leagues, achievements, player_achievements, player_camps, items

def norm_minio_raw_data(**context):
    # type List[Tuple]
    clans, players, leagues, achievements, player_achievements, player_camps, items = context["ti"].xcom_pull(task_ids="split_minio_raw_data")
    
    # Norm clans
    clans_keys = context["ti"].xcom_pull(task_ids="presettup", key="clans_keys")
    norm_clans = tools.delete_duplicates( clans, clans_keys)
    
    # Norm players
    players_keys = context["ti"].xcom_pull(task_ids="presettup", key="players_keys")
    norm_players = tools.delete_duplicates(players, players_keys)
    
    # Norm leagues
    leagues_keys = context["ti"].xcom_pull(task_ids="presettup", key="leagues_keys")
    norm_leagues = tools.delete_duplicates(leagues, leagues_keys)
    
    # Norm achievements
    achievements_keys = context["ti"].xcom_pull(task_ids="presettup", key="achievements_keys")
    norm_achievements = tools.delete_duplicates( achievements, achievements_keys)

    # Norm player_achievements
    player_achievements_keys = context["ti"].xcom_pull(task_ids="presettup", key="player_achievements_keys")
    norm_player_achievements = tools.delete_duplicates( player_achievements, player_achievements_keys)

    # TODO: add icon_link
    # Norm player_camps
    player_camps_keys = context["ti"].xcom_pull(task_ids="presettup", key="player_camps_keys")
    norm_player_camps = tools.delete_duplicates( player_camps, player_camps_keys)
    
    # Norm items
    items_keys = context["ti"].xcom_pull(task_ids="presettup", key="items_keys")
    norm_items = tools.delete_duplicates( items, items_keys)

    return norm_clans, norm_players, norm_leagues, norm_achievements, norm_player_achievements, norm_player_camps, norm_items

def save_minio_norm_data(**context):
    # create hook
    hook = MinioHook()
    bucket = "norm-data"

    # Get metadata
    table_names = context["ti"].xcom_pull(task_ids="presettup", key="table_names")
    # Get data from previous task
    new_tables_data = context["ti"].xcom_pull(task_ids="norm_minio_raw_data")

    for table_name, new_data in zip(table_names, new_tables_data):
        try:
            filename = f"{context['dag_run'].start_date}/{table_name}_info.parquet"
            save_tmp_file(hook, bucket, new_data, filename)

            logging.info(f"sucessfully process data for {table_name} (count of changes: {len(new_data)})")
        except Exception as e:
            raise RuntimeError(f"cannot upload data to minio! error: {e}")

# ------------------
# tasks scd minio
# ------------------

def load_minio_norm_data(**context):
    # Create hook
    hook = MinioHook()
    bucket = "norm-data"
    timestamps = hook.list_prefixes(bucket=bucket)
    # Get metadata
    table_names = context["ti"].xcom_pull(task_ids="presettup", key="table_names")
    tables_target_fields = [
        context["ti"].xcom_pull(task_ids="presettup", key=f"{table_name}_target_fields")
        for table_name in table_names
    ]
    # Iterate over all tables
    tables_data = []
    try:
        timestamps = hook.list_prefixes(bucket=bucket)
    except:
        logging.info(f"there is no data in the {bucket}")
        return [pd.DataFrame(columns=target_fields) for target_fields in tables_target_fields]

    for table_name in table_names:
        path_file = f'{max(timestamps)}/{table_name}_info.parquet'
        print(path_file)
        byte_data = hook.download_to_bytes(bucket=bucket, object_name=path_file)
        table_data = pq.read_table(BytesIO(byte_data))
        tables_data.append(table_data.to_pandas())
        
    # clans, players, leagues, achievements, player_achievements, player_camps, items
    return tables_data
