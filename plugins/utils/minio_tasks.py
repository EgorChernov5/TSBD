import pyarrow.parquet as pq
from io import BytesIO
import pyarrow as pa
import tempfile
import logging
import os
import hashlib

import pandas as pd

from plugins.hooks import MinioHook
from plugins.utils import tools

# ------------------
# tasks before minio
# ------------------

def save_tmp_file(
        hook,
        data,
        object_name
):
    table = pa.Table.from_pylist([data])
    tmp = tempfile.NamedTemporaryFile(delete=False)
    pq.write_table(table, tmp.name)
    tmp.close()

    hook.upload_file("raw-data", object_name, tmp.name)

    if os.path.exists(tmp.name):
        os.remove(tmp.name)

def save_minio_raw_data(
    **context
):
    # create hook
    hook = MinioHook()
    # Get data from previous task
    top_clans_info, top_clans_member_info = context["ti"].xcom_pull(task_ids="preprocess_raw_data")

    for clan_tag, clan_info in top_clans_info.items():       
        try:
            filename = f"{clan_tag}/{context['dag_run'].start_date}/clan_info.parquet"
            save_tmp_file(hook, clan_info, filename)

            for member in top_clans_member_info[clan_tag]:
                member_tag, member_info = next(iter(member.items()))
                filename = f"{clan_tag}/{context['dag_run'].start_date}/member_{member_tag}_info.parquet"
                save_tmp_file(hook, member_info, filename)

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

def split_minio_raw_data(**context):
    # Get data from minio
    raw_clans_dict = context["ti"].xcom_pull(task_ids="load_minio_raw_clan_data")
    raw_players_dict = context["ti"].xcom_pull(task_ids="load_minio_raw_data")

    # Clans info
    clans = {}                  # tag, name, members_count, war_wins_count, clan_level, clan_points
    if raw_clans_dict is not None:
        for clan_tag, raw_df in raw_clans_dict.items():
            for _, clan in raw_df.iterrows():
                # Table clan
                clans[clan_tag] = {
                    'name': clan['name'],
                    'members_count': clan['members'],
                    'war_wins_count': clan['warWins'],
                    'clan_level': clan['clanLevel'],
                    'clan_points': clan['clanPoints']
                }

    # Players info
    players = {}                # tag, tag_clan, name, town_hall_level, id_league
    leagues = {}                # id_league, league_name
    achievements = {}           # id_achievement, name, max_starts=3
    player_achievements = {}    # tag, id_achievement, stars
    player_camps = {}           # tag, id_item, level, icon_link
    items = {}                  # id_item, name, item_type, village, max_level
    if raw_players_dict is not None:
        for clan_tag, raw_df in raw_players_dict.items():
            for _, player in raw_df.iterrows():
                # Table player
                tag = player['tag']
                id_league = player['league']['id'] if player['league'] is not None else None  # TODO: is it ok?

                players[tag] = {
                    'tag_clan': clan_tag,
                    'name': player['name'],
                    'town_hall_level': player['townHallLevel'],
                    'id_league': id_league
                }

                # Table league
                if id_league is not None: leagues[id_league] = player['league']['name']
                
                # Table player_achievement, achievement
                tools.parse_achievements(player, player_achievements, achievements)

                # TODO: add icon_link
                # Table player_camp, item (troops, heroes, spells)
                tools.parse_items(player, 'troops', items, player_camps)
                tools.parse_items(player, 'heroes', items, player_camps)
                tools.parse_items(player, 'spells', items, player_camps)

    return clans, players, leagues, achievements, player_achievements, player_camps, items

def norm_minio_raw_data(**context):
    clans, players, leagues, achievements, player_achievements, player_camps, items = context["ti"].xcom_pull(task_ids="split_minio_raw_data")
    
    # Norm clans
    norm_clans = [('tag', 'name', 'members_count', 'war_wins_count', 'clan_level', 'clan_points')]
    for tag, data in clans.items():
        # tag, name, members_count, war_wins_count, clan_level, clan_points
        norm_clans.append((tag, data['name'], data['members_count'], data['war_wins_count'], data['clan_level'], data['clan_points']))
    
    # Norm players
    norm_players = [('tag', 'tag_clan', 'name', 'town_hall_level', 'id_league')]
    for tag, data in players.items():
        # tag, tag_clan, name, town_hall_level, id_league
        norm_players.append((tag, data['tag_clan'], data['name'], data['town_hall_level'], data['id_league']))
    
    # Norm leagues
    # id_leagues, league_name
    norm_leagues = [('id_leagues', 'league_name')]
    for id_league, name in leagues.items():
        norm_leagues.append((id_league, name))
    
    # Norm achievements
    norm_achievements = [('id_achievement', 'name', 'max_starts')]
    for id_achievement, data in achievements.items():
        # id_achievement, name, max_starts
        norm_achievements.append((id_achievement, data['name'], data['max_starts']))
    
    # Norm player_achievements
    norm_player_achievements = [('tag', 'id_achievement', 'stars')]
    for tag, data in player_achievements.items():
        # tag, id_achievement, stars
        for id_achievement, star in zip(data['id_achievement'], data['stars']):
            norm_player_achievements.append((tag, id_achievement, star))
    
    # TODO: add icon_link
    # Norm player_camps
    # norm_player_camps = [('tag', 'id_item', 'level', 'icon_link')]
    norm_player_camps = [('tag', 'id_item', 'level')]
    for tag, data in player_camps.items():
        # tag, id_item, level, icon_link
        # for id_item, level, icon_link in zip(data['id_item'], data['level'], data['icon_link']):
        for id_item, level in zip(data['id_item'], data['level']):
            norm_player_camps.append((tag, id_item, level))
    
    # Norm items
    norm_items = [('id_item', 'name', 'item_type', 'village', 'max_level')]
    for id_item, data in items.items():
        # id_item, name, item_type, village, max_level
        norm_items.append((id_item, data['name'], data['item_type'], data['village'], data['max_level']))

    return norm_clans, norm_players, norm_leagues, norm_achievements, norm_player_achievements, norm_player_camps, norm_items
