import pyarrow.parquet as pq
from io import BytesIO
import pyarrow as pa
import tempfile
import logging
import os
import hashlib

import pandas as pd

from plugins.hooks import MinioHook

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
    # get data from minio
    raw_clans_dict = context["ti"].xcom_pull(task_ids="load_minio_raw_clan_data")
    raw_players_dict = context["ti"].xcom_pull(task_ids="load_minio_raw_data")

    # Clans info
    if raw_clans_dict is not None:
        for clan_tag, raw_df in raw_clans_dict.items():
            # Table clan
            tag = clan_tag
            name = raw_df['name']
            members_count = raw_df['members']
            war_wins_count = raw_df['warWins']
            clan_level = raw_df['clanLevel']
            clan_points = raw_df['clanPoints']
            break

    # Players info
    if raw_players_dict is not None:
        achievements = {}  # id, name
        player_achievements = {}
        for clan_tag, raw_df in raw_players_dict.items():
            print(raw_df.columns)
            for _, player in raw_df.iterrows():
                # Table player
                tag = player['tag']
                tag_clan = clan_tag
                name = player['name']
                town_hall_level = player['townHallLevel']
                id_league = player['league']
                
                # Table player_achievement
                player_achievements[tag] = {
                    'id_achievement': [],
                    'progress': []
                }
                for achievement in player['achievements']:
                    achievement_name = achievement['name']
                    id_achievement = hashlib.sha256(str(achievement_name).encode()).hexdigest()
                    progress = achievement['value']  # это progress?
                    
                    player_achievements[tag]['id_achievement'].append(id_achievement)
                    player_achievements[tag]['progress'].append(progress)
                    # Table Achievement
                    achievements[id_achievement] = achievement_name

                # Table player_camp

                # Table item

                # Table league

                break
            break
        


def norm_minio_raw_data(**context):
    pass
