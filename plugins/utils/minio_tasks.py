import logging
import json
import os
import io

from plugins.utils import get_location_info, get_top_n_clans, get_clan_info, get_player_info
from plugins.hooks import MinioHook

def get_raw_data(
    country_code: str = "RU",
    top_n_clans: int = 1,
    **context
):
    location_info = get_location_info(country_code)
    if location_info is None:
        raise ValueError(f"cannot find info about specified country using this code: {country_code}")

    top_clans = get_top_n_clans(location_info[0], limit = top_n_clans)

    top_clans_info = {}
    top_clans_member_info = {}
    for clan in top_clans:
        clan_tag = clan["tag"]
        clan_info = get_clan_info(clan_tag)

        top_clans_info[clan_tag] = clan_info
        top_clans_member_info[clan_tag] = []

        for member in clan_info["memberList"]:
            member_tag = member["tag"]
            member_info = get_player_info(member_tag)
            top_clans_member_info[clan_tag].append({member_tag: member_info})

        logging.info(f"sucessfully get info about clan with tag {clan_tag}")

    return top_clans_info, top_clans_member_info

def save_raw_data(
    **context
):
    # create hook
    hook = MinioHook()
    # Get data from previous task
    top_clans_info, top_clans_member_info = context["ti"].xcom_pull(task_ids="get_raw_data")

    for clan_tag, clan_info in top_clans_info.items():
        json_bytes = json.dumps(clan_info, indent=2).encode("utf-8")
        buffer = io.BytesIO(json_bytes)
        
        # save raw data in minio
        try:
            filename = f"{clan_tag}/{context['dag_run'].start_date}/clan_info.json"
            hook.upload_bytes("raw-data", object_name = filename, data = buffer, length=len(json_bytes), content_type='application/json')

            for member in top_clans_member_info[clan_tag]:
                member_tag, member_info = next(iter(member.items()))

                json_bytes = json.dumps(member_info, indent=2).encode("utf-8")
                buffer = io.BytesIO(json_bytes)

                filename = f"{clan_tag}/{context['dag_run'].start_date}/member_{member_tag}_info.json"
                hook.upload_bytes("raw-data", object_name = filename, data = buffer, length=len(json_bytes), content_type='application/json')   

            logging.info(f"sucessfully upload info about clan with tag {clan_tag}")
        except Exception as e:
            raise RuntimeError(f"cannot upload data to minio! error: {e}")

def process_raw_data(
    top_clans_info,
    top_clans_member_info,
    **context
):
    # Get data from previous task
    #top_clans_info, top_clans_member_info = context["ti"].xcom_pull(task_ids="get_raw_data")
    ...

def save_processed_data(
    **context
):
    # Get data from previous task
    #top_clans_info, top_clans_member_info = context["ti"].xcom_pull(task_ids="process_raw_data")
    ...

#if __name__ == "__main__":
#    top_clans_info, top_clans_member_info = get_raw_data()
#    top_clans_info, top_clans_member_info = process_raw_data(top_clans_info, top_clans_member_info)