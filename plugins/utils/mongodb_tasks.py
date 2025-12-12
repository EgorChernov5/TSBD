import logging

import pandas as pd

from plugins.hooks import MongoDBHook

# ------------------
# tasks before mongodb
# ------------------

def save_mongo_json(
        hook, data: dict, 
        object_path: str
):
    hook.insert(
        collection="raw_data",
        documents={
            "path": object_path,
            "data": data
        }
    )

def save_mongodb_raw_data(
        **context
):
    # create hook
    hook = MongoDBHook()
    # Get data from previous task
    top_clans_info, top_clans_member_info = context["ti"].xcom_pull(task_ids="preprocess_raw_data")

    for clan_tag, clan_info in top_clans_info.items():
        try:
            # Save clan info
            path = f"{clan_tag}/{context['dag_run'].start_date}/clan_info.json"
            save_mongo_json(hook, clan_info, path)

            # Save member info
            for member in top_clans_member_info[clan_tag]:
                member_tag, member_info = next(iter(member.items()))
                path = f"{clan_tag}/{context['dag_run'].start_date}/member_{member_tag}_info.json"
                save_mongo_json(hook, member_info, path)

            logging.info(f"Successfully processed data for clan {clan_tag}")

        except Exception as e:
            raise RuntimeError(f"Cannot upload data to MongoDB! Error: {e}")

# ------------------
# tasks after mongodb
# ------------------

def load_mongodb_raw_data(
        **context
):
    # create hook
    hook = MongoDBHook()
    collection = "raw_data"

    out = {}

    # list all clan tags
    clans = hook.list_prefixes(prefix="", collection=collection)

    for clan_tag in clans:
        logging.info(f"Loading MongoDB data for clan {clan_tag}")

        # list all date prefixes for clan
        date_prefixes = hook.list_prefixes(
            prefix=f"{clan_tag}/",
            collection=collection
        )
        if not date_prefixes:
            continue

        # extract date only → pick latest
        dates_only = [p.split("/", 1)[1] for p in date_prefixes]
        latest_date = max(dates_only)

        # get all member documents under latest date
        prefix = f"{clan_tag}/{latest_date}/"
        objects = hook.list_objects(prefix=prefix, collection=collection)

        dfs = []
        for key in objects:
            filename = key.split("/")[-1]

            # load only member info
            if filename.startswith("member"):
                docs = hook.find(
                    collection=collection,
                    query={"path": key},
                    projection={"_id": 0, "data": 1}
                )
                if docs:
                    dfs.append(pd.DataFrame([docs[0]["data"]]))

        if dfs:
            out[clan_tag] = pd.concat(dfs, ignore_index=True)

    return out

def postprocess_mongodb_raw_data(**context):
    # get data from mongodb
    raw_dict = context["ti"].xcom_pull(task_ids="load_mongodb_raw_data")

    results = {}
    for clan_tag, df in raw_dict.items():
        best = df.sort_values("trophies", ascending=False).iloc[0]
        results[clan_tag] = best[["name", "trophies"]].to_dict()
        logging.info(f"Postprocessed clan {clan_tag}")

    return results
