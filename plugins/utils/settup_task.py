def presettup(**context):
    context["ti"].xcom_push(
        key='table_names',
        value=['clan', 'player', 'league', 'achievement', 'player_achievement', 'player_camp', 'item']
    )

    # Table clan
    context["ti"].xcom_push(
        key="clan_target_fields",
        value=['tag', 'name', 'members_count', 'war_wins', 'clan_level', 'clan_points']
    )
    context["ti"].xcom_push(key="clan_keys", value=['tag'])
    # Table player
    context["ti"].xcom_push(
        key="player_target_fields",
        value=['tag', 'name', 'townhall_level', 'clan_id', 'league_id']
    )
    context["ti"].xcom_push(key="player_keys", value=['tag'])
    # Table league
    context["ti"].xcom_push(
        key="league_target_fields",
        value=['id', 'name']
    )
    context["ti"].xcom_push(key="league_keys", value=['id'])
    # Table achievement
    context["ti"].xcom_push(
        key="achievement_target_fields",
        value=['id', 'name', 'max_progress']
    )
    context["ti"].xcom_push(key="achievement_keys", value=['id'])
    # Table player_achievement
    context["ti"].xcom_push(
        key="player_achievement_target_fields",
        value=['achievement_id', 'player_id', 'progress']
    )
    context["ti"].xcom_push(key="player_achievement_keys", value=['achievement_id', 'player_id'])
    # Table player_camp
    context["ti"].xcom_push(
        key="player_camp_target_fields",
        value=['player_id', 'item_id', 'level', 'icon_link']
    )
    context["ti"].xcom_push(key="player_camp_keys", value=['player_id', 'item_id'])
    # Table item
    context["ti"].xcom_push(
        key="item_target_fields",
        value=['id', 'name', 'type', 'village', 'max_level']
    )
    context["ti"].xcom_push(key="item_keys", value=['id'])
