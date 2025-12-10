from plugins.utils.parse_coc_wiki import scrape_troop_images, upload_icons_to_minio
from plugins.utils.clash_of_clans_api import get_location_info, get_top_n_clans, get_clan_info, get_player_info
from plugins.utils.minio_tasks import get_raw_data, save_raw_data, process_raw_data, save_processed_data
from plugins.utils.speed import fetch_metrics