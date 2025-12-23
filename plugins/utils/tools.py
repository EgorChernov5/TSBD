import hashlib
from typing import List, Tuple, Dict, Any, Set
from datetime import date


def parse_achievements(player, player_achievements: List[Dict], achievements: List[Dict]):
    tag_player = player['tag']
    for achievement in player['achievements']:
        achievement_name = achievement['name']
        id_achievement = hashlib.sha256(achievement_name.encode()).hexdigest()
        stars = achievement['stars']

        # Table player_achievement
        player_achievements.append({
            'achievement_id': id_achievement,
            'player_id': tag_player,
            'progress': stars
        })
        # Table achievement
        achievements.append({
            'id': id_achievement,
            'name': achievement_name,
            'max_progress': 3
        })


def parse_items(player, item_type: str, items: List[Dict], player_camps: List[Dict]):
    tag_player = player['tag']
    for camp_item in player[item_type]:
        item_name = camp_item['name']
        id_item = hashlib.sha256(item_name.encode()).hexdigest()
        level = camp_item['level']

        # Table player_camp
        player_camps.append({
            'player_id': tag_player,
            'item_id': id_item,
            'level': level,
            'icon_link': 'icon_link'  # TODO: how to add?
        })
        # Table item
        items.append({
            'id': id_item,
            'name': item_name,
            'type': item_type,
            'village': camp_item['village'],
            'max_level': camp_item['maxLevel']
        })


def delete_duplicates(data_rows: List[Dict], keys: List[str]) -> List[Dict]:
    seen = set()
    unique_data: List[Dict] = []
    for data_row in data_rows:
        # формируем кортеж значений по ключам
        key_values = tuple(data_row[k] for k in keys)
        if key_values not in seen:
            seen.add(key_values)
            unique_data.append(data_row)
    
    return unique_data


def order_list(ref_order, cur_order, dfs):
    # Создаём словарь с индексами правильного порядка
    order_index = {key: i for i, key in enumerate(ref_order)}
    # Сортировка датафреймов
    sorted_dfs = [
        df for _, df in sorted(
            zip(cur_order, dfs),
            key=lambda x: order_index[x[0]]
        )
    ]
    return sorted_dfs


def apply_scd(
        hook,
        table_name: str,
        columns: Tuple[str, ...],
        new_rows: List[Tuple],
        business_key_cols: List[str],
        tracked_cols: List[str],  # атрибуты, изменения которых создают новую версию
        end_date_sentinel: date = date(9999, 12, 31)
    ):
    """
    Применяет SCD Type 2 к таблице в PostgreSQL.
    
    :param hook: PostgresDataHook
    :param table_name: имя таблицы измерения
    :param columns: названия колонок в new_rows
    :param new_rows: новые данные как список кортежей
    :param business_key_cols: список колонок — business key
    :param tracked_cols: колонки, изменения в которых порождают новую версию
    :param end_date_sentinel: метка для "актуальных" записей (обычно 9999-12-31)
    """
    # === 1. Преобразуем входные данные в список словарей для удобства ===
    new_records = [dict(zip(columns, row)) for row in new_rows]

    # # Извлекаем значения business key для запроса
    bk_values = [tuple(rec[col] for col in business_key_cols) for rec in new_records]
    
    # Уникальные business keys
    unique_bk_set: Set[Tuple] = set(bk_values)

    # # === 2. Получаем текущие активные версии из БД ===
    # current_rows, bk_col_list = hook.find_changes(table_name, business_key_cols, tracked_cols, unique_bk_set)

    # # Индексируем текущие записи по business key
    # current_by_bk: Dict[Tuple, Dict[str, Any]] = {}
    # for row in current_rows:
    #     bk = tuple(row[:len(business_key_cols)])
    #     tracked_vals = row[len(business_key_cols):-2]  # до customer_sk и version
    #     customer_sk = row[-2]
    #     version = row[-1]
    #     current_by_bk[bk] = {
    #         'tracked_vals': tracked_vals,
    #         'customer_sk': customer_sk,
    #         'version': version
    #     }

    # # === 3. Определяем, какие записи изменились или новые ===
    # changed_or_new = []
    # today = date.today()

    # for rec in new_records:
    #     bk = tuple(rec[col] for col in business_key_cols)
    #     new_tracked_vals = tuple(rec[col] for col in tracked_cols)

    #     current = current_by_bk.get(bk)

    #     if current is None:
    #         # Новая запись
    #         changed_or_new.append((rec, 1))
    #     else:
    #         # Сравниваем значения (None == None считается равенством)
    #         current_vals = current['tracked_vals']
    #         if new_tracked_vals != current_vals:
    #             changed_or_new.append((rec, current['version'] + 1))

    # if not changed_or_new:
    #     return  # Нет изменений

    # # === 4. Закрываем текущие версии (UPDATE) ===
    # hook.update_sqd(
    #     table_name,
    #     business_key_cols,
    #     tracked_cols,
    #     bk_col_list,
    #     current_by_bk,
    #     changed_or_new,
    #     today,
    #     end_date_sentinel
    # )
