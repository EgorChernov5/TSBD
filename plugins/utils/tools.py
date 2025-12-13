import hashlib
from typing import List, Tuple, Dict, Any, Set
from datetime import date


# TODO: достижения у пользователя могут повторяться
def parse_achievements(player, player_achievements, achievements):
    tag = player['tag']
    player_achievements[tag] = {
        'id_achievement': [],
        'stars': []
    }
    for achievement in player['achievements']:
        achievement_name = achievement['name']
        id_achievement = hashlib.sha256(achievement_name.encode()).hexdigest()
        stars = achievement['stars']
        
        player_achievements[tag]['id_achievement'].append(id_achievement)
        player_achievements[tag]['stars'].append(stars)
        # Table achievement
        achievements[id_achievement] = {
            'name': achievement_name,
            'max_starts': 3
        }


# TODO: add icon_link
def parse_items(player, item_type, items, player_camps):
    tag = player['tag']
    player_camps[tag] = {
        'id_item': [],
        'level': [],
        'icon_link': []
    }
    for camp_item in player[item_type]:
        camp_item_name = camp_item['name']
        id_camp_item = hashlib.sha256(camp_item_name.encode()).hexdigest()
        level = camp_item['level']

        player_camps[tag]['id_item'].append(id_camp_item)
        player_camps[tag]['level'].append(level)
        # player_camps[tag]['icon_link'].append(stars)  # how to add?
        
        # Table item
        village = camp_item['village']
        max_level = camp_item['maxLevel']
        items[id_camp_item] = {
            'name': camp_item_name,
            'item_type': item_type,
            'village': village,
            'max_level': max_level
        }


def delete_duplicates(rows, primary_keys):
    header = rows[0]
    data_rows = rows[1:]

    # Проверим, что все primary_keys есть в заголовке
    header_list = list(header)
    for pk in primary_keys:
        if pk not in header_list:
            raise ValueError(f"Primary key '{pk}' not found in header: {header}")

    # Получаем индексы primary keys
    pk_indices = [header_list.index(pk) for pk in primary_keys]

    seen = set()
    unique_rows = [header]  # Начинаем с заголовка

    for row in data_rows:
        # Извлекаем значения по индексам primary keys
        pk_values = tuple(row[i] for i in pk_indices)
        if pk_values not in seen:
            seen.add(pk_values)
            unique_rows.append(row)

    return unique_rows


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
