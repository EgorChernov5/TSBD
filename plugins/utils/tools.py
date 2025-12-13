import hashlib


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
