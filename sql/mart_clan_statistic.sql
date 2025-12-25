DROP TABLE IF EXISTS mart_clan_stats;

CREATE TABLE mart_clan_stats AS
SELECT
    c.tag AS clan_tag,
    i.name AS troop_name,
    i.type AS troop_type,
    i.village AS village,
    ROUND(AVG(pc.level), 2) AS avg_level,
    MAX(pc.level) AS max_level,
    MIN(pc.level) AS min_level,
    COUNT(pc.player_id) AS players_count
FROM player_camp pc
JOIN player p ON pc.player_id = p.tag
JOIN clan c ON p.clan_id = c.tag
JOIN item i ON pc.item_id = i.id
WHERE p.end_date = '9999-12-31'
GROUP BY clan_tag, troop_name, troop_type, village
ORDER BY clan_tag, troop_name;
