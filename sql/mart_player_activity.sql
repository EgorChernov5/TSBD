DROP TABLE IF EXISTS mart_player_activity;

CREATE TABLE mart_player_activity AS
SELECT
    p.tag AS player_tag,
    p.name AS player_name,
    pa.achievement_id,
    a.name AS achievement_name,
    p.start_date,
    pa.progress
FROM player_achievement pa
JOIN player p ON pa.player_id = p.tag
JOIN achievement a ON pa.achievement_id = a.id
WHERE p.end_date = '9999-12-31'
ORDER BY p.start_date, player_tag;
