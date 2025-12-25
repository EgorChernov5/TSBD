DROP TABLE IF EXISTS mart_clan_activity;

CREATE TABLE mart_clan_activity AS
SELECT
    c.tag AS clan_tag,
    c.name AS clan_name,
    c.start_date,
    c.war_wins,
    c.clan_points
FROM clan c
WHERE c.end_date = '9999-12-31'
ORDER BY start_date, clan_tag;
