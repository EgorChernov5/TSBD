-- League (SCD 1)
CREATE TABLE league (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name        TEXT NOT NULL
);

-- Clan (SCD 2)
CREATE TABLE clan (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tag VARCHAR(50) NOT NULL,
    name TEXT,
    members_count INTEGER,
    war_wins INTEGER,
    clan_level INTEGER,
    clan_points INTEGER,
    start_date DATE NOT NULL,
    end_date DATE,
    UNIQUE (tag, start_date)
);

-- Player (SCD 2)
CREATE TABLE player (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tag VARCHAR(50) NOT NULL,
    name TEXT,
    townhall_level INTEGER,
    clan_id UUID,
    league_id UUID,
    start_date DATE NOT NULL,
    end_date DATE,
    UNIQUE (tag, start_date),
    FOREIGN KEY (clan_id) REFERENCES clan(id),
    FOREIGN KEY (league_id) REFERENCES league(id)
);

-- Achievement (SCD 1)
CREATE TABLE achievement (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            TEXT NOT NULL,
    max_progress    INTEGER
);

-- Item (SCD 1)
CREATE TABLE item (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name        TEXT NOT NULL,
    type        TEXT,
    village     TEXT,
    max_level   INTEGER
);

-- Player Achievements (SCD 1)
CREATE TABLE player_achievement (
    achievement_id     UUID NOT NULL,
    player_id          UUID NOT NULL,
    progress           INTEGER,
    PRIMARY KEY (achievement_id, player_id),
    FOREIGN KEY (achievement_id) REFERENCES achievement(id),
    FOREIGN KEY (player_id) REFERENCES player(id)
);

-- Player Camp (SCD 1)
CREATE TABLE player_camp (
    player_id      UUID NOT NULL,
    item_id        UUID NOT NULL,
    level          INTEGER,
    icon_link      TEXT,
    PRIMARY KEY (player_id, item_id),
    FOREIGN KEY (player_id) REFERENCES player(id),
    FOREIGN KEY (item_id) REFERENCES item(id)
);
