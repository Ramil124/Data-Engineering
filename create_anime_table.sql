-- Anime table needed to ingest data

CREATE TABLE IF NOT EXISTS anime(
    mal_id INTEGER,
    title_english TEXT,
    type TEXT,
    status TEXT,
    synopsis TEXT, 
    episodes INTEGER,
    duration_minutes INTEGER,
    rank  NUMERIC,
    score NUMERIC,
    rating TEXT,
    broadcast_weekday TEXT,
    broadcast_time TEXT,
    from_date DATE

)
