-- SELECT * FROM player_seasons;
--
-- CREATE TYPE season_stats AS (
-- season INT,
--     gp INT,
--     pts REAL,
--     reb REAL,
--     ast REAL
-- );
--
-- CREATE TYPE scoring_class AS ENUM ('star', 'good', 'average', 'bad');
-- CREATE TABLE players (
--     player_name TEXT,
--     height TEXT,
--     college TEXT,
--     country TEXT,
--     draft_year TEXT,
--     draft_number TEXT,
--     season_stats season_stats[],
--     scoring_class scoring_class,
--     year_since_last_season INT,
--     current_season INT,
--     is_active BOOLEAN,
--     PRIMARY KEY(player_name, current_season)
-- );
-- DROP TABLE players;
-- SELECT MIN(season) FROM player_seasons;


--         ALTER TABLE players
--         DROP CONSTRAINT players_pkey;
-- --
-- DO
-- $$
--     DECLARE
--         y INT;
--         t INT;
--     BEGIN
--         FOR y IN 1995..2022
--             LOOP
--                 t := y + 1;
--
--                 INSERT INTO players
--                 WITH yesterday AS (SELECT *
--                                    FROM players
--                                    WHERE current_season = y),
--                      today AS (SELECT *
--                                FROM player_seasons
--                                WHERE season = t)
--
--                 SELECT COALESCE(t.player_name, y.player_name)   AS player_name,
--                        COALESCE(t.height, y.height)             AS height,
--                        COALESCE(t.college, y.college)           AS college,
--                        COALESCE(t.country, y.country)           AS country,
--                        COALESCE(t.draft_year, y.draft_year)     AS draft_year,
--                        COALESCE(t.draft_number, y.draft_number) AS draft_number,
--                        CASE
--                            WHEN y.season_stats IS NULL
--                                THEN ARRAY [ROW (
--                                t.season,
--                                t.gp,
--                                t.pts,
--                                t.reb,
--                                t.ast)::season_stats]
--                            WHEN t.season IS NOT NULL THEN y.season_stats || ARRAY [ROW (
--                                t.season,
--                                t.gp,
--                                t.pts,
--                                t.reb,
--                                t.ast)::season_stats]
--                            ELSE y.season_stats
--                            END                                  AS season_stats,
--                        CASE
--                            WHEN t.season IS NOT NULL THEN
--                                (CASE
--                                     WHEN t.pts > 20 THEN 'star'
--                                     WHEN t.pts > 15 THEN 'good'
--                                     WHEN t.pts > 20 THEN 'average'
--                                     ELSE 'bad' END)::scoring_class
--                            ELSE y.scoring_class
--                            END                                  AS scoring_class,
--                        CASE
--                            WHEN t.season IS NOT NULL THEN 0
--                            ELSE y.year_since_last_season + 1
--                            END                                  AS years_since_last_season,
--                        COALESCE(t.season, y.current_season + 1) as current_season,
--                        t.season IS NOT NULL                     AS is_active
--
--                 FROM today t
--                          FULL OUTER JOIN yesterday y
--                                          ON t.player_name = y.player_name;
--             END LOOP;
--     END
-- $$;
-- SELECT * FROM players;

-- SELECT
--     player_name,
--     (season_stats[CARDINALITY(season_stats)]::season_stats).pts/
--     CASE WHEN (season_stats[1]::season_stats).pts = 0 THEN 1 ELSE (season_stats[1]::season_stats).pts END
--
--     FROM players WHERE current_season = 2001 AND player_name = 'Don MacLean'
-- ORDER BY 2 DESC;

-- WITH unnested AS (
-- SELECT player_name,
--        UNNEST(season_stats)::season_stats AS season_stats
-- FROM players
--          WHERE player_name = 'Michael Jordan'
--            AND current_season= 2001
-- )
-- SELECT player_name,
--       (season_stats::season_stats).*
-- FROM unnested;


--    loopnya bisa juga pakai:
--     WITH years AS (
--         SELECT *
--         FROM generate_series(1996,2022) as season
--     ),
--

-- CREATE TABLE players_scd (
--     player_name TEXT,
--     scoring_class scoring_class,
--     is_active BOOLEAN,
--     current_season INT,
--     start_season INT,
--     end_season INT,
--     PRIMARY KEY(player_name, current_season)
-- );


-- drop table players_scd;

WITH streak_started AS (
SELECT player_name,
       scoring_class,
       is_active,
       current_season,
       LAG(scoring_class, 1) OVER (PARTITION BY player_name ORDER BY current_season) <> scoring_class
           OR LAG(scoring_class,1) OVER (PARTITION BY player_name ORDER BY current_season) IS NULL AS did_change
    FROM players
), streak_identified AS (SELECT player_name,
                                scoring_class,
                                current_season,
                                is_active,
                                SUM(CASE WHEN did_change THEN 1 ELSE 0 END)
                                OVER (PARTITION BY player_name ORDER BY current_season) as streak_identifier
                         FROM streak_started),
    aggregated AS (
        SELECT player_name,
               scoring_class,
               streak_identifier,
               MIN(current_season) AS start_date,
               MAX(current_season) AS end_date
        FROM streak_identified
        GROUP BY player_name, scoring_class, streak_identifier)
    SELECT player_name, scoring_class, start_date, end_date
        FROM aggregated;


CREATE TYPE scd_type AS (
                    scoring_class scoring_class,
                    is_active boolean,
                    start_season INTEGER,
                    end_season INTEGER
                        );


WITH last_season_scd AS (
    SELECT * FROM players_scd
    WHERE current_season = 2021
    AND end_season = 2021
),
     historical_scd AS (
        SELECT
            player_name,
               scoring_class,
               is_active,
               start_season,
               end_season
        FROM players_scd
        WHERE current_season = 2021
        AND end_season < 2021
     ),
     this_season_data AS (
         SELECT * FROM players
         WHERE current_season = 2022
     ),
     unchanged_records AS (
         SELECT
                ts.player_name,
                ts.scoring_class,
                ts.is_active,
                ls.start_season,
                ts.current_season as end_season
        FROM this_season_data ts
        JOIN last_season_scd ls
        ON ls.player_name = ts.player_name
         WHERE ts.scoring_class = ls.scoring_class
         AND ts.is_active = ls.is_active
     ),
     changed_records AS (
        SELECT
                ts.player_name,
                UNNEST(ARRAY[
                    ROW(
                        ls.scoring_class,
                        ls.is_active,
                        ls.start_season,
                        ls.end_season

                        )::scd_type,
                    ROW(
                        ts.scoring_class,
                        ts.is_active,
                        ts.current_season,
                        ts.current_season
                        )::scd_type
                ]) as records
        FROM this_season_data ts
        LEFT JOIN last_season_scd ls
        ON ls.player_name = ts.player_name
         WHERE (ts.scoring_class <> ls.scoring_class
          OR ts.is_active <> ls.is_active)
     ),
     unnested_changed_records AS (

         SELECT player_name,
                (records::scd_type).scoring_class,
                (records::scd_type).is_active,
                (records::scd_type).start_season,
                (records::scd_type).end_season
                FROM changed_records
         ),
     new_records AS (

         SELECT
            ts.player_name,
                ts.scoring_class,
                ts.is_active,
                ts.current_season AS start_season,
                ts.current_season AS end_season
         FROM this_season_data ts
         LEFT JOIN last_season_scd ls
             ON ts.player_name = ls.player_name
         WHERE ls.player_name IS NULL

     )


SELECT *, 2022 AS current_season FROM (
     SELECT * FROM historical_scd
                  UNION ALL

SELECT * FROM unchanged_records
                UNION ALL

SELECT * FROM unnested_changed_records
                UNION ALL

SELECT * FROM new_records) AS last_season_scd;