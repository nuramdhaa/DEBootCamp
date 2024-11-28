CREATE TYPE actor_scd_type AS (
    quality_class quality_class,
    is_active boolean,
    start_date int,
    end_date int
     )

WITH last_year_actor_scd AS (SELECT *
                             FROM actors_history_scd
                             WHERE current_year = 2021
                               AND end_date = 2021),
     historical_actors_scd AS (SELECT actor,
                                      actorid,
                                      quality_class,
                                      is_active,
                                      start_date,
                                      end_date
                               FROM actors_history_scd
                               WHERE current_year = 2021
                                 AND end_date < 2021),
     this_year_actor AS (SELECT *
                         FROM actors
                         WHERE current_year = 2022),
     unchanged_records AS (SELECT ty.actor,
                                 ty.actorid,
                                 ty.quality_class,
                                 ty.is_active,
                                 ly.start_date,
                                 ty.current_year as end_date
                          FROM this_year_actor ty
                                   JOIN last_year_actor_scd ly
                                        ON ly.actorid = ty.actorid
                          WHERE ly.quality_class = ty.quality_class
                            AND ly.is_active = ty.is_active),
     changed_records AS (SELECT ty.actor,
                                ty.actorid,
                        UNNEST(
                         ARRAY[
                                    ROW(
                                        ly.quality_class,
                                        ly.is_active,
                                        ly.start_date,
                                        ly.end_date
                                        )::actor_scd_type,
                                    ROW(
                                        ty.quality_class,
                                        ty.is_active,
                                        ty.current_year,
                                        ty.current_year
                                        )::actor_scd_type
                                    ]) as records
          FROM this_year_actor ty
                                  LEFT JOIN last_year_actor_scd ly
                                            ON ly.actorid = ty.actorid
                         WHERE (ly.quality_class <> ty.quality_class
                             OR ly.is_active <> ty.is_active)),
    unnested_changed_records AS (
            SELECT actor,
            actorid,
            (records::actor_scd_type).quality_class,
            (records::actor_scd_type).is_active,
            (records::actor_scd_type).start_date,
            (records::actor_scd_type).end_date
             FROM changed_records),
    new_records AS (
        SELECT
ty.actor,
ty.actorid,
ty.quality_class,
ty.is_active,
ty.current_year as start_date,
ty.current_year as end_date
            FROM this_year_actor ty
                 LEFT JOIN last_year_actor_scd ly ON
            ty.actorid = ly.actorid
            WHERE ly.actorid IS NULL

    )

SELECT * FROM historical_actors_scd
    UNION ALL
    SELECT * FROM unchanged_records
                  UNION ALL
 SELECT * FROM unnested_changed_records
   UNION ALL
   SELECT * FROM new_records
