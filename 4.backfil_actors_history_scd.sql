INSERT INTO actors_history_scd
with streak_started as (select actor,
                               actorid,
                               quality_class,
                               is_active,
                               LAG(quality_class, 1) OVER (PARTITION BY actorid ORDER BY current_year) <> quality_class
                                   or
                               LAG(is_active, 1) OVER (PARTITION BY actorid ORDER BY current_year) is null as did_change,
                               current_year
                        from actors
                        WHERE current_year <= 2021),
     streak_identified AS (select actor,
                                  actorid,
                                  quality_class,
                                  is_active,
                                  sum(CASE WHEN did_change THEN 1 else 0 END)
                                  over (partition by actorid order by current_year) as streak_identifier,
                                  current_year
                           from streak_started),
     aggregated as (select actor,
                           actorid,
                           quality_class,
                           is_active,
                           min(current_year) as start_date,
                           max(current_year) as end_date,
                           2021              AS current_year
                    from streak_identified
                    group by 1, 2, 3, 4
                    order by 1)

select actor, actorid, quality_class, is_active, start_date, end_date, current_year
from aggregated
order by actor, start_date;