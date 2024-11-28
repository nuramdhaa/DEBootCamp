create type films as (
    film text,
    votes real,
    rating real,
    filmid text);

create type quality_class as enum ('star', 'good', 'average', 'bad');

   create table actors (
        actor text,
        actorid text,
        films films[],
         current_year integer,
        is_active boolean,
       quality_class quality_class,
        primary key (actorid, current_year)
    );

DO $$
DECLARE
   y INT;
   t INT;
BEGIN
   FOR y IN 1969..2021 LOOP
       t := y + 1;

INSERT INTO actors
 WITH yesterday AS (
           SELECT *
           FROM actors
           WHERE current_year = y
       ),
       today AS (
           SELECT
               actor,
               actorid,
               ARRAY_AGG(ROW(film, votes, rating, filmid)::films) AS films,
               MAX(year) AS max_year,
               AVG(rating) AS avg_rating
           FROM actor_films
           WHERE year = t
           GROUP BY actor, actorid
       )
       SELECT
           COALESCE(t.actor, y.actor) AS actor,
           COALESCE(t.actorid, y.actorid) AS actorid,
           CASE
               WHEN y.films IS NULL THEN t.films
               WHEN t.films IS NOT NULL THEN y.films || t.films
               ELSE y.films
           END AS films,
           COALESCE(t.max_year, y.current_year + 1) AS current_year,
           CASE
               WHEN t.actor IS NOT NULL THEN TRUE
               ELSE FALSE
           END AS is_active,
           CASE
               WHEN t.max_year IS NOT NULL THEN
                   CASE
                       WHEN t.avg_rating > 8 THEN 'star'
                       WHEN t.avg_rating > 7 THEN 'good'
                       WHEN t.avg_rating > 6 THEN 'average'
                       ELSE 'bad'
                   END::quality_class
               ELSE y.quality_class
           END AS quality_class
       FROM today t
       FULL OUTER JOIN yesterday y ON t.actorid = y.actorid;
   END LOOP;
END; $$;

select actor,
       actorid,
       current_year,
       is_active
from actors
order by actor;

CREATE TABLE actors_history_scd
(
    actor         text,
    actorid       text,
    quality_class quality_class,
    is_active     boolean,
    start_date    integer,
    end_date      integer,
     current_year  int,
    primary key (actorid, start_date)
);

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
where actor ='Charlie Murphy'
order by actor, start_date;