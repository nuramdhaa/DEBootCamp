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