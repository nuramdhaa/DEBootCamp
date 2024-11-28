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