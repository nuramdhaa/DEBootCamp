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



