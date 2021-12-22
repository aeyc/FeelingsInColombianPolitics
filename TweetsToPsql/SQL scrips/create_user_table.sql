CREATE TABLE public."user"
(
    id integer NOT NULL,
    screen_name character varying(15) NOT NULL,
    followers integer,
    following integer,
    twitter_user_id integer,
    verified boolean,
    favourites_count integer,
    name character varying(50),
    PRIMARY KEY (id)
);

ALTER TABLE public."user"
    OWNER to postgres;