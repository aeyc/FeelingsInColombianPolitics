CREATE TABLE public.hashtag
(
    id serial NOT NULL,
    tag character varying(240) NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT tag_constraint UNIQUE (tag)
);

ALTER TABLE public.hashtag
    OWNER to postgres;