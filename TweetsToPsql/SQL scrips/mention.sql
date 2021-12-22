CREATE TABLE public.mention
(
    id bigint NOT NULL,
    tweet_id bigint NOT NULL,
    username character varying (50),
    is_reply boolean,
    PRIMARY KEY (id, tweet_id)
);

ALTER TABLE public.mention
    OWNER to postgres;