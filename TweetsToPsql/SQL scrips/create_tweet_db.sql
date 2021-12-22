CREATE TABLE public.tweet
(
    id SERIAL NOT NULL,
    twitter_id bigint NOT NULL,
    user_id bigint NOT NULL,
    full_text character varying(800),
    created_at date,
    src character varying(80),
    in_reply_twitter_id bigint,
    geo_id character varying(20),
    retweet_cnt integer,
    favorited_cnt integer,
    replies integer,
    quotes integer,
    lang character varying(4),
    is_sensitive boolean,
    PRIMARY KEY (id),
    CONSTRAINT tweet_id_constraint UNIQUE (twitter_id)
);

ALTER TABLE public.tweet
    OWNER to postgres;