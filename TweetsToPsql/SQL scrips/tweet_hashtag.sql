CREATE TABLE public.tweet_hashtag
(
    tweet_id integer NOT NULL,
    hashtag_id integer NOT NULL,
    CONSTRAINT tweet_hashtag_pkey PRIMARY KEY (tweet_id, hashtag_id)
);
