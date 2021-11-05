DROP TABLE IF EXISTS db_watch_test;

CREATE TABLE db_watch_test (
    id serial PRIMARY KEY,
    date timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    guid text NOT NULL,
    link text,
    feed text,
    props jsonb,
    tags jsonb
); 
