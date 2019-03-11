-- Postgres schema for audience-count

-- Types

CREATE TYPE public.viewcount_method AS ENUM (
    'ip',
    'uid',
    'ip+uid',
    'live'
);
COMMENT ON TYPE public.viewcount_method IS 'methods of calculating unique viewers';

CREATE TYPE public.viewcount_source AS ENUM (
    'icecast',
    'rtmp',
    'youtube',
    'hls'
);

-- Tables

CREATE TABLE public.hls_pl_loads (
    "timestamp" timestamp with time zone NOT NULL,
    ip character varying(45) NOT NULL,
    uid bigint NOT NULL,
    stream character varying(10) NOT NULL
);
ALTER TABLE ONLY public.hls_pl_loads
    ADD CONSTRAINT hls_pl_loads_pkey PRIMARY KEY ("timestamp", ip, uid, stream);

CREATE TABLE public.viewcount (
    "timestamp" timestamp with time zone DEFAULT now() NOT NULL,
    source public.viewcount_source NOT NULL,
    stream character varying(16) NOT NULL,
    count smallint NOT NULL,
    method public.viewcount_method NOT NULL
);
ALTER TABLE ONLY public.viewcount
    ADD CONSTRAINT viewcount_pkey PRIMARY KEY ("timestamp", source, stream, method);

CREATE TABLE public.viewers_latest (
    source public.viewcount_source NOT NULL,
    stream character varying(16) NOT NULL,
    method public.viewcount_method NOT NULL,
    "timestamp" timestamp with time zone DEFAULT now() NOT NULL,
    count smallint NOT NULL
);
ALTER TABLE ONLY public.viewers_latest
    ADD CONSTRAINT viewers_latest_pk PRIMARY KEY (source, stream, method);
