/* pg_plan_inspector/pg_query_plan--1.0.sql */

CREATE OR REPLACE FUNCTION public.pg_query_plan(
       IN  pid          INT,
       OUT pid          INT,
       OUT database     TEXT,
       OUT worker_type  TEXT,
       OUT nested_level INT,
       OUT queryid      TEXT,
       OUT query_start  TIMESTAMP WITH TIME ZONE,
       OUT query        TEXT,
       OUT plan         TEXT,
       OUT plan_json    TEXT
   )
     RETURNS SETOF record
     AS 'pg_query_plan'
LANGUAGE C;
