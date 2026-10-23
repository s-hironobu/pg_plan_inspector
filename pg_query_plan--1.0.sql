/* pg_plan_inspector/pg_query_plan--1.0.sql */

-- The return type was changed (planid was added), so drop the old one first.
DROP FUNCTION IF EXISTS public.pg_query_plan(INT);

CREATE OR REPLACE FUNCTION public.pg_query_plan(
       IN  pid          INT,
       OUT pid          INT,
       OUT database     TEXT,
       OUT worker_type  TEXT,
       OUT nested_level INT,
       OUT queryid      TEXT,
       OUT query_start  TIMESTAMP WITH TIME ZONE,
       OUT query        TEXT,
       OUT planid       TEXT,
       OUT plan         TEXT,
       OUT plan_json    TEXT
   )
     RETURNS SETOF record
     AS 'pg_query_plan'
LANGUAGE C;
