\echo Use "CREATE EXTENSION pgpi" to load this file. \quit

CREATE FUNCTION pgpi_requests(
    OUT seq bigint, OUT route text, OUT point text, OUT rels text,
    OUT outer_rels text, OUT pg_value float8, OUT answer float8,
    OUT source text, OUT applied boolean, OUT request text, OUT query text)
RETURNS SETOF record
AS 'MODULE_PATHNAME' LANGUAGE C VOLATILE;

CREATE FUNCTION pgpi_clear() RETURNS void
AS 'MODULE_PATHNAME' LANGUAGE C VOLATILE;

CREATE FUNCTION pgpi_status(
    OUT target text, OUT installed boolean, OUT hits bigint, OUT note text)
RETURNS SETOF record
AS 'MODULE_PATHNAME' LANGUAGE C VOLATILE;

CREATE FUNCTION pgpi_log_status(
    OUT ring_size bigint, OUT ring_used bigint, OUT written bigint,
    OUT dropped bigint, OUT worker_pid int, OUT stats_versions bigint,
    OUT log_plans boolean)
RETURNS record
AS 'MODULE_PATHNAME' LANGUAGE C VOLATILE;

-- wait until the log writer has stored everything logged so far
CREATE FUNCTION pgpi_log_flush(timeout_ms int DEFAULT 10000) RETURNS boolean
AS 'MODULE_PATHNAME' LANGUAGE C VOLATILE;

-- forget which statistics versions were already captured, so that they are
-- captured again (use this after deleting rows from pgpi.stats by hand)
CREATE FUNCTION pgpi_stats_reset_seen() RETURNS void
AS 'MODULE_PATHNAME' LANGUAGE C VOLATILE;
REVOKE ALL ON FUNCTION pgpi_stats_reset_seen() FROM PUBLIC;
