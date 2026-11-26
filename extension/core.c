/*-------------------------------------------------------------------------
 * core.c
 *	  The intervention core shared by route H (hooks) and route D (detours).
 *
 * A route asks a question (PgpiRequest): "how many rows does this
 * relation set produce, under these clauses, with this parameterization?"
 * The core turns the question into JSON and looks for an answer: first the
 * directives, then the external estimator server over a Unix socket. It logs
 * the exchange and returns the value that the planner should use, according
 * to pgpi.mode.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include "access/xact.h"
#include "miscadmin.h"
#include "optimizer/cost.h"
#include "optimizer/extendplan.h"
#include "optimizer/optimizer.h"
#include "parser/parsetree.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"

#include "pgpi.h"

/* ---------- per-planning state ---------- */

static MemoryContext plan_cxt = NULL;	/* reset at outermost planning start */
static int	plan_depth = 0;
static char *plan_query = NULL;

/*
 * Clauses evaluated inside a join relation are kept on the RelOptInfo itself
 * through the PostgreSQL 19 planner-extension state. They are allocated in the
 * same memory context as the rel. This matters for GEQO, which builds joinrels
 * in a temporary context and throws them away. A side table keyed by relids
 * would keep dangling pointers.
 */
typedef struct JoinrelClauses
{
	List	   *clauses;
} JoinrelClauses;

int			pgpi_ext_id = -1;

typedef struct CachedAnswer
{
	char	   *key;
	bool		has_answer;
	double		value;
	char	   *source;
} CachedAnswer;

static List *answer_cache = NIL;	/* of CachedAnswer *, in plan_cxt */

/* ---------- request log (per backend) ---------- */

typedef struct LogEntry
{
	int64		seq;
	char		route;
	PgpiPoint	point;
	char	   *rels;
	char	   *outer;
	double		pg_value;
	bool		has_answer;
	double		answer;
	char	   *source;
	bool		applied;
	char	   *request;
	char	   *query;
} LogEntry;

static List *log_entries = NIL;		/* in TopMemoryContext */
static int64 log_seq = 0;

/* ---------- directives ---------- */

typedef enum DirKind
{
	DIR_ROWS, DIR_SCALE, DIR_PROWS, DIR_PSCALE, DIR_SEL
} DirKind;

typedef struct Directive
{
	DirKind		kind;
	char	   *target;			/* sorted aliases, space separated */
	char	   *outer;			/* sorted aliases, or "" */
	double		value;
} Directive;

static List *directives = NIL;	/* in TopMemoryContext */
static char *directives_src = NULL;

/* ---------- callback connection ---------- */

static int	cb_sock = -1;
static TimestampTz cb_down_until = 0;
static char cb_path[108] = "";	/* path cb_sock is connected to (or failed on) */

/* forward */
static void xact_callback(XactEvent event, void *arg);
static void subxact_callback(SubXactEvent event, SubTransactionId mySubid,
							 SubTransactionId parentSubid, void *arg);

const char *
pgpi_point_name(PgpiPoint p)
{
	switch (p)
	{
		case PGPI_PT_BASE:
			return "base";
		case PGPI_PT_JOIN:
			return "join";
		case PGPI_PT_PARAM_BASE:
			return "param_base";
		case PGPI_PT_PARAM_JOIN:
			return "param_join";
		case PGPI_PT_CLAUSELIST:
			return "clauselist";
	}
	return "?";
}

void
pgpi_core_init(void)
{
	plan_cxt = AllocSetContextCreate(TopMemoryContext, "pgpi planning",
									 ALLOCSET_DEFAULT_SIZES);
	pgpi_ext_id = GetPlannerExtensionId("pgpi");
	RegisterXactCallback(xact_callback, NULL);
	RegisterSubXactCallback(subxact_callback, NULL);
}

void
pgpi_planning_reset(void)
{
	plan_depth = 0;
	pgpi_detour_reset_state();
	answer_cache = NIL;
	plan_query = NULL;
	if (plan_cxt)
		MemoryContextReset(plan_cxt);
}

void
pgpi_planning_begin(const char *query_string)
{
	if (plan_depth == 0)
	{
		pgpi_planning_reset();
		if (query_string)
			plan_query = MemoryContextStrdup(plan_cxt, query_string);
	}
	plan_depth++;
}

void
pgpi_planning_end(void)
{
	if (plan_depth > 0)
		plan_depth--;
}

static void
xact_callback(XactEvent event, void *arg)
{
	if (event == XACT_EVENT_ABORT || event == XACT_EVENT_PARALLEL_ABORT)
		pgpi_planning_reset();
}

static void
subxact_callback(SubXactEvent event, SubTransactionId mySubid,
				 SubTransactionId parentSubid, void *arg)
{
	if (event == SUBXACT_EVENT_ABORT_SUB)
		pgpi_planning_reset();
}

/* ---------- relation helpers ---------- */

bool
pgpi_is_plain_baserel(PlannerInfo *root, RelOptInfo *rel)
{
	RangeTblEntry *rte;

	if (rel->reloptkind != RELOPT_BASEREL || rel->relid <= 0 ||
		rel->relid >= root->simple_rel_array_size)
		return false;
	rte = root->simple_rte_array[rel->relid];
	return rte != NULL && rte->rtekind == RTE_RELATION;
}

static RangeTblEntry *
rte_of(PlannerInfo *root, int rti)
{
	if (rti <= 0 || rti >= root->simple_rel_array_size)
		return NULL;
	return root->simple_rte_array[rti];
}

static int
cmp_cstr(const void *a, const void *b)
{
	return strcmp(*(char *const *) a, *(char *const *) b);
}

/* Sorted, space separated aliases of the non-join RTEs in relids. */
static char *
alias_string(PlannerInfo *root, Relids relids)
{
	char	  **names;
	int			n = 0;
	int			rti = -1;
	StringInfoData buf;

	names = palloc(sizeof(char *) * (bms_num_members(relids) + 1));
	while ((rti = bms_next_member(relids, rti)) >= 0)
	{
		RangeTblEntry *rte = rte_of(root, rti);

		if (rte == NULL || rte->rtekind == RTE_JOIN)
			continue;
		names[n++] = rte->eref->aliasname;
	}
	qsort(names, n, sizeof(char *), cmp_cstr);
	initStringInfo(&buf);
	for (int i = 0; i < n; i++)
		appendStringInfo(&buf, "%s%s", i ? " " : "", names[i]);
	return buf.data;
}

static void
append_rels_json(StringInfo buf, PlannerInfo *root, Relids relids, bool *nonrel)
{
	int			rti = -1;
	bool		first = true;

	appendStringInfoChar(buf, '[');
	while ((rti = bms_next_member(relids, rti)) >= 0)
	{
		RangeTblEntry *rte = rte_of(root, rti);

		if (rte == NULL || rte->rtekind == RTE_JOIN)
			continue;
		if (!first)
			appendStringInfoChar(buf, ',');
		first = false;
		appendStringInfo(buf, "{\"rti\":%d,\"alias\":", rti);
		pgpi_append_json_string(buf, rte->eref->aliasname);
		if (rte->rtekind == RTE_RELATION)
		{
			char	   *nsp = get_namespace_name(get_rel_namespace(rte->relid));
			char	   *rel = get_rel_name(rte->relid);

			appendStringInfoString(buf, ",\"rel\":");
			pgpi_append_json_string(buf, quote_qualified_identifier(nsp, rel));
			appendStringInfo(buf, ",\"relid\":%u,\"inh\":%s}", rte->relid,
							 rte->inh ? "true" : "false");
		}
		else
		{
			appendStringInfo(buf, ",\"rel\":null,\"rtekind\":%d}", (int) rte->rtekind);
			if (nonrel)
				*nonrel = true;
		}
	}
	appendStringInfoChar(buf, ']');
}

/* Append [{"sql":..., "rels":"a b"}, ...] and return the number of opaque clauses. */
static int
append_clauses_json(StringInfo buf, PlannerInfo *root, List *clauses)
{
	ListCell   *lc;
	bool		first = true;
	int			opaque = 0;

	appendStringInfoChar(buf, '[');
	foreach(lc, clauses)
	{
		Node	   *n = (Node *) lfirst(lc);
		char	   *sql;
		Relids		varnos;

		if (IsA(n, RestrictInfo) && ((RestrictInfo *) n)->pseudoconstant)
			continue;
		sql = pgpi_deparse_clause(root, n);
		varnos = IsA(n, RestrictInfo) ? ((RestrictInfo *) n)->clause_relids
			: pull_varnos(root, n);
		if (!first)
			appendStringInfoChar(buf, ',');
		first = false;
		appendStringInfoString(buf, "{\"sql\":");
		pgpi_append_json_string(buf, sql);
		appendStringInfoString(buf, ",\"rels\":");
		pgpi_append_json_string(buf, alias_string(root, varnos));
		appendStringInfoChar(buf, '}');
		if (sql == NULL)
			opaque++;
	}
	appendStringInfoChar(buf, ']');
	return opaque;
}

/* ---------- joinrel clause bookkeeping ---------- */

static JoinrelClauses *
find_joinrel(RelOptInfo *rel)
{
	if (pgpi_ext_id < 0)
		return NULL;
	return (JoinrelClauses *) GetRelOptInfoExtensionState(rel, pgpi_ext_id);
}

/*
 * All clauses evaluated inside rel: the base restrictions of its members plus
 * every join clause applied while building it.
 */
List *
pgpi_all_clauses(PlannerInfo *root, RelOptInfo *rel)
{
	JoinrelClauses *jc;
	List	   *result = NIL;
	int			rti = -1;

	if (rel->reloptkind == RELOPT_BASEREL)
		return rel->baserestrictinfo;

	jc = find_joinrel(rel);
	if (jc)
		return jc->clauses;

	/* Fallback: base restrictions plus the join clauses that are fully inside rel. */
	while ((rti = bms_next_member(rel->relids, rti)) >= 0)
	{
		RelOptInfo *brel;
		ListCell   *lc;

		if (rti >= root->simple_rel_array_size ||
			(brel = root->simple_rel_array[rti]) == NULL)
			continue;
		result = list_concat_unique_ptr(result, brel->baserestrictinfo);
		foreach(lc, brel->joininfo)
		{
			RestrictInfo *ri = (RestrictInfo *) lfirst(lc);

			if (bms_is_subset(ri->clause_relids, rel->relids))
				result = list_append_unique_ptr(result, ri);
		}
	}
	return result;
}

void
pgpi_record_joinrel(PlannerInfo *root, RelOptInfo *joinrel,
					RelOptInfo *outer_rel, RelOptInfo *inner_rel,
					List *restrictlist)
{
	MemoryContext old;
	JoinrelClauses *jc;
	List	   *clauses;

	/*
	 * Already recorded. This also covers grouped joinrels (eager aggregation).
	 * They are memcpy'd from their plain joinrel and share its extension state,
	 * and we never write to a shared array.
	 */
	if (pgpi_ext_id < 0 || find_joinrel(joinrel))
		return;
	clauses = list_concat_unique_ptr(list_copy(pgpi_all_clauses(root, outer_rel)),
									 pgpi_all_clauses(root, inner_rel));
	clauses = list_concat_unique_ptr(clauses, restrictlist);

	/* Allocate next to the rel, so that both die together (GEQO). */
	old = MemoryContextSwitchTo(GetMemoryChunkContext(joinrel));
	jc = palloc(sizeof(JoinrelClauses));
	jc->clauses = list_copy(clauses);
	MemoryContextSwitchTo(old);
	SetRelOptInfoExtensionState(joinrel, pgpi_ext_id, jc);
}

/* ---------- directives ---------- */

static char *
sort_alias_tokens(char *s)
{
	char	   *tok,
			   *save;
	char	  **names = palloc(sizeof(char *) * (strlen(s) + 1));
	int			n = 0;
	StringInfoData buf;

	for (tok = strtok_r(s, " ,\t", &save); tok; tok = strtok_r(NULL, " ,\t", &save))
		names[n++] = tok;
	qsort(names, n, sizeof(char *), cmp_cstr);
	initStringInfo(&buf);
	for (int i = 0; i < n; i++)
		appendStringInfo(&buf, "%s%s", i ? " " : "", names[i]);
	return buf.data;
}

static void
parse_directives(void)
{
	MemoryContext old;
	char	   *buf,
			   *item,
			   *save;

	if (pgpi_directives == NULL || pgpi_directives[0] == '\0')
	{
		directives = NIL;
		return;
	}
	if (directives_src && strcmp(directives_src, pgpi_directives) == 0)
		return;

	old = MemoryContextSwitchTo(TopMemoryContext);
	directives = NIL;
	directives_src = pstrdup(pgpi_directives);
	buf = pstrdup(pgpi_directives);
	for (item = strtok_r(buf, ";", &save); item; item = strtok_r(NULL, ";", &save))
	{
		Directive  *d;
		char	   *p = item,
				   *lp,
				   *rp,
				   *bar,
				   *eq;
		DirKind		kind;

		while (*p == ' ' || *p == '\t' || *p == '\n')
			p++;
		if (*p == '\0')
			continue;
		if (strncmp(p, "PROWS", 5) == 0)
			kind = DIR_PROWS;
		else if (strncmp(p, "PSCALE", 6) == 0)
			kind = DIR_PSCALE;
		else if (strncmp(p, "ROWS", 4) == 0)
			kind = DIR_ROWS;
		else if (strncmp(p, "SCALE", 5) == 0)
			kind = DIR_SCALE;
		else if (strncmp(p, "SEL", 3) == 0)
			kind = DIR_SEL;
		else
		{
			elog(WARNING, "pgpi: unknown directive \"%s\"", p);
			continue;
		}
		lp = strchr(p, '(');
		rp = lp ? strchr(lp, ')') : NULL;
		eq = rp ? strchr(rp, '=') : NULL;
		if (!lp || !rp || !eq)
		{
			elog(WARNING, "pgpi: malformed directive \"%s\"", p);
			continue;
		}
		*rp = '\0';
		d = palloc0(sizeof(Directive));
		d->kind = kind;
		d->value = strtod(eq + 1, NULL);
		bar = strchr(lp + 1, '|');
		if (bar)
		{
			*bar = '\0';
			d->outer = sort_alias_tokens(bar + 1);
		}
		else
			d->outer = pstrdup("");
		d->target = sort_alias_tokens(lp + 1);
		directives = lappend(directives, d);
	}
	MemoryContextSwitchTo(old);
}

static bool
directive_answer(PgpiRequest *req, const char *target, const char *outer,
				 double *value)
{
	ListCell   *lc;
	bool		found = false;

	parse_directives();
	foreach(lc, directives)
	{
		Directive  *d = (Directive *) lfirst(lc);
		bool		param = (req->point == PGPI_PT_PARAM_BASE ||
							 req->point == PGPI_PT_PARAM_JOIN);

		if (strcmp(d->target, target) != 0 || strcmp(d->outer, outer) != 0)
			continue;
		switch (d->kind)
		{
			case DIR_ROWS:
				if (!param && req->point != PGPI_PT_CLAUSELIST)
				{
					*value = d->value;
					found = true;
				}
				break;
			case DIR_SCALE:
				if (!param && req->point != PGPI_PT_CLAUSELIST)
				{
					*value = req->pg_value * d->value;
					found = true;
				}
				break;
			case DIR_PROWS:
				if (param)
				{
					*value = d->value;
					found = true;
				}
				break;
			case DIR_PSCALE:
				if (param)
				{
					*value = req->pg_value * d->value;
					found = true;
				}
				break;
			case DIR_SEL:
				if (req->point == PGPI_PT_CLAUSELIST)
				{
					*value = d->value;
					found = true;
				}
				break;
		}
	}
	return found;
}

/* ---------- callback ---------- */

static void
cb_close(bool mark_down)
{
	if (cb_sock >= 0)
		close(cb_sock);
	cb_sock = -1;
	if (mark_down)
		cb_down_until = TimestampTzPlusMilliseconds(GetCurrentTimestamp(), 5000);
}

static bool
cb_connect(void)
{
	struct sockaddr_un addr;

	if (strlen(pgpi_callback) >= sizeof(addr.sun_path))
		return false;

	/* pgpi.callback may change within a session, so follow it */
	if (strcmp(cb_path, pgpi_callback) != 0)
	{
		if (cb_sock >= 0)
			close(cb_sock);
		cb_sock = -1;
		cb_down_until = 0;
		strlcpy(cb_path, pgpi_callback, sizeof(cb_path));
	}
	if (cb_sock >= 0)
		return true;
	if (GetCurrentTimestamp() < cb_down_until)
		return false;
	cb_sock = socket(AF_UNIX, SOCK_STREAM, 0);
	if (cb_sock < 0)
		return false;
	memset(&addr, 0, sizeof(addr));
	addr.sun_family = AF_UNIX;
	strcpy(addr.sun_path, pgpi_callback);
	if (connect(cb_sock, (struct sockaddr *) &addr, sizeof(addr)) < 0)
	{
		cb_close(true);
		return false;
	}
	return true;
}

/* Send one JSON line, receive one JSON line. */
static bool
cb_roundtrip(const char *line, StringInfo reply)
{
	size_t		len = strlen(line);
	size_t		off = 0;
	TimestampTz deadline;

	if (!cb_connect())
		return false;
	while (off < len)
	{
		ssize_t		n = send(cb_sock, line + off, len - off, MSG_NOSIGNAL);

		if (n <= 0)
		{
			cb_close(true);
			return false;
		}
		off += n;
	}
	if (send(cb_sock, "\n", 1, MSG_NOSIGNAL) != 1)
	{
		cb_close(true);
		return false;
	}

	deadline = TimestampTzPlusMilliseconds(GetCurrentTimestamp(), pgpi_callback_timeout);
	for (;;)
	{
		struct pollfd pfd;
		char		chunk[4096];
		ssize_t		n;
		long		wait_ms;
		char	   *nl;

		wait_ms = (deadline - GetCurrentTimestamp()) / 1000;
		if (wait_ms <= 0)
		{
			cb_close(true);
			return false;
		}
		pfd.fd = cb_sock;
		pfd.events = POLLIN;
		pfd.revents = 0;
		if (poll(&pfd, 1, (int) Min(wait_ms, 1000)) < 0)
		{
			if (errno == EINTR)
			{
				CHECK_FOR_INTERRUPTS();
				continue;
			}
			cb_close(true);
			return false;
		}
		CHECK_FOR_INTERRUPTS();
		if (!(pfd.revents & POLLIN))
			continue;
		n = recv(cb_sock, chunk, sizeof(chunk), 0);
		if (n <= 0)
		{
			cb_close(true);
			return false;
		}
		appendBinaryStringInfo(reply, chunk, n);
		if ((nl = memchr(reply->data, '\n', reply->len)) != NULL)
		{
			*nl = '\0';
			reply->len = nl - reply->data;
			return true;
		}
	}
}

static bool
json_number(const char *json, const char *key, double *out)
{
	char		pattern[64];
	const char *p;
	char	   *end;

	snprintf(pattern, sizeof(pattern), "\"%s\"", key);
	p = strstr(json, pattern);
	if (p == NULL)
		return false;
	p += strlen(pattern);
	while (*p == ' ' || *p == ':')
		p++;
	*out = strtod(p, &end);
	return end != p;
}

static char *
json_string(const char *json, const char *key)
{
	char		pattern[64];
	const char *p,
			   *q;

	snprintf(pattern, sizeof(pattern), "\"%s\"", key);
	p = strstr(json, pattern);
	if (p == NULL)
		return NULL;
	p += strlen(pattern);
	while (*p == ' ' || *p == ':')
		p++;
	if (*p != '"')
		return NULL;
	q = strchr(p + 1, '"');
	if (q == NULL)
		return NULL;
	return pnstrdup(p + 1, q - p - 1);
}

/* ---------- logging ---------- */

static void
log_exchange(PgpiRequest *req, const char *rels, const char *outer,
			 bool has_answer, double answer, const char *source,
			 bool applied, const char *json)
{
	MemoryContext old = MemoryContextSwitchTo(TopMemoryContext);
	LogEntry   *e = palloc0(sizeof(LogEntry));

	e->seq = ++log_seq;
	e->route = req->route;
	e->point = req->point;
	e->rels = pstrdup(rels);
	e->outer = pstrdup(outer);
	e->pg_value = req->pg_value;
	e->has_answer = has_answer;
	e->answer = answer;
	e->source = source ? pstrdup(source) : NULL;
	e->applied = applied;
	e->request = pstrdup(json);
	e->query = plan_query ? pnstrdup(plan_query, Min(strlen(plan_query), 1000)) : NULL;
	log_entries = lappend(log_entries, e);
	while (list_length(log_entries) > Max(pgpi_log_size, 1))
	{
		LogEntry   *old_e = (LogEntry *) linitial(log_entries);

		log_entries = list_delete_first(log_entries);
		pfree(old_e->rels);
		pfree(old_e->outer);
		if (old_e->source)
			pfree(old_e->source);
		pfree(old_e->request);
		if (old_e->query)
			pfree(old_e->query);
		pfree(old_e);
	}
	MemoryContextSwitchTo(old);
}

/* ---------- the core ---------- */

/*
 * Relids without RTE_JOIN members. Outer-join relids are part of joinrel
 * relids since PostgreSQL 16, but they are never part of the relation set of a plan node.
 */
Relids
pgpi_nonjoin_relids(PlannerInfo *root, Relids relids)
{
	Relids		result = NULL;
	int			rti = -1;

	while ((rti = bms_next_member(relids, rti)) >= 0)
	{
		RangeTblEntry *rte = rte_of(root, rti);

		if (rte != NULL && rte->rtekind == RTE_JOIN)
			continue;
		result = bms_add_member(result, rti);
	}
	return result;
}

/*
 * The part of a request that describes the question itself. Plan nodes in
 * the observation log have exactly the same fields. So a request and an
 * observation of the same relation set can be compared field by field.
 *
 * Emit object members without the enclosing braces:
 *   "point":..,"plan_name":..,"rels":[..],"outer":[..],"clauses":[..],
 *   "outer_clauses":[..],"opaque":n,"nonrel":b,"jointype":j
 */
void
pgpi_append_descriptor(StringInfo buf, PlannerInfo *root, PgpiPoint point,
					   Relids relids, Relids outer_relids, List *clauses,
					   JoinType jointype)
{
	bool		nonrel = false;
	int			opaque;
	int			rti = -1;
	StringInfoData oc;
	bool		first = true;

	appendStringInfo(buf, "\"point\":\"%s\",\"plan_name\":", pgpi_point_name(point));
	pgpi_append_json_string(buf, root->plan_name);
	appendStringInfoString(buf, ",\"rels\":");
	append_rels_json(buf, root, relids, &nonrel);
	appendStringInfoString(buf, ",\"outer\":");
	append_rels_json(buf, root, outer_relids, &nonrel);
	appendStringInfoString(buf, ",\"clauses\":");
	opaque = append_clauses_json(buf, root, clauses);

	/* Filters of the outer (parameterizing) base relations. */
	initStringInfo(&oc);
	appendStringInfoChar(&oc, '[');
	while ((rti = bms_next_member(outer_relids, rti)) >= 0)
	{
		RelOptInfo *orel;
		StringInfoData one;

		if (rti >= root->simple_rel_array_size ||
			(orel = root->simple_rel_array[rti]) == NULL ||
			(orel->reloptkind != RELOPT_BASEREL &&
			 orel->reloptkind != RELOPT_OTHER_MEMBER_REL))
			continue;
		initStringInfo(&one);
		opaque += append_clauses_json(&one, root, orel->baserestrictinfo);
		if (one.len > 2)
		{
			if (!first)
				appendStringInfoChar(&oc, ',');
			first = false;
			appendBinaryStringInfo(&oc, one.data + 1, one.len - 2);
		}
		pfree(one.data);
	}
	appendStringInfoChar(&oc, ']');
	appendStringInfoString(buf, ",\"outer_clauses\":");
	appendBinaryStringInfo(buf, oc.data, oc.len);
	pfree(oc.data);

	appendStringInfo(buf, ",\"opaque\":%d,\"nonrel\":%s,\"jointype\":%d",
					 opaque, nonrel ? "true" : "false", (int) jointype);
}

void
pgpi_append_int64(StringInfo buf, int64 v)
{
	appendStringInfo(buf, INT64_FORMAT, v);
}

/* JSON has no NaN/Infinity, so emit null for them. */
void
pgpi_append_double(StringInfo buf, double v)
{
	if (isnan(v) || isinf(v))
		appendStringInfoString(buf, "null");
	else
		appendStringInfo(buf, "%.17g", v);
}

/* ---------- per-planning observation state ---------- */

PgpiPlanState *
pgpi_plan_state(PlannerGlobal *glob)
{
	if (glob == NULL || pgpi_ext_id < 0)
		return NULL;
	return (PgpiPlanState *) GetPlannerGlobalExtensionState(glob, pgpi_ext_id);
}

/* Remember a PlannerInfo, so that the observation can find its rels later. */
void
pgpi_note_root(PlannerInfo *root)
{
	PgpiPlanState *ps = pgpi_plan_state(root->glob);
	MemoryContext old;

	if (ps == NULL || list_member_ptr(ps->roots, root))
		return;
	old = MemoryContextSwitchTo(ps->cxt);	/* never a GEQO context */
	ps->roots = lappend(ps->roots, root);
	MemoryContextSwitchTo(old);
}

static void
note_request(PgpiRequest *req, bool has_answer, double value,
			 const char *source, bool applied, const char *json)
{
	PgpiPlanState *ps = pgpi_plan_state(req->root->glob);
	MemoryContext old;
	PgpiPlanReq *pr;

	if (ps == NULL)
		return;
	old = MemoryContextSwitchTo(ps->cxt);
	pr = palloc0(sizeof(PgpiPlanReq));
	pr->root = req->root;
	pr->point = req->point;
	pr->route = req->route;
	pr->relids = pgpi_nonjoin_relids(req->root, req->relids);
	pr->outer_relids = pgpi_nonjoin_relids(req->root, req->outer_relids);
	pr->pg_value = req->pg_value;
	pr->has_answer = has_answer;
	pr->answer = value;
	pr->source = source ? pstrdup(source) : NULL;
	pr->applied = applied;
	pr->seq = log_seq;
	pr->json = pstrdup(json);
	ps->reqs = lappend(ps->reqs, pr);
	MemoryContextSwitchTo(old);
}

/* ---------- the core ---------- */

double
pgpi_intervene(PgpiRequest *req)
{
	char	   *rels;
	char	   *outer;
	char	   *json;
	StringInfoData desc;
	StringInfoData jbuf;
	StringInfoData key;
	ListCell   *lc;
	bool		has_answer = false;
	double		value = 0.0;
	char	   *source = NULL;
	bool		applied;
	double		result;

	if (pgpi_mode == PGPI_MODE_OFF || req->root == NULL)
		return req->pg_value;

	rels = alias_string(req->root, req->relids);
	outer = alias_string(req->root, req->outer_relids);

	initStringInfo(&desc);
	pgpi_append_descriptor(&desc, req->root, req->point, req->relids,
						   req->outer_relids, req->clauses, req->jointype);

	initStringInfo(&jbuf);
	appendStringInfo(&jbuf, "{\"route\":\"%c\",\"queryid\":", req->route);
	pgpi_append_int64(&jbuf, req->root->parse->queryId);
	appendStringInfoString(&jbuf, ",\"label\":");
	pgpi_append_json_string(&jbuf, (pgpi_label && pgpi_label[0]) ? pgpi_label : NULL);
	appendStringInfoChar(&jbuf, ',');
	appendBinaryStringInfo(&jbuf, desc.data, desc.len);
	if (req->split)
	{
		/* not part of the descriptor: observations and the cache ignore it */
		appendStringInfoString(&jbuf, ",\"split\":");
		pgpi_append_json_string(&jbuf, alias_string(req->root, req->split));
		appendStringInfoString(&jbuf, ",\"outer_rows\":");
		pgpi_append_double(&jbuf, req->outer_rows);
		appendStringInfoString(&jbuf, ",\"inner_rows\":");
		pgpi_append_double(&jbuf, req->inner_rows);
	}
	appendStringInfoString(&jbuf, ",\"hints\":");
	pgpi_append_json_string(&jbuf, (pgpi_hints && pgpi_hints[0]) ? pgpi_hints : NULL);
	appendStringInfoString(&jbuf, ",\"pg\":");
	pgpi_append_double(&jbuf, req->pg_value);
	appendStringInfoString(&jbuf, ",\"tuples\":");
	pgpi_append_double(&jbuf, req->tuples);
	appendStringInfoChar(&jbuf, '}');
	json = jbuf.data;

	/*
	 * Per-planning cache: the same question gets the same answer, whichever
	 * route asks it. The key is the descriptor plus the value of PostgreSQL.
	 */
	initStringInfo(&key);
	appendStringInfo(&key, "%p|", (void *) req->root);
	appendBinaryStringInfo(&key, desc.data, desc.len);
	appendStringInfo(&key, "|%.17g|%.17g", req->pg_value, req->tuples);
	foreach(lc, answer_cache)
	{
		CachedAnswer *ca = (CachedAnswer *) lfirst(lc);

		if (strcmp(ca->key, key.data) == 0)
		{
			has_answer = ca->has_answer;
			value = ca->value;
			source = ca->source;
			goto decided;
		}
	}

	if (directive_answer(req, rels, outer, &value))
	{
		has_answer = true;
		source = "directive";
	}
	else if (pgpi_callback && pgpi_callback[0] != '\0')
	{
		StringInfoData reply;
		double		v;

		initStringInfo(&reply);
		if (cb_roundtrip(json, &reply))
		{
			bool		sel_point = (req->point == PGPI_PT_CLAUSELIST);

			if (sel_point && json_number(reply.data, "sel", &v))
			{
				value = v;
				has_answer = true;
			}
			else if (json_number(reply.data, "rows", &v))
			{
				if (sel_point)
				{
					if (req->tuples > 0)
					{
						value = v / req->tuples;
						has_answer = true;
					}
				}
				else
				{
					value = v;
					has_answer = true;
				}
			}
			else if (json_number(reply.data, "factor", &v))
			{
				value = req->pg_value * v;
				has_answer = true;
			}
			if (has_answer)
			{
				source = json_string(reply.data, "source");
				if (source == NULL)
					source = "callback";
			}
		}
		else
			source = "callback-error";
	}

	if (has_answer && (isnan(value) || isinf(value) || value < 0))
	{
		has_answer = false;
		source = "invalid";
	}

	{
		MemoryContext old = MemoryContextSwitchTo(plan_cxt);
		CachedAnswer *ca = palloc(sizeof(CachedAnswer));

		ca->key = pstrdup(key.data);
		ca->has_answer = has_answer;
		ca->value = value;
		ca->source = source ? pstrdup(source) : NULL;
		answer_cache = lappend(answer_cache, ca);
		MemoryContextSwitchTo(old);
	}

decided:
	if (has_answer)
	{
		if (req->point == PGPI_PT_CLAUSELIST)
			value = Min(Max(value, 1e-10), 1.0);
		else
			value = clamp_row_est(value);
	}
	applied = has_answer && pgpi_mode == PGPI_MODE_ENFORCE;
	result = applied ? value : req->pg_value;

	log_exchange(req, rels, outer, has_answer, value, source, applied, json);
	note_request(req, has_answer, value, source, applied, json);

	if (pgpi_trace)
		elog(NOTICE, "pgpi: %c %s (%s%s%s) pg=%.6g -> %s%s",
			 req->route, pgpi_point_name(req->point), rels,
			 outer[0] ? " | " : "", outer, req->pg_value,
			 has_answer ? psprintf("%.6g", value) : "abstain",
			 has_answer ? (applied ? " [applied]" : " [shadow]") : "");
	return result;
}

/* ---------- SQL-callable functions ---------- */

#include "fmgr.h"
#include "funcapi.h"
#include "utils/tuplestore.h"

PG_FUNCTION_INFO_V1(pgpi_requests);
PG_FUNCTION_INFO_V1(pgpi_clear);
PG_FUNCTION_INFO_V1(pgpi_status);

Datum
pgpi_requests(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	ListCell   *lc;

	InitMaterializedSRF(fcinfo, 0);
	foreach(lc, log_entries)
	{
		LogEntry   *e = (LogEntry *) lfirst(lc);
		Datum		values[11];
		bool		nulls[11] = {0};

		values[0] = Int64GetDatum(e->seq);
		values[1] = CStringGetTextDatum(psprintf("%c", e->route));
		values[2] = CStringGetTextDatum(pgpi_point_name(e->point));
		values[3] = CStringGetTextDatum(e->rels);
		values[4] = CStringGetTextDatum(e->outer);
		values[5] = Float8GetDatum(e->pg_value);
		if (e->has_answer)
			values[6] = Float8GetDatum(e->answer);
		else
			nulls[6] = true;
		if (e->source)
			values[7] = CStringGetTextDatum(e->source);
		else
			nulls[7] = true;
		values[8] = BoolGetDatum(e->applied);
		values[9] = CStringGetTextDatum(e->request);
		if (e->query)
			values[10] = CStringGetTextDatum(e->query);
		else
			nulls[10] = true;
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}
	return (Datum) 0;
}

Datum
pgpi_clear(PG_FUNCTION_ARGS)
{
	ListCell   *lc;

	foreach(lc, log_entries)
	{
		LogEntry   *e = (LogEntry *) lfirst(lc);

		pfree(e->rels);
		pfree(e->outer);
		if (e->source)
			pfree(e->source);
		pfree(e->request);
		if (e->query)
			pfree(e->query);
		pfree(e);
	}
	list_free(log_entries);
	log_entries = NIL;
	PG_RETURN_VOID();
}

Datum
pgpi_status(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

	InitMaterializedSRF(fcinfo, 0);
	for (int i = 0; i < pgpi_detour_count(); i++)
	{
		PgpiDetour *d = pgpi_detour_get(i);
		Datum		values[4];
		bool		nulls[4] = {0};

		values[0] = CStringGetTextDatum(d->name);
		values[1] = BoolGetDatum(d->installed);
		values[2] = Int64GetDatum(d->hits);
		values[3] = CStringGetTextDatum(d->note);
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}
	return (Datum) 0;
}
