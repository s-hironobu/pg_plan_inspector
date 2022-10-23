/*
 * adjust_rows.h
 *
 * Copyright (c) 2021-2022, Hironobu Suzuki @ interdb.jp
 *
 */
#ifndef __ADJUST_ROWS_H_
#define __ADJUST_ROWS_H_

#include "optimizer/pathnode.h"
#include "optimizer/planner.h"
#include "nodes/print.h"
#include "nodes/bitmapset.h"
#include "optimizer/pgqp_allpaths.h"
#include "nodes/nodes.h"
#include "utils/guc.h"

/*
 * Store the egression parameters of the current query.
 */
typedef struct baseRel
{
	int			id;
	Oid			oid;
}			baseRel;

typedef struct param
{
	NodeTag		type;

	Relids		relids;
	Relids		outer_relids;
	bool		is_outer_relids_empty;
	Relids		inner_relids;
	bool		is_inner_relids_empty;

	double		coef[2];		/* In Nested Loop, coef[0] is only used;
								 * otherwise, coef[0] and coef[1] are for
								 * outer and inner, respectively. */
	double		coef2;
	bool		is_coef2_null;
	double		intercept;
	bool		is_intercept_null;
	bool		mergeflag;
}			param;

typedef struct regParams
{
	int			relsize;
	baseRel    *baseRels;

	int			paramsize;
	param	   *params;
}			regParams;


/*
 * Store the current states, such as enable_nestloop, enable_mergejoin, etc.
 */
typedef struct CurrentState
{
	int			init_join_mask; /* initial value join parameter */
	GucContext	context;		/* which GUC parameters can we set? */
}			CurrentState;

/*
 * The following enums and definitions are imported from pg_hint_plan.
 * https://github.com/ossc-db/pg_hint_plan
 */
typedef enum
{
	ENABLE_SEQSCAN = 0x01,
	ENABLE_INDEXSCAN = 0x02,
	ENABLE_BITMAPSCAN = 0x04,
	ENABLE_TIDSCAN = 0x08,
	ENABLE_INDEXONLYSCAN = 0x10
} SCAN_TYPE_BITS;

typedef enum
{
	ENABLE_NESTLOOP = 0x01,
	ENABLE_MERGEJOIN = 0x02,
	ENABLE_HASHJOIN = 0x04,
	ENABLE_MEMOIZE = 0x08
} JOIN_TYPE_BITS;

#define ENABLE_ALL_SCAN (ENABLE_SEQSCAN | ENABLE_INDEXSCAN | \
						 ENABLE_BITMAPSCAN | ENABLE_TIDSCAN | \
						 ENABLE_INDEXONLYSCAN)
#define ENABLE_ALL_JOIN (ENABLE_NESTLOOP | ENABLE_MERGEJOIN | ENABLE_HASHJOIN)
#define DISABLE_ALL_SCAN 0
#define DISABLE_ALL_JOIN 0

extern void set_join_config_options(unsigned char enforce_mask, bool set_memoize,
									GucContext context);

/*
 * Declare functions
 */
extern int	get_join_mask(NodeTag join_type);
extern void set_current_state(void);

extern bool check_join_param(NodeTag *join_type, bool *mergeflag, const Relids outer_relids,
							 const Relids inner_relids);
extern bool check_rel_param(NodeTag *rel_type, const Relids relids);
extern int	relname2rti(const char *relname);
extern void set_nodeid(const int nid, const char *node_type);
extern void set_relids(const int nid, Relids relids);
extern void set_outer_relids(const int nid, Relids relids);
extern void set_inner_relids(const int nid, Relids relids);
extern void add_relids(Relids relids, const int rti);
extern void free_reg_params(void);
extern void set_coef(const int nid, const double outer_coef, const double inner_coef);
extern void set_coef2(const int nid, const double coef2);
extern void set_intercept(const int nid, const double intercept);
extern void set_mergeflag(const int nid, const char *mergeflag);
extern char *selectParams(const char *queryid);
extern bool set_reg_params(const Query *parse, char *params);
extern void pgqp_set_rel_pathlist(PlannerInfo *root, RelOptInfo *rel,
								  Index rti, RangeTblEntry *rte);
extern void pgqp_set_join_pathlist(PlannerInfo *root, RelOptInfo *joinrel,
								   RelOptInfo *outerrel, RelOptInfo *innerrel,
								   JoinType jointype, JoinPathExtraData *extra);
extern bool adjust_joinrel_rows(RelOptInfo *joinrel,
								RelOptInfo *outer_rel,
								RelOptInfo *inner_rel,
								bool swap_rels);

#endif							/* __ADJUST_ROWS_H_ */
