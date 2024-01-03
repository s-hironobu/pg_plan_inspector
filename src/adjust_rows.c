/*
 * adjust_rows.c
 *
 * The set of functions of this file is for the feasibility study to intervene in the
 * optimizer's processing.
 * This only adjusts the plan rows estimated by the optimizer and does not improve the
 * cardinality estimation.
 *
 * Orverview:
 * The regression parameters are stored in the query_plan.reg table in each database by
 * the repo_mgr.py with push command.
 *
 * Whenever a query is issued, the optimizer checks the query_plan.reg table, and gets
 * the parameters if found. See selectParams().
 *
 * To adjust the plan rows with the regression parameters, pgqp_set_rel_pathlist() and
 * adjust_joinrel_rows() are used. pgqp_set_rel_pathlist() is used when adjusting the
 * baserel rows, such as seqscan, index only scan, etc. adjust_joinrel_rows() is used
 * when adjusting the joinrel rows, such as nestloop, mergejoin, and hashjoin.
 *
 * To force to use the specified join method by the regression parameter in each join
 * level, the set_join_config_options() function, which is imported from the pg_hint_plan
 * https://github.com/ossc-db/pg_hint_plan, is issued.
 * For example, if it is described in the regression parameters that the hashjoin method
 * is used between tbl1 and tbl2, set_jon_config_options() with enable_hashjoin option
 * is issued when the optimizer considers the join plan with tbl1 and tbl2.
 * See populate_joinrel_with_paths()@src/optimizer/pgqp_joinrels.c in detail.
 *
 * Copyright (c) 2021-2024, Hironobu Suzuki @ interdb.jp
 *
 */
#include "postgres.h"

#include "access/hash.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "catalog/namespace.h"
#include "nodes/nodes.h"
#include "optimizer/cost.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/guc.h"
#include "utils/rel.h"
#include "adjust_rows.h"
#include "param.h"
#include "param_parser.h"
#include "miscadmin.h"


/*
 * Declare variables
 */
extern set_rel_pathlist_hook_type prev_set_rel_pathlist;
extern set_join_pathlist_hook_type prev_set_join_pathlist;

extern bool pgqp_enable_adjust_joinrel_rows;
extern bool pgqp_enable_adjust_rel_rows;


CurrentState current_state;		/* Hold the planner method configuration
								 * variables related to join methods. */
char	   *pgqp_reg_params;	/* This is read by
								 * input_params()@param_scanner.l. */
bool		pgqp_adjust_rows;	/* Whether plan rows can be adjusted or not */
regParams	reg_params;			/* Store the regression parameters of the
								 * current query. */

static unsigned int qno = 0;
static unsigned int msgqno = 0;

/*
 * Declare functions
 */
#ifdef __DEBUG__
static char *get_nodetype(const NodeTag ntag);
static void show_rtables(PlannerInfo *root);
static void show_relids(const Relids relids, char *_relids);
static void show_reg_params(const char *string);
#endif							/* __DEBUG__  */

static int	relid2rti(const Oid oid);
static NodeTag get_nodeid(const char *node_type);
static bool get_join_param(NodeTag *join_type,
						   const Relids outer_relids, const Relids inner_relids,
						   double *outer_coef, double *inner_coef,
						   double *coef2, bool *is_coef2_null,
						   double *intercept, bool *is_intercept_null, bool *mergeflag);
static bool get_rel_param(const int rti, NodeTag *rel_type, double *coef,
						  double *intercept, bool *is_intercept_null, bool *mergeflag);
static double get_parallel_divisor(Path *path);

/*----
 * Three functions are imported from pg_hint_plan.
 * 	https://github.com/ossc-db/pg_hint_plan
 */
#include "../pg_hint_plan/pg_hint_plan.c"

/*
 * Set current join mask and context to current_state.
 */
void
set_current_state(void)
{
	current_state.init_join_mask = get_current_join_mask();
	current_state.context = superuser() ? PGC_SUSET : PGC_USERSET;
}

/*
 * Return join mask.
 */
int
get_join_mask(NodeTag join_type)
{
	int			mask = 0;

	if (join_type == T_NestPath)
		mask |= ENABLE_NESTLOOP;
	if (join_type == T_MergePath)
		mask |= ENABLE_MERGEJOIN;
	if (join_type == T_HashPath)
		mask |= ENABLE_HASHJOIN;

	return mask;
}


#ifdef __DEBUG__
static char *
get_nodetype(const NodeTag ntag)
{
	switch (ntag)
	{
		case T_MergePath:
			return "Merge Join";
			break;
		case T_HashPath:
			return "Hash Join";
			break;
		case T_NestPath:
			return "Nested Loop";
			break;
		default:
			return "Scan";
	};
}

static void
show_rtables(PlannerInfo *root)
{
	int			rti;

	for (rti = 1; rti < root->simple_rel_array_size; rti++)
	{
		RelOptInfo *rel = root->simple_rel_array[rti];
		RangeTblEntry *rte = root->simple_rte_array[rti];

		if (rel == NULL || rel->reloptkind != RELOPT_BASEREL)
			continue;
		if (rte->rtekind == RTE_RELATION)
			elog(LOG, "TEST rtable[%d] = %u", rti, rte->relid);
	}
}

#define RELIDS_LEN 64
#define N_LEN 10
static void
show_relids(const Relids relids, char *_relids)
{
	int			i,
				j;
	char		n[N_LEN];

	for (i = 0; i < RELIDS_LEN; i++)
		_relids[i] = '\0';

	if (bms_is_empty(relids))
	{
		sprintf(_relids, "EMPTY");
		return;
	}


	j = 0;
	for (i = 1; i < N_LEN; i++)
	{
		if (bms_is_member(i, relids))
		{
			sprintf(n, " %d ", i);
			for (int k = 0; k < (int) strlen(n); k++)
				_relids[j + k] = n[k];
			j += (int) strlen(n);
		}
	}
}
#undef N_LEN

static void
show_reg_params(const char *string)
{

	int			i;
	char	   *type;
	char		_relids[RELIDS_LEN];
	char		_outer_relids[RELIDS_LEN];
	char		_inner_relids[RELIDS_LEN];

	elog(LOG, "============ %s ============= %s ", __func__, string);

	/* baseRels */
	elog(LOG, "    baseRels (%d)", reg_params.relsize);
	for (i = 0; i < reg_params.relsize; i++)
		elog(LOG, "        rti(%d) = %u",
			 reg_params.baseRels[i].id, reg_params.baseRels[i].oid);

	/* params */
	elog(LOG, "    paramsize (%d)", reg_params.paramsize);
	for (i = 0; i < reg_params.paramsize; i++)
	{
		switch ((int) reg_params.params[i].type)
		{
			case T_NestPath:
				type = "NestedLoop";
				break;
			case T_MergePath:
				type = "MergeJoin";
				break;
			case T_HashPath:
				type = "HashJoin";
				break;
			default:
				type = "SCAN OR INDEXSCAN";
				break;
		}

		show_relids(reg_params.params[i].relids, _relids);
		show_relids(reg_params.params[i].outer_relids, _outer_relids);
		show_relids(reg_params.params[i].inner_relids, _inner_relids);

		elog(LOG, "    %d: type=%s  relid=(%s)  outer_relids(%s)  inner_relids(%s)",
			 i, type, _relids, _outer_relids, _inner_relids);
	}
}

#undef RELIDS_LEN

#endif							/* __DEBUG__ */


/*
 * Free reg_params
 */
void
free_reg_params(void)
{
	/* Free reg_params.baseRels */
	if (reg_params.baseRels != NULL)
		pfree(reg_params.baseRels);
	reg_params.relsize = 0;
	reg_params.baseRels = NULL;

	/* Free reg_params.params */
	if (reg_params.params != NULL)
	{
		for (int i = 0; i < reg_params.paramsize; i++)
		{
			if (reg_params.params[i].relids != NULL)
				bms_free(reg_params.params[i].relids);
			if (reg_params.params[i].outer_relids != NULL)
				bms_free(reg_params.params[i].outer_relids);
			if (reg_params.params[i].inner_relids != NULL)
				bms_free(reg_params.params[i].inner_relids);
		}
		pfree(reg_params.params);
	}
	reg_params.paramsize = 0;
	reg_params.params = NULL;
}


/*
 * Get node type of the specified node name.
 */
static NodeTag
get_nodeid(const char *node_type)
{
	if (strcmp(node_type, "\"Merge Join\"") == 0)
		return T_MergePath;
	else if (strcmp(node_type, "\"Hash Join\"") == 0)
		return T_HashPath;
	else if (strcmp(node_type, "\"Nested Loop\"") == 0)
		return T_NestPath;
	else if (strcmp(node_type, "\"Seq Scan\"") == 0)
		return T_Path;
	else if (strcmp(node_type, "\"Index Scan\"") == 0
			 || strcmp(node_type, "\"Index Only Scan\"") == 0)
		return T_IndexPath;

	return T_Invalid;
}


/*
 * Functions used in param_parser.y
 */
void
add_relids(Relids relids, const int rti)
{
	Assert(rti >= 0);
	relids = bms_add_member(relids, rti);
}

void
set_relids(const int nid, Relids relids)
{
	Assert(nid < reg_params.relsize);
	reg_params.params[nid].relids = bms_copy(relids);
}

void
set_outer_relids(const int nid, Relids relids)
{
	Assert(nid < reg_params.relsize);
	if (!bms_is_empty(relids))
	{
		reg_params.params[nid].outer_relids = bms_copy(relids);
		reg_params.params[nid].is_outer_relids_empty = false;
	}
}

void
set_inner_relids(const int nid, Relids relids)
{
	Assert(nid < reg_params.relsize);
	if (!bms_is_empty(relids))
	{
		reg_params.params[nid].inner_relids = bms_copy(relids);
		reg_params.params[nid].is_inner_relids_empty = false;
	}
}

void
set_nodeid(const int nid, const char *node_type)
{
	Assert(nid < reg_params.relsize);
	reg_params.params[nid].type = get_nodeid(node_type);
}

void
set_coef(const int nid, const double outer_coef, const double inner_coef)
{
	Assert(nid < reg_params.relsize);
	reg_params.params[nid].coef[0] = outer_coef;
	reg_params.params[nid].coef[1] = inner_coef;
}

void
set_coef2(const int nid, const double coef2)
{
	Assert(nid < reg_params.relsize);
	reg_params.params[nid].coef2 = coef2;
	reg_params.params[nid].is_coef2_null = false;
}

void
set_intercept(const int nid, const double intercept)
{
	Assert(nid < reg_params.relsize);
	reg_params.params[nid].intercept = intercept;
	reg_params.params[nid].is_intercept_null = false;
}

void
set_mergeflag(const int nid, const char *mergeflag)
{
	Assert(nid < reg_params.relsize);
	if (strcmp(mergeflag, "\"True\"") == 0)
		reg_params.params[nid].mergeflag = true;
	else
		reg_params.params[nid].mergeflag = false;
}

/*
 * Get rti of the specified oid. If baseRels does not contain the oid, return Error.
 */
static int
relid2rti(const Oid oid)
{
	int			rti;

	for (rti = 0; rti < reg_params.relsize; rti++)
	{
		if (reg_params.baseRels[rti].oid == oid)
			return reg_params.baseRels[rti].id;
	}

	elog(ERROR, "oid:%d is not found in baseRels.", (int) oid);
	return -1;
}

/*
 * Get rti of the specified relname. If baseRels does not contain the relname, return Error.
 */
int
relname2rti(const char *relname)
{
	Oid			nsoid;
	char		schema[NAMEDATALEN];
	char		rel[NAMEDATALEN];
	int			i,
				j,
				len;

	memset(schema, 0, sizeof(schema));
	memset(rel, 0, sizeof(rel));

	/*
	 * Get schema and rel from relname whose format is '\"schema.relation\"'.
	 */
	len = (int) strlen(relname);
	for (i = 1; i < len; i++)	/* skip '^\"' */
	{
		if (relname[i] != '.')
			schema[i - 1] = relname[i];
		else
		{
			schema[i - 1] = '\0';
			break;
		}
	}

	i++;						/* skip '.' */

	for (j = i; j < len - 1; j++)	/* skip '\"$' */
		rel[j - i] = relname[j];

	/*
	 * Get the oid of schema
	 */
	if ((nsoid = get_namespace_oid(schema, true)) == InvalidOid)
	{
		elog(ERROR, "schema:%s is invalid.", schema);
		return -1;
	}

	return relid2rti(get_relname_relid(rel, nsoid));
}

/*
 * Create and set reg_params and Set pgqp_reg_params.
 */
bool
set_reg_params(const Query *parse, char *params)
{
	int			i,
				params_len;
	int			rti;
	ListCell   *lc;
	int			numNodeType = 0;

	/*
	 * Free all elements of reg_params that stores the regression parameters.
	 *
	 * In general, the execution of free_reg_params() is done in
	 * pgqp_ExecutorStart() after the end of planning. However, if the
	 * previous query process was interrupted during planning, all elements of
	 * reg_params will remain un-cleaned. To deal with such a case, we will
	 * execute this function here.
	 */
	free_reg_params();

	/*
	 * Set baseRels
	 */

	/* Count number of rtable. */
	rti = 0;
	foreach(lc, parse->rtable)
		rti++;
	reg_params.relsize = rti;

	/* Create baseRels */
	reg_params.baseRels = (baseRel *) palloc(sizeof(baseRel) * reg_params.relsize);
	if (reg_params.baseRels == NULL)
		return false;

	/* Set baseRels */
	rti = 0;
	foreach(lc, parse->rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);

		reg_params.baseRels[rti].id = rti + 1;	/* Because the range table's
												 * id is numbered sequentially
												 * from 1. */
		if (rte->rtekind == RTE_RELATION)
			reg_params.baseRels[rti].oid = rte->relid;
		else
			reg_params.baseRels[rti].oid = 0;
		rti++;
	}

	/*
	 * Set params
	 */
	Assert(params != NULL);

	/* Count the number of "Node Type" */
	params_len = 0;
	while (params[params_len] != '\0')
		params_len++;

#define __PS__ "{"				/* Param Start code */
	for (i = 0; i < (params_len - (int) (strlen(__PS__))); i++)
		if (strncmp((char *) (&params[i]), __PS__, (size_t) strlen(__PS__)) == 0)
			numNodeType++;
#undef __PS__
	reg_params.paramsize = numNodeType;

	/* Create reg_params.params that stores regression params. */
	reg_params.params = (param *) palloc(sizeof(param) * reg_params.paramsize);
	if (reg_params.params == NULL)
		return false;
	for (i = 0; i < reg_params.paramsize; i++)
	{
		reg_params.params[i].relids = NULL;
		reg_params.params[i].outer_relids = NULL;
		reg_params.params[i].inner_relids = NULL;
		reg_params.params[i].is_outer_relids_empty = true;
		reg_params.params[i].is_inner_relids_empty = true;
		reg_params.params[i].is_coef2_null = true;
		reg_params.params[i].is_intercept_null = true;
		reg_params.params[i].mergeflag = false;
	}

	/*
	 * Parse params string.
	 */
	pgqp_reg_params = params;	/* pgqp_reg_params is a global variable and is
								 * read by input_params()@param_scanner.l,
								 * which is called by param_parse(). */

	pre_param_parse(params_len);
	if (param_parse() != 0)
	{
		elog(WARNING, "Parse error in the regression params.");
		return false;
	}

#ifdef __DEBUG__
	show_reg_params("After set_reg_params");
#endif

	return true;
}


/*
 * Check whether the specified relids is contained in reg_params. If reg_params contains relids,
 * set the node type of the relids (T_Path or T_IndexPath) to rel_type, and return true;
 * otherwise, return false.
 */
bool
check_rel_param(NodeTag *rel_type, const Relids relids)
{
	int			i;

	*rel_type = T_Invalid;
	for (i = 0; i < reg_params.paramsize; i++)
	{
		if (reg_params.params[i].is_outer_relids_empty == true
			&& reg_params.params[i].is_inner_relids_empty == true)
		{
			if (bms_compare(relids, reg_params.params[i].relids) == 0)
			{
				*rel_type = reg_params.params[i].type;
				return true;
			}
		}
	}
	return false;
}

/*
 * Check whether the specified relids is contained in reg_params. If reg_params contains relids,
 * set the node type of the relids (T_MergePath, T_HashPath or T_NestPath) to join_type,
 * and return true; otherwise, return false.
 */
bool
check_join_param(NodeTag *join_type, bool *mergeflag, const Relids outer_relids, const Relids inner_relids)
{
	int			i;

	for (i = 0; i < reg_params.paramsize; i++)
	{
		if (reg_params.params[i].is_outer_relids_empty == false
			&& reg_params.params[i].is_inner_relids_empty == false)
		{
			if (bms_compare(outer_relids, reg_params.params[i].outer_relids) == 0
				&& bms_compare(inner_relids, reg_params.params[i].inner_relids) == 0)
			{
				*join_type = reg_params.params[i].type;
				*mergeflag = reg_params.params[i].mergeflag;
				return true;
			}
		}
	}
	return false;
}


/*
 * Get regression params and set join_type.
 */
static bool
get_join_param(NodeTag *join_type, const Relids outer_relids, const Relids inner_relids,
			   double *outer_coef, double *inner_coef, double *coef2, bool *is_coef2_null,
			   double *intercept, bool *is_intercept_null, bool *mergeflag)
{
	int			i;

	for (i = 0; i < reg_params.paramsize; i++)
	{
		if (reg_params.params[i].is_outer_relids_empty == false
			&& reg_params.params[i].is_inner_relids_empty == false)
		{

			if (bms_compare(outer_relids, reg_params.params[i].outer_relids) == 0
				&& bms_compare(inner_relids, reg_params.params[i].inner_relids) == 0)
			{
				NodeTag		type;

				*join_type = reg_params.params[i].type;
				type = *join_type;

				*outer_coef = reg_params.params[i].coef[0];

				if (type == T_MergePath || type == T_HashPath)
					*inner_coef = reg_params.params[i].coef[1];

				if (reg_params.params[i].is_coef2_null)
				{
					*is_coef2_null = true;
				}
				else
				{
					*coef2 = reg_params.params[i].coef2;
					*is_coef2_null = false;
				}

				if (reg_params.params[i].is_intercept_null)
				{
					*is_intercept_null = true;
				}
				else
				{
					*intercept = reg_params.params[i].intercept;
					*is_intercept_null = false;
				}

				*mergeflag = reg_params.params[i].mergeflag;

				return true;
			}
		}
	}

	return false;
}

/*
 * Get regression params and set rel_type.
 */
static bool
get_rel_param(const int rti, NodeTag *rel_type, double *coef, double *intercept,
			  bool *is_intercept_null, bool *mergeflag)
{
	int			i;

	for (i = 0; i < reg_params.paramsize; i++)
	{
		NodeTag		type = reg_params.params[i].type;

		if (bms_is_member(rti, reg_params.params[i].relids)
			&& (type != T_NestPath && type != T_MergePath && type != T_HashPath)
			&& bms_num_members(reg_params.params[i].relids) == 1
			&& reg_params.params[i].is_outer_relids_empty == true
			&& reg_params.params[i].is_inner_relids_empty == true)
		{
			*rel_type = reg_params.params[i].type;
			*coef = reg_params.params[i].coef[0];
			*is_intercept_null = reg_params.params[i].is_intercept_null;
			if (!reg_params.params[i].is_intercept_null)
				*intercept = reg_params.params[i].intercept;
			*mergeflag = reg_params.params[i].mergeflag;
			return true;
		}
	}
	return false;
}

/*
 * Adjust rel->rows using regression params.
 */
void
pgqp_set_rel_pathlist(PlannerInfo *root, RelOptInfo *rel,
					  Index rti, RangeTblEntry *rte)
{
	double		rows,
				adjusted_rows;
	ListCell   *lc;
	void	   *obj;
	Path		path;
	double		coef,
				intercept;
	bool		is_intercept_null,
				mergeflag;
	NodeTag		rel_type;

	/* call the previous hook */
	if (prev_set_rel_pathlist)
		prev_set_rel_pathlist(root, rel, rti, rte);

	if (!pgqp_adjust_rows)
		return;

	if (!pgqp_enable_adjust_rel_rows)
		return;

	if (IS_DUMMY_REL(rel) || (rel->reloptkind != RELOPT_BASEREL))
		return;

	/*
	 *
	 */
	if (!get_rel_param(rti, &rel_type, &coef, &intercept, &is_intercept_null, &mergeflag))
	{
		elog(DEBUG1, "The parameter of rti(%d) not found.", rti);
		return;
	}

#ifdef __DEBUG__
/* 	elog_node_display(LOG, "TEST BEFORE set_rel_pathlist", rel, true); */
#endif

	/*
	 * Calculate the adjusted rows using coefficients.
	 */
	rows = rel->rows;
	adjusted_rows = rows * coef;
	if (is_intercept_null == false)
		adjusted_rows += intercept;

	/*
	 * Adjust rows.
	 */
	/* pathlist */
	foreach(lc, rel->pathlist)
	{
		if ((obj = (void *) lfirst(lc)) == NULL)
			return;

		switch (nodeTag(obj))
		{
			case T_Path:
				switch (((Path *) obj)->pathtype)
				{
					case T_SeqScan: /* SeqScan */
						if (rel_type == T_IndexPath)
						{
							/* Because this path must be index scan. */
							((Path *) obj)->startup_cost = disable_cost;
							((Path *) obj)->total_cost = disable_cost;
						}
						else
						{
							/*
							 * Adjust rows
							 */
							rel->rows = adjusted_rows;
							/* Skip un-related nodes */
							if (rows != ((Path *) obj)->rows)
								break;
							((Path *) obj)->rows = adjusted_rows;
						}
						break;
					case T_SampleScan:	/* SampleScan */
					case T_FunctionScan:	/* FunctionScan */
					case T_TableFuncScan:	/* TableFuncScan */
					case T_ValuesScan:	/* ValuesScan */
					case T_CteScan: /* CteScan */
					case T_NamedTuplestoreScan: /* NamedTuplestoreScan */
					case T_Result:	/* Result */
					case T_WorkTableScan:	/* WorkTableScan */
					default:
						break;
				}
				break;

			case T_IndexPath:	/* IndexPath */
				path = (Path) (((IndexPath *) obj)->path);
				if (rel_type == T_Path)
				{
					/* Because this path must be seq scan. */
					path.startup_cost = disable_cost;
					path.total_cost = disable_cost;
				}
				else
				{
					/*
					 * Adjust rows
					 */
					/* Skip un-related nodes */
					if (rows != path.rows)
						break;
					path.rows = adjusted_rows;
					((IndexPath *) obj)->path = path;
					rel->rows = adjusted_rows;
				}
				break;
			case T_BitmapHeapPath:	/* BitmapHeapScan */
			case T_BitmapAndPath:	/* BitmapAndPath */
			case T_BitmapOrPath:	/* BitmapOrPath */
			case T_TidPath:		/* TidScan */
			case T_SubqueryScanPath:	/* SubqueryScan */
			case T_ForeignPath: /* ForeignScan */
			case T_CustomPath:	/* CustomScan */
			case T_AppendPath:	/* Append */
			case T_MergeAppendPath: /* MergeAppend */
			case T_GroupResultPath: /* GroupResult */
			case T_MaterialPath:	/* Material */
			case T_MemoizePath: /* Memoize */
			case T_UniquePath:	/* Unique */
			case T_GatherPath:	/* Gather */
			case T_GatherMergePath: /* GatherMerge */
			case T_ProjectionPath:	/* Projection */
			case T_ProjectSetPath:	/* ProjectSet */
			case T_SortPath:	/* Sort */
			case T_IncrementalSortPath: /* IncrementalSort */
			case T_GroupPath:	/* Group */
			case T_UpperUniquePath: /* UpperUnique */
			case T_AggPath:		/* Agg */
			case T_GroupingSetsPath:	/* GroupingSets */
			case T_MinMaxAggPath:	/* MinMaxAgg */
			case T_WindowAggPath:	/* WindowAgg */
			case T_SetOpPath:	/* SetOp */
			case T_RecursiveUnionPath:	/* RecursiveUnion */
			case T_LockRowsPath:	/* LockRows */
			case T_ModifyTablePath: /* ModifyTable */
			case T_LimitPath:	/* Limit */
			default:
				break;
		}
	}

	/*
	 * partial_pathlist
	 */
	foreach(lc, rel->partial_pathlist)
	{
		double		original_rows;

		if ((obj = (void *) lfirst(lc)) == NULL)
			return;

		switch (nodeTag(obj))
		{
			case T_Path:
				switch (((Path *) obj)->pathtype)
				{
					case T_SeqScan: /* SeqScan */
						if (rel_type == T_IndexPath)
						{
							/* Because this path must be index scan. */
							((Path *) obj)->startup_cost = disable_cost;
							((Path *) obj)->total_cost = disable_cost;
						}
						break;
					default:
						break;
				}
				break;

			case T_IndexPath:	/* IndexPath */
				path = (Path) (((IndexPath *) obj)->path);
				original_rows = path.rows;

				if (rel_type == T_Path)
				{
					/* Because this path must be seq scan. */
					path.startup_cost = disable_cost;
					path.total_cost = disable_cost;
				}
				else
				{
					path.rows = original_rows * coef;
					if (is_intercept_null == false)
						path.rows += intercept;
					((IndexPath *) obj)->path = path;

					/* TODO: Is this correct? */
					if (rel->rows > path.rows)
						rel->rows = path.rows;
				}
				break;
			default:
				break;
		}
	}

#ifdef __DEBUG__
/* 	elog_node_display(LOG, "TEST AFTER set_rel_pathlist", rel, true); */
#endif
}


/*
 * Adjust joinrel->rows using regression params.
 */
bool
adjust_joinrel_rows(RelOptInfo *joinrel, RelOptInfo *outer_rel, RelOptInfo *inner_rel, bool swap_rels)
{
	NodeTag		type;
	RelOptKind	reloptkind;
	double		outer_rows,
				inner_rows;
	double		outer_coef,
				inner_coef,
				coef2,
				intercept;
	bool		is_coef2_null,
				is_intercept_null,
				mergeflag;
	double		adjusted_rows;
#ifdef __DEBUG__
	char		_outer_relids[32];
	char		_inner_relids[32];
#endif

	if (!pgqp_adjust_rows)
		return true;

	if (!pgqp_enable_adjust_joinrel_rows)
		return true;

	reloptkind = joinrel->reloptkind;
	if (reloptkind != RELOPT_JOINREL
		&& reloptkind != RELOPT_OTHER_JOINREL)	/* TODO: Is this correct? */
		return true;

	/* To avoid compiler wornings. */
	outer_coef = inner_coef = coef2 = intercept = 0.0;
	is_coef2_null = is_intercept_null = true;

	outer_rows = outer_rel->rows;
	inner_rows = inner_rel->rows;

	/* Get regression params */
	if (!get_join_param(&type, outer_rel->relids, inner_rel->relids,
						&outer_coef, &inner_coef, &coef2, &is_coef2_null,
						&intercept, &is_intercept_null, &mergeflag))
	{
#ifdef __DEBUG__
		show_relids(outer_rel->relids, _outer_relids);
		show_relids(inner_rel->relids, _inner_relids);
		elog(LOG, "TEST %s <No Params>  outer=(%s)   inner=(%s)",
			 __func__, _outer_relids, _inner_relids);
#endif
		return false;
	}
#ifdef __DEBUG__
	else
	{
		show_relids(outer_rel->relids, _outer_relids);
		show_relids(inner_rel->relids, _inner_relids);
		elog(LOG, "TEST %s type=%s  outer=(%s)   inner=(%s)",
			 __func__, get_nodetype(type), _outer_relids, _inner_relids);
	}
#endif

	/* Calculate adjusted_rows using regresson params. */
	switch (type)
	{
		case T_NestPath:
			adjusted_rows = outer_coef * outer_rows * inner_rows;
#ifdef __DEBUG__
			elog(LOG, "TEST %s  adjusted_rows(%f) = outer_rows(%f) * inner_rows(%f) * outer_coef(%f)",
				 __func__, adjusted_rows, outer_rows, inner_rows, outer_coef);
#endif

			if (!is_intercept_null)
				adjusted_rows += intercept;
			break;
		case T_MergePath:
		case T_HashPath:
			if (!swap_rels)
			{
				adjusted_rows = outer_rows * outer_coef + inner_rows * inner_coef;
#ifdef __DEBUG__
				elog(LOG, "TEST %s  adjusted_rows(%f) = outer_rows(%f)*outer_coef(%f) + inner_rows(%f)*inner_coef(%f)",
					 __func__, adjusted_rows, outer_rows, outer_coef, inner_rows, inner_coef);
#endif
			}
			else
			{
				adjusted_rows = outer_rows * inner_coef + inner_rows * outer_coef;
#ifdef __DEBUG__
				elog(LOG, "TEST %s  adjusted_rows(%f) = outer_rows(%f)*inner_coef(%f) + inner_rows(%f)*outer_coef(%f)",
					 __func__, adjusted_rows, outer_rows, inner_coef, inner_rows, outer_coef);
#endif
			}

			if (!is_intercept_null)
				adjusted_rows += intercept;
#ifdef __DEBUG__
/* 			elog_node_display(LOG, "TEST adjust_joinrel_rows", joinrel, true); */
#endif
			break;
		default:
			adjusted_rows = joinrel->rows;
			break;
	};

	/* Set adjusted_rows */
	joinrel->rows = adjusted_rows;

	return true;
}

/*
 * From costsize.c
 */
static double
get_parallel_divisor(Path *path)
{
	double		parallel_divisor = path->parallel_workers;

	if (parallel_leader_participation)
	{
		double		leader_contribution;

		leader_contribution = 1.0 - (0.3 * path->parallel_workers);
		if (leader_contribution > 0)
			parallel_divisor += leader_contribution;
	}
	return parallel_divisor;
}

/*
 * set_join_pathlist hook
 *
 * This is for readjusting the rows, but I don't use it because it didn't work as expected.
 */
void
pgqp_set_join_pathlist(PlannerInfo *root, RelOptInfo *joinrel,
					   RelOptInfo *outerrel, RelOptInfo *innerrel,
					   JoinType jointype, JoinPathExtraData *extra)
{
	NodeTag		type = T_Invalid;
	RelOptKind	reloptkind;
	ListCell   *lc;
	void	   *obj;
	double		outer_coef,
				inner_coef,
				coef2,
				intercept;
	bool		is_coef2_null,
				is_intercept_null,
				mergeflag = false;
	double		re_adjusted_rows,
				joinrel_rows;

	/* call the previous hook */
	if (prev_set_join_pathlist)
		prev_set_join_pathlist(root, joinrel, outerrel, innerrel, jointype, extra);

	/*
	 * Pass through this function.
	 */
	return;

	if (!pgqp_adjust_rows)
		return;

	if (!pgqp_enable_adjust_joinrel_rows)
		return;

	reloptkind = joinrel->reloptkind;
	if (reloptkind != RELOPT_JOINREL
		&& reloptkind != RELOPT_OTHER_JOINREL)	/* TODO: Is this correct? */
		return;

	/* Get regression params */
	if (!get_join_param(&type, outerrel->relids, innerrel->relids,
						&outer_coef, &inner_coef, &coef2, &is_coef2_null,
						&intercept, &is_intercept_null, &mergeflag))
		return;

	if (!mergeflag)
		return;

	/* TODO-TODO is this correct? */
	if (type == T_NestPath)
		return;

#ifdef __DEBUG__
	elog_node_display(LOG, "TEST BEFORE pgqp_set_join_pathlist", joinrel, true);
#endif

	/*
	 * Re-adjust joinrel rows
	 */
	joinrel_rows = joinrel->rows;

	/* pathlist */
	foreach(lc, joinrel->pathlist)
	{
		JoinPath	jpath;
		double		parallel_divisor = 1.0;

		if ((obj = (void *) lfirst(lc)) == NULL)
			break;

		switch (nodeTag(obj))
		{
			case T_MergePath:
				jpath = (((MergePath *) obj)->jpath);
				if (jpath.path.parallel_workers > 0)
				{
					parallel_divisor = get_parallel_divisor(&jpath.path);
					re_adjusted_rows = clamp_row_est(jpath.path.rows * parallel_divisor);

					if (abs(jpath.path.rows - joinrel_rows) > joinrel_rows * 0.1)
					{
						jpath.path.rows = re_adjusted_rows;
						((MergePath *) obj)->jpath = jpath;
					}
				}

				break;
			case T_HashPath:

			default:
				break;
		}
	}

	/* partial_pathlist */
	foreach(lc, joinrel->partial_pathlist)
	{
		JoinPath	jpath;
		double		parallel_divisor = 1.0;

		if ((obj = (void *) lfirst(lc)) == NULL)
			break;

		switch (nodeTag(obj))
		{
			case T_MergePath:
				jpath = (((MergePath *) obj)->jpath);
				if (jpath.path.parallel_workers > 0)
				{
					parallel_divisor = get_parallel_divisor(&jpath.path);
					re_adjusted_rows = clamp_row_est(jpath.path.rows * parallel_divisor);

					if (abs(jpath.path.rows - joinrel_rows) > joinrel_rows * 0.1)
					{
						jpath.path.rows = re_adjusted_rows;
						((MergePath *) obj)->jpath = jpath;
					}
				}
				break;
			case T_HashPath:

			default:
				break;
		}
	}
#ifdef __DEBUG__
	elog_node_display(LOG, "TEST AFTER pgqp_set_join_pathlist", joinrel, true);
#endif

}

/*
 * Fetch rows from query_plan.reg table
 */
char *
selectParams(const char *queryid)
{
	Relation	rel = NULL;
	Oid			relationId;
	Oid			relationPkeyId;
	ScanKeyData scanKey;
	SysScanDesc scanDescriptor = NULL;
	int			scanKeyCount = 1;
	bool		indexOK = true;
	HeapTuple	heapTuple = NULL;

	char	   *params = NULL;

	relationId = get_relname_relid("reg", LookupExplicitNamespace("query_plan", true));
	relationPkeyId = get_relname_relid("reg_pkey", LookupExplicitNamespace("query_plan", true));

	if (relationId == InvalidOid)
		return NULL;

	rel = table_open(relationId, AccessShareLock);
	if (rel == NULL)
		return NULL;

	ScanKeyInit(&scanKey, Anum_reg_queryid,
				BTEqualStrategyNumber, F_TEXTEQ, CStringGetTextDatum(queryid));
	scanDescriptor = systable_beginscan(rel, relationPkeyId, indexOK, NULL, scanKeyCount, &scanKey);
	heapTuple = systable_getnext(scanDescriptor);

	while (HeapTupleIsValid(heapTuple))
	{
		TupleDesc	tupleDescriptor = RelationGetDescr(rel);
		bool		isNullArray[Natts_reg];
		Datum		datumArray[Natts_reg];

		heap_deform_tuple(heapTuple, tupleDescriptor, datumArray, isNullArray);
		if (!isNullArray[Anum_reg_params - 1])
		{
			params = TextDatumGetCString(datumArray[Anum_reg_params - 1]);
			break;
		}
		heapTuple = systable_getnext(scanDescriptor);
	}

	systable_endscan(scanDescriptor);
	table_close(rel, NoLock);

	return params;
}
