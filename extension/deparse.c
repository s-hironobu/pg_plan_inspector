/*-------------------------------------------------------------------------
 * deparse.c
 *	  Turn planner clauses into self-contained SQL text.
 *
 * The output uses the range-table aliases of the query, so that an external
 * estimator can rebuild "SELECT count(*) FROM <rels> WHERE <clauses>".
 * Only a well-understood subset of expressions is supported; anything else
 * makes pgpi_deparse_clause() return NULL and the clause is reported as
 * opaque.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "nodes/nodeFuncs.h"
#include "nodes/pathnodes.h"
#include "parser/parsetree.h"
#include "utils/builtins.h"
#include "utils/json.h"
#include "utils/lsyscache.h"

#include "pgpi.h"

static bool deparse_node(PlannerInfo *root, Node *node, StringInfo buf);

static bool
deparse_args(PlannerInfo *root, List *args, const char *sep, StringInfo buf)
{
	ListCell   *lc;
	bool		first = true;

	foreach(lc, args)
	{
		if (!first)
			appendStringInfoString(buf, sep);
		first = false;
		if (!deparse_node(root, (Node *) lfirst(lc), buf))
			return false;
	}
	return true;
}

static bool
deparse_var(PlannerInfo *root, Var *var, StringInfo buf)
{
	RangeTblEntry *rte;
	char	   *attname;

	if (var->varlevelsup != 0 || var->varattno <= 0)
		return false;
	if (var->varno <= 0 || var->varno >= root->simple_rel_array_size)
		return false;
	rte = root->simple_rte_array[var->varno];
	if (rte == NULL || rte->rtekind != RTE_RELATION)
		return false;
	attname = get_attname(rte->relid, var->varattno, true);
	if (attname == NULL)
		return false;
	appendStringInfo(buf, "%s.%s",
					 quote_identifier(rte->eref->aliasname),
					 quote_identifier(attname));
	return true;
}

static bool
deparse_const(Const *c, StringInfo buf)
{
	Oid			typoutput;
	bool		typisvarlena;
	char	   *s;

	if (c->constisnull)
	{
		appendStringInfo(buf, "NULL::%s",
						 format_type_with_typemod(c->consttype, c->consttypmod));
		return true;
	}
	getTypeOutputInfo(c->consttype, &typoutput, &typisvarlena);
	s = OidOutputFunctionCall(typoutput, c->constvalue);
	appendStringInfo(buf, "%s::%s", quote_literal_cstr(s),
					 format_type_with_typemod(c->consttype, c->consttypmod));
	return true;
}

static bool
deparse_node(PlannerInfo *root, Node *node, StringInfo buf)
{
	if (node == NULL)
		return false;

	switch (nodeTag(node))
	{
		case T_RestrictInfo:
			return deparse_node(root, (Node *) ((RestrictInfo *) node)->clause, buf);

		case T_Var:
			return deparse_var(root, (Var *) node, buf);

		case T_Const:
			return deparse_const((Const *) node, buf);

		case T_RelabelType:
			return deparse_node(root, (Node *) ((RelabelType *) node)->arg, buf);

		case T_OpExpr:
			{
				OpExpr	   *op = (OpExpr *) node;
				char	   *opname = get_opname(op->opno);

				if (opname == NULL)
					return false;
				appendStringInfoChar(buf, '(');
				if (list_length(op->args) == 2)
				{
					if (!deparse_node(root, linitial(op->args), buf))
						return false;
					appendStringInfo(buf, " %s ", opname);
					if (!deparse_node(root, lsecond(op->args), buf))
						return false;
				}
				else if (list_length(op->args) == 1)
				{
					appendStringInfo(buf, "%s ", opname);
					if (!deparse_node(root, linitial(op->args), buf))
						return false;
				}
				else
					return false;
				appendStringInfoChar(buf, ')');
				return true;
			}

		case T_DistinctExpr:
			{
				DistinctExpr *op = (DistinctExpr *) node;

				if (list_length(op->args) != 2)
					return false;
				appendStringInfoChar(buf, '(');
				if (!deparse_node(root, linitial(op->args), buf))
					return false;
				appendStringInfoString(buf, " IS DISTINCT FROM ");
				if (!deparse_node(root, lsecond(op->args), buf))
					return false;
				appendStringInfoChar(buf, ')');
				return true;
			}

		case T_ScalarArrayOpExpr:
			{
				ScalarArrayOpExpr *sa = (ScalarArrayOpExpr *) node;
				char	   *opname = get_opname(sa->opno);

				if (opname == NULL || list_length(sa->args) != 2)
					return false;
				appendStringInfoChar(buf, '(');
				if (!deparse_node(root, linitial(sa->args), buf))
					return false;
				appendStringInfo(buf, " %s %s (", opname, sa->useOr ? "ANY" : "ALL");
				if (!deparse_node(root, lsecond(sa->args), buf))
					return false;
				appendStringInfoString(buf, "))");
				return true;
			}

		case T_ArrayExpr:
			{
				ArrayExpr  *ae = (ArrayExpr *) node;

				appendStringInfoString(buf, "ARRAY[");
				if (!deparse_args(root, ae->elements, ", ", buf))
					return false;
				appendStringInfo(buf, "]::%s", format_type_be(ae->array_typeid));
				return true;
			}

		case T_BoolExpr:
			{
				BoolExpr   *be = (BoolExpr *) node;

				appendStringInfoChar(buf, '(');
				switch (be->boolop)
				{
					case AND_EXPR:
						if (!deparse_args(root, be->args, " AND ", buf))
							return false;
						break;
					case OR_EXPR:
						if (!deparse_args(root, be->args, " OR ", buf))
							return false;
						break;
					case NOT_EXPR:
						appendStringInfoString(buf, "NOT ");
						if (!deparse_node(root, linitial(be->args), buf))
							return false;
						break;
				}
				appendStringInfoChar(buf, ')');
				return true;
			}

		case T_NullTest:
			{
				NullTest   *nt = (NullTest *) node;

				appendStringInfoChar(buf, '(');
				if (!deparse_node(root, (Node *) nt->arg, buf))
					return false;
				appendStringInfoString(buf, nt->nulltesttype == IS_NULL ?
									   " IS NULL)" : " IS NOT NULL)");
				return true;
			}

		case T_BooleanTest:
			{
				BooleanTest *bt = (BooleanTest *) node;
				const char *s = NULL;

				switch (bt->booltesttype)
				{
					case IS_TRUE:
						s = " IS TRUE)";
						break;
					case IS_NOT_TRUE:
						s = " IS NOT TRUE)";
						break;
					case IS_FALSE:
						s = " IS FALSE)";
						break;
					case IS_NOT_FALSE:
						s = " IS NOT FALSE)";
						break;
					case IS_UNKNOWN:
						s = " IS UNKNOWN)";
						break;
					case IS_NOT_UNKNOWN:
						s = " IS NOT UNKNOWN)";
						break;
				}
				appendStringInfoChar(buf, '(');
				if (!deparse_node(root, (Node *) bt->arg, buf))
					return false;
				appendStringInfoString(buf, s);
				return true;
			}

		case T_CoerceViaIO:
			{
				CoerceViaIO *cv = (CoerceViaIO *) node;

				appendStringInfoChar(buf, '(');
				if (!deparse_node(root, (Node *) cv->arg, buf))
					return false;
				appendStringInfo(buf, ")::%s", format_type_be(cv->resulttype));
				return true;
			}

		case T_FuncExpr:
			{
				FuncExpr   *fe = (FuncExpr *) node;

				if (fe->funcformat == COERCE_IMPLICIT_CAST ||
					fe->funcformat == COERCE_EXPLICIT_CAST)
				{
					if (list_length(fe->args) < 1)
						return false;
					appendStringInfoChar(buf, '(');
					if (!deparse_node(root, linitial(fe->args), buf))
						return false;
					appendStringInfo(buf, ")::%s", format_type_be(fe->funcresulttype));
					return true;
				}
				else if (fe->funcformat == COERCE_EXPLICIT_CALL)
				{
					char	   *fname = get_func_name(fe->funcid);

					if (fname == NULL || fe->funcvariadic)
						return false;
					appendStringInfo(buf, "%s(", quote_identifier(fname));
					if (!deparse_args(root, fe->args, ", ", buf))
						return false;
					appendStringInfoChar(buf, ')');
					return true;
				}
				return false;
			}

		default:
			return false;
	}
}

/*
 * Deparse one clause (RestrictInfo or expression). Return a palloc'd
 * string, or NULL if the clause uses an unsupported construct.
 */
char *
pgpi_deparse_clause(PlannerInfo *root, Node *clause)
{
	StringInfoData buf;

	initStringInfo(&buf);
	if (!deparse_node(root, clause, &buf))
	{
		pfree(buf.data);
		return NULL;
	}
	return buf.data;
}

/* Append s as a JSON string literal. */
void
pgpi_append_json_string(StringInfo buf, const char *s)
{
	if (s == NULL)
	{
		appendStringInfoString(buf, "null");
		return;
	}
	escape_json(buf, s);
}
