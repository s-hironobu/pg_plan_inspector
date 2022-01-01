/*-------------------------------------------------------------------------
 * planid_parser.y
 *
 * planid_parser.y and planid_scanner.l are to get the planid from a given
 * json plan.
 *
 * The algorithm for calculating the planid is rough. It decomposes the given
 * json plan, packs only the minimum required keys and values into planid_buf,
 * and calculates the hash value of the string in the planid_buf.
 *
 * This algorithm uses only the minimum data of a given json plan and also
 * loses the json plan tree structure information when packing into planid_buf.
 * Therefore, it is not generally always possible to identify the plan only by
 * the obtained planid. However, the computed planid is always used in pairs
 * with the queryid, so this algorithm is sufficient to identify the given plans.
 *
 * Copyright (c) 2021-2022, Hironobu Suzuki @ interdb.jp
 *
 *-------------------------------------------------------------------------
 */

%{
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "postgres.h"
#include "common/hashfn.h"

	/*
	 * Declear functions
	 */
	uint64 get_planId(void);
	void pre_plan_parse(const int plan_len);

	static bool check_parent_relationship(const char *key, const char *value);
	static void set_depth(void);
	static char *get_depth(void);
	static void add_buf(char *);

	/*
	 * Initialize the InputBufInfo's variables in planid_scanner.l
	 */
	extern void init_input_buf(const int plan_len);

	/*
	 * A temporal buffer to hold the string retrieved from pgqp_json_plan.
	 */
#define PLANID_BUF_SIZE 128*1024 /* 128 KB */
	struct
	{
		/*
		 * Buffer
		 */
		unsigned char buf[PLANID_BUF_SIZE];
		int len; /* The length of the buffered string. */

		/*
		 * Support variable and charactor array.
		 */
		int depth; /* Depth of the node in the plan tree. */
		char c_depth[16]; /* Store the converted string from integer `depth`. */
	} planid_buf;

	/*
	 * flex's variables and functions.
	 */
/*#define YYDEBUG 1 */
	int yydebug = 0;

	extern int plan_lineno;	/* Changed the variable name from 'yylineno'
							 * to 'plan_lineno' by setting '%option prefix="plan_"'
							 * in planid_scanner.l
							 */
	extern char* yytext;
	int yylex();
	void yyerror(const char *s);

%}

%name-prefix "plan_"

%union {
	char *plan;
	char *key;
	char *key_with_value;
	char *string;
	int integer;
	double decimal;
}

%token <plan> PLAN
%token <key> KEY
%token <key_with_value> KEY_WITH_VALUE
%token LCURLY RCURLY LBRAC RBRAC COMMA COLON
%token VTRUE VFALSE VNULL
%token <string> STRING
%token <decimal> DECIMAL
%token <integer> INTEGER

%start json

%%

json: object
    ;

object: LCURLY RCURLY
      | LCURLY members RCURLY
      ;

members: member
       | members COMMA member
       ;

member: PLAN COLON value
      | KEY COLON value {
          add_buf($1);
        }
      | KEY_WITH_VALUE COLON STRING {
		  /*
		   * Add the depth info of the plan tree if the key is "Parent Relationship"
		   * to reflect the plan tree structure to the planid.
		   * Of course, this will not fully reflect the structure, but it is better than
		   * doing nothing.
		   */
          if (check_parent_relationship($1, $3) == true)
			  add_buf(get_depth());
		  /* Add key and value to planid_buf.buf. */
          add_buf($1);
          add_buf($3);
        }
      | KEY_WITH_VALUE COLON DECIMAL
	  | KEY_WITH_VALUE COLON INTEGER
	  | KEY_WITH_VALUE COLON VTRUE
	  | KEY_WITH_VALUE COLON VFALSE
	  | KEY_WITH_VALUE COLON VNULL
	  | KEY_WITH_VALUE COLON array
	  | KEY_WITH_VALUE COLON object
      | STRING COLON value
      ;

array: LBRAC RBRAC
     | LBRAC members RBRAC
     | LBRAC elements RBRAC
     ;

elements : value
         | value COMMA elements
         ;

value: object
     | STRING
     | DECIMAL
     | INTEGER
     | array
     | VTRUE
     | VFALSE
     | VNULL
     ;

%%

void
yyerror(const char *s)
{
	fprintf(stderr,"error: %s on line %d\n", s, plan_lineno);
}

/*
 * Get the planid from the plan stored in planid_buf.buf.
 */
uint64
get_planId(void)
{
#ifdef __DEBUG__
	elog(LOG, "buf=%s", planid_buf.buf);
#endif
	return hash_bytes_extended(planid_buf.buf, planid_buf.len, 0);
}

/*
 * Initialize planid_buf and bufInfo@planid_scanner.l.
 * This should be called before plan_parse().
 */
void
pre_plan_parse(const int plan_len)
{
	planid_buf.len = 0;
	memset(planid_buf.buf, '\0', sizeof(planid_buf.buf));

	planid_buf.depth = 0;

	init_input_buf(plan_len);
}

/*
 * Check the key and increase/decrease planid_buf.depth if the key is
 * "Parent Relationship".
 */
static bool
check_parent_relationship(const char *key, const char *value)
{
	if (strcmp(key, "\"Parent Relationship\"") == 0)
	{
		if (strcmp(value, "\"Outer\"") == 0)
		{
			planid_buf.depth += 1;
			set_depth();
			return true;
		}
		else if (strcmp(value, "\"Inner\"") == 0)
		{
			set_depth();
			planid_buf.depth -= 1;
			return true;
		}
	}

	return false;
}

/*
 * Set the string converted from the integer planid_buf.depth
 * to the planid_buf.c_depth.
 */
static void
set_depth(void)
{
	memset(planid_buf.c_depth, '\0', sizeof(planid_buf.c_depth));
	sprintf(planid_buf.c_depth, ":depth=%d:", planid_buf.depth);
}

static char*
get_depth(void)
{
	return planid_buf.c_depth;
}


/*
 * Add string 's' to the planid_buf.
 */
static void
add_buf(char *s)
{
	int offset = planid_buf.len;
	int len = strlen(s);
	unsigned char *dest = &planid_buf.buf[offset];

	if (offset + len < PLANID_BUF_SIZE - 1)
	{
		char *b = s;
		if (len > 0 && s[len-1] == '"')
			len--;
		if (len > 0 && s[0] == '"')
		{
			b = &s[1];
			len--;
		}
		memcpy(dest, b, len);
		planid_buf.len += len;
	}
}
