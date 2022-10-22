/*-------------------------------------------------------------------------
 * param_parser.y
 *
 * param_parser.y and param_scanner.l are to parase the regression params
 * in the query_plan.reg table.
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
#include "adjust_rows.h"

	/*
	 * Declear functions
	 */
	void pre_param_parse(const int params_len);
	void init_param_parse_env(void);

	/*
	 * Initialize the InputBufInfo's variables in param_scanner.l
	 */
	extern void init_input_param_buf(const int params_len);

	/*
	 * A temporal buffer to hold the string retrieved from pgqp_reg_params.
	 */
#define PARAM_BUF_SIZE 128*1024 /* 128 KB */
	struct
	{
		unsigned char buf[PARAM_BUF_SIZE];
		int len; /* The length of the buffered string. */
	} param_buf;
#undef PARAM_BUF_SIZE

	/*
	 *
	 */
	struct
	{
		int num_node; /* The counter of the number of nodes. */
		Relids relids;
		int rti;
	} temp_data;

	/*
	 *
	 */
	extern regParams reg_params;

	/*
	 * flex's variables and functions.
	 */
//#define YYDEBUG
	int yydebug = 1;

	extern int param_lineno;	/* Changed the variable name from 'yylineno'
								 * to 'param_lineno' by setting '%option prefix="param_"'
								 * in param_scanner.l
								 */
	extern char* yytext;
	int yylex();
	void yyerror(const char *s);

%}

%define api.prefix {param_}
/* If bison makes error because of old version, use %name-prefix "param_" */

%union {
	char   *string;
	int    integer;
	double decimal;
}

%token LCURLY RCURLY LBRAC RBRAC LB RB COMMA COLON SEMICOLON
%token <string> STRING
%token <decimal> DECIMAL
%token <integer> INTEGER

%start paramset

%%

paramset: param
        | paramset SEMICOLON param
        ;

param: LCURLY items RCURLY {
                temp_data.num_node++;
      }
;

items: nodetype COLON rtable COLON outertable COLON innertable COLON coef COLON coef2 COLON intercept COLON mergeflag
     ;

nodetype: STRING {
                set_nodeid(temp_data.num_node, $1);
        }
        ;

rtable: LB RB {
      }
      | LB STRING RB {
                temp_data.rti = relname2rti($2);
				if (temp_data.rti != -1)
				{
				        add_relids(temp_data.relids, temp_data.rti);
				        set_relids(temp_data.num_node, temp_data.relids);
				}
				/* Reset temp_data.relids */
				temp_data.relids = bms_del_members(temp_data.relids, temp_data.relids);
      }
      | LB rtables RB {
				set_relids(temp_data.num_node, temp_data.relids);
				/* Reset temp_data.relids */
				temp_data.relids = bms_del_members(temp_data.relids, temp_data.relids);
      }
      ;

outertable: LB RB {
          }
          | LB STRING RB {
                temp_data.rti = relname2rti($2);
				if (temp_data.rti != -1)
				{
				        add_relids(temp_data.relids, temp_data.rti);
						set_outer_relids(temp_data.num_node, temp_data.relids);
				}
				/* Reset temp_data.relids */
				temp_data.relids = bms_del_members(temp_data.relids, temp_data.relids);
          }
          | LB rtables RB {
				set_outer_relids(temp_data.num_node, temp_data.relids);
				/* Reset temp_data.relids */
				temp_data.relids = bms_del_members(temp_data.relids, temp_data.relids);
          }
          ;

innertable: LB RB {
 }
          | LB STRING RB {
                temp_data.rti = relname2rti($2);
				if (temp_data.rti != -1)
				{
				        add_relids(temp_data.relids, temp_data.rti);
						set_inner_relids(temp_data.num_node, temp_data.relids);
				}
				/* Reset temp_data.relids */
				temp_data.relids = bms_del_members(temp_data.relids, temp_data.relids);
         }
         | LB rtables RB {
				set_inner_relids(temp_data.num_node, temp_data.relids);
				/* Reset temp_data.relids */
				temp_data.relids = bms_del_members(temp_data.relids, temp_data.relids);
         }
         ;

rtables: STRING COMMA STRING {
                temp_data.rti = relname2rti($1);
				if (temp_data.rti != -1)
				{
				        add_relids(temp_data.relids, temp_data.rti);
				}
                temp_data.rti = relname2rti($3);
				if (temp_data.rti != -1)
				{
				        add_relids(temp_data.relids, temp_data.rti);
				}
       }
       | rtables COMMA STRING {
                temp_data.rti = relname2rti($3);
				if (temp_data.rti != -1)
				{
				       add_relids(temp_data.relids, temp_data.rti);
				}
       }
       ;

coef: LBRAC RBRAC {
     }
    | LBRAC DECIMAL RBRAC {
				set_coef(temp_data.num_node, $2,  /* dummy */ 0);
    }
    | LBRAC DECIMAL COMMA DECIMAL  RBRAC {
				set_coef(temp_data.num_node, $2, $4);
    }
    ;

coef2: LBRAC RBRAC {
     }
     | LBRAC DECIMAL RBRAC {
				set_coef2(temp_data.num_node, $2);
     }
     ;

intercept: LBRAC RBRAC {
         }
         | LBRAC DECIMAL RBRAC {
				set_intercept(temp_data.num_node, $2);
         }
         ;

mergeflag: STRING {
	/* Assume that $1 is a string 'True' or 'False'. */
                set_mergeflag(temp_data.num_node, $1);
        }
        ;

%%

void
yyerror(const char *s)
{
	fprintf(stderr,"error: %s on line %d\n", s, param_lineno);
}

/*
 * Initialize param_buf and bufInfo@param_scanner.l.
 * This should be called just before param_parse().
 */
void
pre_param_parse(const int params_len)
{
	/* Initialize param_buf */
	param_buf.len = 0;
	memset(param_buf.buf, '\0', sizeof(param_buf.buf));

	/* Initialize temp_data */
	temp_data.num_node = 0;

	/* Reset temp_data.relids */
	if (!bms_is_empty(temp_data.relids))
		temp_data.relids = bms_del_members(temp_data.relids, temp_data.relids);
	Assert(bms_is_empty(temp_data.relids));

	init_input_param_buf(params_len);
}

void
init_param_parse_env(void)
{
	temp_data.relids = bms_make_singleton(0); /* Make dummy */
	temp_data.relids = bms_del_member(temp_data.relids, 0);
	Assert(bms_is_empty(temp_data.relids));
}
