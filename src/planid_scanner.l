%{
#include "planid_parser.h"
#include <string.h>

extern void yyerror(const char * message);

void init_input_buf(const int plan_len);

void __dummy_input(void);
static int input_json(char *buf, int max_size);

extern char *pgqp_json_plan;

/*
 * Helper structure and variable for input_json() function.
 */
struct _InputBufInfo
{
	int plan_len;
	int offset;
} _InputBufInfo;

static struct _InputBufInfo bufInfo;

#undef YY_INPUT
#define YY_INPUT(buf,result,size) \
 (result = input_json(buf,size))

/* #define __DEBUG__ */
#ifdef __DEBUG__
#define DEBUG_ECHO fwrite( yytext, yyleng, 1, yyout ); fflush(yyout);
#else
#define DEBUG_ECHO
#endif

%}

%option noyywrap

%option prefix="plan_"

%%

"{" { DEBUG_ECHO; return _LCURLY; }
"}" { DEBUG_ECHO; return _RCURLY; }
"[" { DEBUG_ECHO; return _LBRAC; }
"]" { DEBUG_ECHO; return _RBRAC; }
"," { DEBUG_ECHO; return _COMMA; }
":" { DEBUG_ECHO; return _COLON; }
("true"|"TRUE") { DEBUG_ECHO; return VTRUE; }
("false"|"FALSE") { DEBUG_ECHO; return VFALSE; }
("null"|"NULL") { DEBUG_ECHO; return VNULL; }

("\"Plan\""|"\"Plans\"") { plan_lval.plan = strdup(yytext); DEBUG_ECHO; return PLAN; }

("\"Filter\""|"\"Join Filter\""|"\"Conflict Filter\""|"\"Merge Cond\""|"\"Hash Cond\""|"\"Index Cond\""|"\"TID Cond\""|"\"Recheck Cond\"") { plan_lval.key = strdup(yytext); DEBUG_ECHO; return KEY; }

("\"Node Type\""|"\"Parent Relationship\""|"\"Relation Name\""|"\"Function Name\""|"\"Index Name\""|"\"CTE Name\""|"\"Relation\""|"\"Schema\""|"\"Alias\""|"\"Output\""|"\"Scan Direction\""|"\"Strategy\""|"\"Join Type\""|"\"Sort Method\""|"\"Sort Key\""|"\"Operation\""|"\"Subplan Name\""|"\"Command\""|"\"Triggers\""|"\"Trigger\""|"\"Trigger Name\""|"\"Constraint Name\""|"\"Group Key\""|"\"Grouping Sets\""|"\"Group Keys\""|"\"Hash Keys\""|"\"Hash Key\""|"\"Partial Mode\""|"\"Inner Unique\"") { plan_lval.key_with_value = strdup(yytext); DEBUG_ECHO; return KEY_WITH_VALUE; }

[0-9]+ { DEBUG_ECHO; plan_lval.integer = atoi(yytext); return _INTEGER; }

[0-9]+"."?[0-9]* { DEBUG_ECHO; plan_lval.decimal = atof(yytext); return _DECIMAL; }

\"[^"]*\" { plan_lval.string = strdup(yytext); DEBUG_ECHO; return _STRING; }

[ \t\r\n]+ { /* eat up whitespace */ }

%%

/*
 * Initialize the bufInfo variable. This is called by pre_plan_parse()@planid_parser.y.
 */
void
init_input_buf(const int plan_len)
{
	bufInfo.plan_len = plan_len;
	bufInfo.offset = 0;
}

static int
input_json(char *buf, int max_size)
{
	int i, j;
	int start = bufInfo.offset;
	int end = ((start + max_size) < (bufInfo.plan_len)) ? (start + max_size) : bufInfo.plan_len;
	int result = end - start;

	j = 0;
	for (i = start; i < end; i++)
		buf[j++] = pgqp_json_plan[i];

	bufInfo.offset += result;
#ifdef __DEBUG__
	elog(LOG, "\nYY_INPUT (%d/%d) start=%d  end=%d  offset=%d  plan_len=%d\n",
		max_size, result, start, end, bufInfo.offset, bufInfo.plan_len);
#endif
	return result;
}

/*
 * To avoid the compiler warnings.
 */
void
__dummy_input(void)
{
	int c = 0;
	register char *yy_bp = NULL;
	yyunput(c, yy_bp);
	input();
};
