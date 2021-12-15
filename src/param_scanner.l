%{
#include "param_parser.h"
#include <string.h>

extern void yyerror(const char * message);

void init_input_param_buf(const int params_len);

void __dummy_input2(void);
static int input_params(char *buf, int max_size);

extern char *pgqp_reg_params;

/*
 * Helper structure and variable for input_params() function.
 */
struct InputBufInfo
{
	int params_len;
	int offset;
} InputBufInfo;

static struct InputBufInfo bufInfo;

#undef YY_INPUT
#define YY_INPUT(buf,result,size) \
 (result = input_params(buf,size))

//#define __DEBUG__
#ifdef __DEBUG__
#define DEBUG_ECHO fprintf(stderr, "%s", yytext); fflush(yyout);
#else
#define DEBUG_ECHO
#endif

%}

%option noyywrap

%option prefix="param_"

%%

"{" { DEBUG_ECHO; return LCURLY; }
"}" { DEBUG_ECHO; return RCURLY; }
"[" { DEBUG_ECHO; return LBRAC; }
"]" { DEBUG_ECHO; return RBRAC; }
"(" { DEBUG_ECHO; return LB; }
")" { DEBUG_ECHO; return RB; }
"," { DEBUG_ECHO; return COMMA; }
":" { DEBUG_ECHO; return COLON; }
";"  { DEBUG_ECHO; return SEMICOLON; }

[-]*[0-9]+ { DEBUG_ECHO; param_lval.integer = atoi(yytext); return INTEGER; }

[-]*[0-9]+"."?"e-"?"e+"?[0-9]+ { DEBUG_ECHO; param_lval.decimal = atof(yytext); return DECIMAL; }

\"[^"]*\" { param_lval.string = strdup(yytext); DEBUG_ECHO; return STRING; }

[ \t\r\n]+ { /* eat up whitespace */ }

%%

/*
 * Initialize the bufInfo variable. This is called by pre_param_parse()@param_parser.y.
 */
void
init_input_param_buf(const int params_len)
{
	bufInfo.params_len = params_len;
	bufInfo.offset = 0;
}

static int
input_params(char *buf, int max_size)
{
	int i, j;
	int start = bufInfo.offset;
	int end = ((start + max_size) < (bufInfo.params_len)) ? (start + max_size) : bufInfo.params_len;
	int result = end - start;
#ifdef __DEBUG__
	fprintf(stderr, "\nBEFORE YY_INPUT (%d/%d) start=%d  end=%d  offset=%d  params_len=%d\n",
		max_size, result, start, end, bufInfo.offset, bufInfo.params_len);
#endif
	j = 0;
	for (i = start; i < end; i++)
		buf[j++] = pgqp_reg_params[i];

	bufInfo.offset += result;
#ifdef __DEBUG__
	fprintf(stderr, "\nYY_INPUT (%d/%d) start=%d  end=%d  offset=%d  params_len=%d\n",
		max_size, result, start, end, bufInfo.offset, bufInfo.params_len);
#endif
	return result;
}

/*
 * To avoid the compiler warnings.
 */
void
__dummy_input2(void)
{
	int c = 0;
	register char *yy_bp = NULL;
	yyunput(c, yy_bp);
	input();
};
