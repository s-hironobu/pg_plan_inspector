# pg_plan_inspector/Makefile

MODULE_big = pg_query_plan
OBJS = 	src/pgqp_explain.o \
	src/buffer.o \
	src/hash.o \
	src/bgworker.o \
	src/planid_parser.o \
	src/planid_scanner.o \
	src/param_parser.o \
	src/param_scanner.o \
	src/qpam.o \
	src/adjust_rows.o \
	src/optimizer/pgqp_costsize.o \
	src/optimizer/pgqp_indexpath.o \
	src/optimizer/pgqp_inherit.o \
	src/optimizer/pgqp_initsplan.o \
	src/optimizer/pgqp_joinpath.o \
	src/optimizer/pgqp_joinrels.o \
	src/optimizer/pgqp_pathnode.o \
	src/optimizer/pgqp_planagg.o \
	src/optimizer/pgqp_planmain.o \
	src/optimizer/pgqp_planner.o \
	src/optimizer/pgqp_preunion.o \
	src/optimizer/pgqp_relnode.o \
	src/optimizer/pgqp_setrefs.o \
	src/optimizer/pgqp_subselect.o \
	src/optimizer/pgqp_allpaths.o \
	src/pg_query_plan.o \
	pg_stat_statements/pg_stat_statements.o

src/planid_scanner.c: FLEXFLAGS = -CF -p

src/planid_parser.c: BISONFLAGS += -d

src/param_scanner.c: FLEXFLAGS = -CF -p

src/param_parser.c: BISONFLAGS += -d

EXTRA_CLEAN = src/planid_parser.h src/planid_parser.c src/planid_scanner.c \
	src/param_parser.h src/param_parser.c src/param_scanner.c \
	src/*~ src/statistics/*~ src/optimizer/*~ pg_stat_statements/*~

DATA = pg_query_plan--1.0.sql

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_plan_inspector
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

