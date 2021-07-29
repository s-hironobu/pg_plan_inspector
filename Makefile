# pg_plan_inspector/Makefile

MODULE_big = pg_query_plan
OBJS = src/pg_query_plan.o \
	src/pgqp_explain.o \
	src/buffer.o \
	src/hash.o \
	src/bgworker.o \
	src/qpam.o \
	pg_stat_statements/pg_stat_statements.o

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

