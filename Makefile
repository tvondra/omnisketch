MODULE_big = omnisketch
OBJS = omnisketch.o

EXTENSION = omnisketch
DATA = omnisketch--1.0.0.sql
MODULES = omnisketch

CFLAGS=`pg_config --includedir-server`

REGRESS      = basic parallel_query
REGRESS_OPTS = --inputdir=test

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
