MODULE_big = pgpi
OBJS = pgpi.o core.o deparse.o detour.o obs_plan.o obs_exec.o obs_shm.o obs_worker.o stats.o
EXTENSION = pgpi
DATA = pgpi--0.4.sql
PG_CONFIG ?= pg_config

# Route D (run-time detours): x86_64, arm64 (Linux) or none. The default is this machine.
#   make PGPI_ARCH=arm64     make PGPI_ARCH=none
ifeq ($(shell uname -s),Darwin)
PGPI_ARCH ?= none
else
PGPI_ARCH ?= $(shell uname -m)
endif
ifneq ($(filter x86_64 amd64,$(PGPI_ARCH)),)
PG_CPPFLAGS += -DPGPI_DETOUR_X86_64
else ifneq ($(filter arm64 aarch64,$(PGPI_ARCH)),)
PG_CPPFLAGS += -DPGPI_DETOUR_ARM64
else ifeq ($(PGPI_ARCH),none)
PG_CPPFLAGS += -DPGPI_DETOUR_NONE
else
$(error PGPI_ARCH must be x86_64, arm64 or none (got $(PGPI_ARCH)))
endif
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
