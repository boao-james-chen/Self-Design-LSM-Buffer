# Define paths
PATH_TO_LIBROCKSDB = ../../build/librocksdb.a

# Define compiler flags
CXXFLAGS += -pg -std=c++17 -O2 -DPROFILE

# Conditional flags
ifndef DISABLE_JEMALLOC
	ifdef JEMALLOC
		CXXFLAGS += -DROCKSDB_JEMALLOC -DJEMALLOC_NO_DEMANGLE $(JEMALLOC_INCLUDE)
		EXEC_LDFLAGS += $(JEMALLOC_LIB) -lpthread
	else
		EXEC_LDFLAGS += -lpthread
	endif
endif

ifneq ($(USE_RTTI), 1)
	CXXFLAGS += -fno-rtti
endif

# Targets
all: working_version 

working_version: working_version.cc db_env.cc stats.cc aux_time.cc parse_arguments.h config_options.h run_workload.h
	$(CXX) $(CXXFLAGS) $^ -o $@ $(PATH_TO_LIBROCKSDB) -I../../include -I../.. $(EXEC_LDFLAGS)

clean:
	rm -rf working_version
