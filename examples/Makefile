include ../make_config.mk

ifndef DISABLE_JEMALLOC
	ifdef JEMALLOC
		PLATFORM_CXXFLAGS += -DROCKSDB_JEMALLOC -DJEMALLOC_NO_DEMANGLE
	endif
	EXEC_LDFLAGS := $(JEMALLOC_LIB) $(EXEC_LDFLAGS) -lpthread
	PLATFORM_CXXFLAGS += $(JEMALLOC_INCLUDE)
endif

ifneq ($(USE_RTTI), 1)
	CXXFLAGS += -fno-rtti
endif


.PHONY: clean librocksdb

CXX = g++
CXX_FLAGS += -std=c++1y -O0 -ggdb -g -Wall #-Werror #-fPIC
# use this next NDEBUG declaration to avoid asserts
CXX_FLAGS += -DNDEBUG

# LD_FLAGS = -lz -lbz2 
LIB_ROCKSDB = ../librocksdb.a
INC_PATH = -I../include

UPDATED_PLATFORM_CXXFLAGS := $(filter-out -std=c++11,$(PLATFORM_CXXFLAGS))

all: dostoevsky simple_example test_example column_families_example compact_files_example   c_simple_example optimistic_transaction_example transaction_example compaction_filter_example options_file_example

simple_example: librocksdb simple_example.cc
	$(CXX) $(CXX_FLAGS) $(LD_FLAGS) $@.cc -o$@ $(LIB_ROCKSDB) $(INC_PATH)  $(PLATFORM_LDFLAGS) $(UPDATED_PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS) 

test_example: librocksdb test_example.cc
	$(CXX) $(CXX_FLAGS) $(LD_FLAGS) $@.cc -o$@ $(LIB_ROCKSDB) $(INC_PATH)  $(PLATFORM_LDFLAGS) $(UPDATED_PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS) 

column_families_example: librocksdb column_families_example.cc
	$(CXX) $(CXX_FLAGS) $(LD_FLAGS) $@.cc -o$@ $(LIB_ROCKSDB) $(INC_PATH)  $(PLATFORM_LDFLAGS) $(UPDATED_PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

compaction_filter_example: librocksdb compaction_filter_example.cc
	$(CXX) $(CXX_FLAGS) $(LD_FLAGS) $@.cc -o$@ $(LIB_ROCKSDB) $(INC_PATH)  $(PLATFORM_LDFLAGS) $(UPDATED_PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

compact_files_example: librocksdb compact_files_example.cc
	$(CXX) $(CXX_FLAGS) $(LD_FLAGS) $@.cc -o$@ $(LIB_ROCKSDB) $(INC_PATH)  $(PLATFORM_LDFLAGS) $(UPDATED_PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)
	
bloom_filters: bloom_filters.cc    
	$(CXX) $(CXX_FLAGS) $@.cc   $(INC_PATH) -c  

dostoevsky: librocksdb  bloom_filters  dostoevsky.cc  
	$(CXX) $(CXX_FLAGS) dostoevsky.cc $(INC_PATH) -c    

# forycsb: dostoevsky
# 	$(CXX) $(CXX_FLAGS) dostoevsky.o bloom_filters.o -shared -o libdostoevsky.so

holistic_tuner: holistic_tuner.cc  
	$(CXX) $(CXX_FLAGS) holistic_tuner.cc $(INC_PATH) -c    

holistic_tuner_test: holistic_tuner  holistic_tuner_test.cc  
	$(CXX) $(CXX_FLAGS) $@.cc -o$@ holistic_tuner.o $(INC_PATH) 

dostoevsky_test:  dostoevsky  dostoevsky_test.cc  
	$(CXX) $(CXX_FLAGS) $(LD_FLAGS) $@.cc -o$@ $(LIB_ROCKSDB) dostoevsky.o bloom_filters.o $(INC_PATH) $(PLATFORM_LDFLAGS) $(UPDATED_PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)


.c.o:
	$(CC) $(CFLAGS) -c $< -o $@ $(INC_PATH)

c_simple_example: librocksdb c_simple_example.o
	$(CXX) $(CXX_FLAGS) $(LD_FLAGS) $@.o -o$@ $(LIB_ROCKSDB) $(INC_PATH)  $(PLATFORM_LDFLAGS) $(UPDATED_PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

optimistic_transaction_example: librocksdb optimistic_transaction_example.cc
	$(CXX) $(CXX_FLAGS) $(LD_FLAGS) $@.cc -o$@ $(LIB_ROCKSDB) $(INC_PATH) $(PLATFORM_LDFLAGS) $(UPDATED_PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

transaction_example: librocksdb transaction_example.cc
	$(CXX) $(CXX_FLAGS) $(LD_FLAGS) $@.cc -o$@ $(LIB_ROCKSDB) $(INC_PATH) $(PLATFORM_LDFLAGS) $(UPDATED_PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)   

options_file_example: librocksdb options_file_example.cc
	$(CXX) $(CXX_FLAGS) $(LD_FLAGS) $@.cc -o$@ $(LIB_ROCKSDB) $(INC_PATH) $(PLATFORM_LDFLAGS) $(UPDATED_PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)   

clean:
	rm -rf ./*.o ./simple_example ./column_families_example ./compact_files_example  ./compaction_filter_example ./c_simple_example c_simple_example.o ./optimistic_transaction_example ./transaction_example ./options_file_example ./dostoevsky ./dostoevsky_test

librocksdb:
	cd .. && $(MAKE) static_lib
