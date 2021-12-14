// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// An example code demonstrating how to use CompactFiles, EventListener,
// and GetColumnFamilyMetaData APIs to implement custom compaction algorithm.

#include <mutex>
#include <string>
#include <cmath>
#include <iostream>
#include <vector>

#include <unordered_set>
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/listener.h"

#include "dostoevsky.h"

#include <memory>
#include <unistd.h>


/*#include "../util/gethash.h"
#include "../table/filter_block.h"
#include "../table/block_based_filter_block.h"
#include "../util/bloom.cc"
 */
using namespace std;
using namespace rocksdb;
using namespace dost;



//TODO
// -- parallel merge operations
// -- when merging from L-1 to L, including L in the merge in advance

void insert_entries(DB* db, FluidLSMTree* tree) {
  WriteOptions w_options;
  time_t start_c,end_c;
  time (&start_c);
  int num_failed = 0;
  for (int i = 0; i < 100000; ++i) {
    int key = rand() % 1000000000;
    Status s = db->Put(w_options, std::to_string(key),
        std::string(500, 'a' + (i % 26)));
    num_failed += !s.ok();
    tree->finish_all_merge_operations(db);
  }

  FlushOptions ops;
  ops.wait = true;
  db->Flush(ops);
  tree->set_num_parallel_compactions_allowed(0);
  tree->finish_all_merge_operations(db);
  tree->buildStructure(db);
  tree->printFluidLSM(db);

  time (&end_c);
  double construction_time = difftime (end_c,start_c);
  printf("construction time:   %lf \n ", construction_time);
}

// void bulk_load(Options& options, string kDBPath, CrimsonDBFiltersPolicy* filters_policy, int T, int K, int Z, int L, long file_size ) {
//   /*Options options;
//   options.create_if_missing = true;
//   options.compaction_style = kCompactionStyleNone;
//   options.write_buffer_size = 1024 * 1024;
//   options.compression = kNoCompression; // eventually remove this
//   options.num_levels = 1000;
//   options.max_write_buffer_number = 2;
//   options.min_write_buffer_number_to_merge = 0;
//   options.max_write_buffer_number_to_maintain = 0;
//   options.IncreaseParallelism(5);*/

//   FluidLSMTreeBulkLoader* loader = new FluidLSMTreeBulkLoader(T, K, Z, file_size, options);
//   loader->set_batch_size(1000);
//   loader->set_num_parallel_compactions_allowed(1);
//   stats_collector* rsc = new stats_collector(loader);
//   options.listeners.emplace_back(loader);
//   options.listeners.emplace_back(rsc);

//   options.PrepareForBulkLoad();
//   options.num_levels = 1000;
//   DB* db = nullptr;
//   Status s = DB::Open(options, kDBPath, &db);

//   filters_policy->set_db(db, loader);
//   loader->setFiltersPolicy(filters_policy);

//   if (!s.ok()) {
//     printf("Problem opening DB. Closing.\n");
//     exit(0);
//   }



//   assert(s.ok());
//   assert(db);
//   long first_level_size = pow(2, 20);
//   loader->bulk_load_levels(db, "default", 128, first_level_size, L);

//   db->SyncWAL();
//   db->Close();
//   delete db;
//   db = nullptr;
// }


int main() {
  Options options;
  options.create_if_missing = true;
  // Disable RocksDB background compaction.
  // options.compaction_style = kCompactionStyleNone;
  options.level_compaction_dynamic_level_bytes = false;
  options.level0_file_num_compaction_trigger = 20;
  options.curr_max_level = 0;
  options.max_background_jobs = 5;
  // options.min_write_buffer_number_to_merge = 2;
  options.max_bytes_for_level_base = 64 << 12;
  options.write_buffer_size = 64 << 10;
  //options.compaction_style = kCompactionStyleLevel;
  //options.compaction_style = kCompactionStyleUniversal;

  //options.compaction_style = kCompactionStyleLevel;
  //options.level0_file_num_compaction_trigger = options.max_bytes_for_level_multiplier;
  //options.level0_slowdown_writes_trigger = options.max_bytes_for_level_multiplier;
  //options.level0_stop_writes_trigger = options.max_bytes_for_level_multiplier;
  //options.max_bytes_for_level_base = options.write_buffer_size * options.max_bytes_for_level_multiplier;

  // options.write_buffer_size = 1024 * 1024;
  // options.compression = kNoCompression; // eventually remove this
  options.num_levels = 1000;
  //options.min_write_buffer_number_to_merge = 1;
  // options.max_write_buffer_number = 2;
  // options.min_write_buffer_number_to_merge = 0;
  // options.max_write_buffer_number_to_maintain = 0;
  options.use_direct_reads = true;


  // Small slowdown and stop trigger for experimental purpose.

  //options.IncreaseParallelism(5);
  //FullCompactor* fc = new FullCompactor(options);
  long file_size = 1024 * 1024 * 2; //std::numeric_limits<uint64_t>::max(); // 1024 * 1024;
  //long file_size = std::numeric_limits<uint64_t>::max(); // 1024 * 1024;


  // MonkeyFilterPolicy monk = MonkeyFilterPolicy(10, false);
  //monk.show_hypothetical_FPRs(10, 10, 2, 1, 1);

  rocksdb::BlockBasedTableOptions table_options;
  //const FilterPolicy* fp = rocksdb::NewBloomFilterPolicy(10, true); // TODO: later on use full filter rather than block filter
  //printf("Filters policy name:  %s \n", filters_policy->Name());

  //CrimsonDBFiltersPolicy* filters_policy = new IterativelyOptimizingFilterPolicy(5, true);
  MonkeyFilterPolicy* filters_policy = new MonkeyFilterPolicy(5, true);
  filters_policy->show_hypothetical_FPRs(10, 5, 1);
  // filters_policy->set_update_FPRs_dynamically(false);
  filter_stats* fs = new filter_stats();
  filters_policy->set_stats_collection(fs);
  table_options.filter_policy.reset(filters_policy);

  options.table_factory.reset(NewBlockBasedTableFactory(table_options));


  string kDBPath = "/tmp/rocksdb_monkey_example";
  DestroyDB(kDBPath, options);




  // time_t start_b, end_b;
  // time (&start_b);
  // //bulk_load(options, kDBPath, filters_policy, 3, 2, 1, 3, file_size);
  // time (&end_b);
  // double load_time = difftime (end_b, start_b);
  // printf("load time:   %lf \n ", load_time);


  int size_ratio = 4;
  FluidLSMTree* fc = new FluidLSMTree(size_ratio, 1,  1, file_size, options);
  fc->set_debug_mode(true);
  fc->set_num_parallel_compactions_allowed(10);
  stats_collector* rsc = new stats_collector(fc);
  options.listeners.emplace_back(fc);
  options.listeners.emplace_back(rsc);
  DB* db = nullptr;
  DB::Open(options, kDBPath, &db);
  filters_policy->set_db(db, fc);
  fc->set_db(db);
  fc->setFiltersPolicy(filters_policy);
  //filters_policy->init_filters(10);


  insert_entries(db, fc);

  //w_options.no_slowdown = true;
  //w_options.sync = true;
  // if background compaction is not working, write will stall
  // because of options.level0_stop_writes_trigger

  time_t start_q, end_q;
  time (&start_q);
  // verify the values are still there
  std::string value;
  for (int i = 1000; i < 19999; ++i) {
    int key = rand() % 1000000000;
    Status s = db->Get(ReadOptions(), std::to_string(key), &value);
    if (fs) {
      fs->true_positives += s.ok();
    }
    //assert(value == std::string(500, 'a' + (i % 26)));
  }
  time (&end_q);
  double query_time = difftime (end_q, start_q);
  printf("query time:   %lf \n ", query_time);

  if (fs) {
    printf("\n");
    fs->print();
  }
  if (rsc) {
    printf("\n");
    rsc->print();
  }
  //db->CompactRange(rocksdb::CompactRangeOptions(), nullptr, nullptr);
  //fc->printFluidLSM(db);
//  /fc->buildStructure(db);
  //fc->printFluidLSM(db);
 /*
  fc->finish_all_merge_operations(db);
  fc->buildStructure(db);
  fc->printFluidLSM(db);*/
  //fc->finish_all_merge_operations(db);
  //fc->buildStructure(db);
  //fc->printFluidLSM(db);
  // close the db.
  delete fs;
  delete db;
  //delete fc;
  //delete rsc;

  return 0;
}
