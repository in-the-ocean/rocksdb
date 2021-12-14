/*
 * dostoevsky.h
 *
 *  Created on: Mar 24, 2018
 *      Author: niv
 */

#ifndef EXAMPLES_DOSTOEVSKY_H_
#define EXAMPLES_DOSTOEVSKY_H_

#include <set>
#include <mutex>
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/listener.h"
#include "bloom_filters.h"
// #include "../examples/monkey_experiments/environment.h"

namespace dost {


class FluidLSMInfrastructure;


// Example structure that describes a compaction task.
struct CompactionTask {
  CompactionTask(
      DB* _db, FluidLSMInfrastructure* _compactor,
      const std::string& _column_family_name,
      const std::vector<std::string>& _input_file_names,
      const int _output_level,
      const CompactionOptions& _compact_options,
      bool _retry_on_fail,
      bool debug_mode)
  : db(_db),
    compactor(_compactor),
    column_family_name(_column_family_name),
    input_file_names(_input_file_names),
    output_level(_output_level),
    compact_options(_compact_options),
    retry_on_fail(_retry_on_fail),
    debug_mode(debug_mode) {}
  DB* db;
  FluidLSMInfrastructure* compactor;
  const std::string& column_family_name;
  std::vector<std::string> input_file_names;
  int output_level;
  CompactionOptions compact_options;
  bool retry_on_fail;
  bool debug_mode;
};

struct run {
  void addFile(rocksdb::SstFileMetaData file);
  bool contains(std::string file_name);
  std::vector<rocksdb::SstFileMetaData> files;
  std::set<std::string> file_names;
  int RocksDBLevel;
  // WQ-Note: adding to make Compaction Thread safe.
  // rationale is to mirror core-rocksdb SortedRun (in compaction_picker_universal.h)
  bool being_compacted;
  // std::mutex *compactor_mutex;
  run(int rocksDBLevel){
    RocksDBLevel = rocksDBLevel;
    // compactor_mutex = new std::mutex();
  }
};

struct lazyLevel {
  lazyLevel() {
    // g_compaction_mutex = new std::mutex();
  }
  void addRun(run new_run) { runs.push_back(new_run); }
  int size() const;
  int num_live_runs();
  int size_in_bytes() const;
  bool contains(std::string file_name);
  std::vector<run> runs;
  // std::mutex *g_compaction_mutex;
};

struct write_stats {
  write_stats();
  void record_write_stats(int bytes_written_now, int keys_inserted_now);
  void print();
  long long bytes_written;
  long long keys_inserted;
};

struct read_stats {
  read_stats();
};

class stats_collector: public EventListener {
public:
  stats_collector(FluidLSMInfrastructure const*const);
  void OnFlushCompleted(DB* db, const FlushJobInfo& info) override;
  void OnTableFileCreated(const TableFileCreationInfo& /*info*/) override;
  long long getBytesFlushed();
  long long getBytesMerged();
  void print();
private:
  double num_runs;
  double FPR_sum;
  long buffer_flushes;
  long long bytes_merged;
  long long bytes_flushed;
  FluidLSMInfrastructure const*const fluidLSM;
};

class FluidLSMInfrastructure: public EventListener {
public:
  FluidLSMInfrastructure(int T, int K, int Z, long file_size, const Options options);
  void buildStructure(DB* db);
  int get_size_ratio() const;
  int get_max_runs_at_largest_level() const;
  int get_max_runs_at_smaller_levels() const;
  long long get_buffer_size() const;
  void printLSM(DB* db) const;
  void printFluidLSM(DB* db) const;
  virtual void OnFlushCompleted(DB* db, const FlushJobInfo& info) override;
  virtual int largestOccupiedLevel() const;
  virtual void PickCompaction(DB* db, const std::string& cf_name) = 0;
  void setFiltersPolicy(CrimsonDBFiltersPolicy* fp);
  int get_num_runs() const;
  double get_FPR_sum() const;
  //bool is_compacting(DB* db) const;
  void finish_all_merge_operations(DB* db);
  static void CompactFiles(void* arg);
  void set_num_parallel_compactions_allowed(int);
  unsigned long get_fluidLSM_level(int rocksdb_level) const;
  void set_debug_mode(bool debugging);
  void set_db(DB*);
  Options const& getOptions();
  // std::vector<CompactionTask*> compactionTasks;
protected:
  void addFilesToCompaction(DB* db, int level, std::vector<rocksdb::SstFileMetaData*>& input_file_names);
  long getCompactionSize( std::vector<rocksdb::SstFileMetaData*> const& input_file_names) const;
  void addRun(DB* db, std::vector<rocksdb::SstFileMetaData> const& file_names, int level, int RocksDB_level);
  void ScheduleCompaction(DB* db, const std::string& cf_name, int origin_level, int target_level, std::vector<rocksdb::SstFileMetaData*> input_files);
  std::mutex g_lazyLevels_mutex;
  std::mutex g_compaction_pick_mutex;
  std::vector<lazyLevel> lazyLevels;
  int T; // size ratio
  int K; // bound on the number of runs at lower levels
  int Z; // bound on the number of runs at last level
  long file_size;
  Options options_;
  CrimsonDBFiltersPolicy* filters_policy;
  CompactionOptions compact_options_;
  int num_parallel_compactions_allowed;
  int num_parallel_compactions;
  std::mutex num_parallel_compactions_mutex;
  bool debug_mode;
  DB* db;

};

// class FluidLSMTreeBulkLoader: public FluidLSMInfrastructure {
// public:
//   FluidLSMTreeBulkLoader(int T, int K, int Z, long file_size, const Options options);
//   void PickCompaction(DB* db, const std::string& cf_name) override;
//   void OnFlushCompleted(DB* db, const FlushJobInfo& info) override;

//   //main bulk load function
//   void _bulk_load(DB* db, const std::string& cf_name, int entry_size, long buffer_size, std::vector <long>& entries_per_level, int num_levels, int verbosity);
//   //wrapper bulk load functions
//   long bulk_load_levels(DB* db, const std::string& cf_name, int entry_size, int buffer_size, int num_levels, bool show_progress=false);
//   long bulk_load_levels(DB* db, const std::string& cf_name, ExpEnv* _env);
//   long bulk_load_entries(DB* db, const std::string& cf_name, ExpEnv* _env);
  
//   void set_batch_size(int batch_size);
//   void set_key_prefix_for_X_smallest_levels(int num_levels, string prefix); // used so that we can issue non-zero-result point lookups later to only the X smallest levels

//   //virtual int largestOccupiedLevel() const;
// private:
//   void bulk_load_level(DB* db, const std::string& cf_name, int entry_size, long num_entries, int level, int num_runs);
//   void bulk_load_run(DB* db, const std::string& cf_name, int entry_size, long num_entries, int level);
//   void load_batch(rocksdb::WriteBatch& batch, int batch_size, int entry_size, string key_prefix, string val_prefix) const;
//   int batch_size;
//   string key_prefix; // used so that we can issue non-zero-result point lookups later to only the X smallest levels
//   int num_levels_to_set_prefix_for;
//   //int num_levels;
// };

class FluidLSMTree: public FluidLSMInfrastructure {
public:
  FluidLSMTree(int T, int K, int Z, long file_size, const Options options);
  void PickCompaction(DB* db, const std::string& cf_name);
  int getCompactionTarget(int origin_level, std::vector<rocksdb::SstFileMetaData*> const& input_files) const;
};

class DataGenerator {
public:
  static string generate_key(string const key_prefix = "");
  static string generate_val(long size, const string val_prefix = "");
  static pair<string, string> generate_key_val_pair(unsigned long size, const string key_prefix = "", const string val_prefix = "");
  static unsigned long long KEY_DOMAIN_SIZE;
};

}

#endif /* EXAMPLES_DOSTOEVSKY_H_ */
