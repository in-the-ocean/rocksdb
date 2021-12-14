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

struct CompactionTask;
class FullCompactor;


/*struct filters_statistics {
  filters_statistics() : num_negatives(0), num_positives(0) {}
  int num_negatives;
  int num_positives;
};*/


void run::addFile(rocksdb::SstFileMetaData file) {
  files.push_back(file);
  file_names.insert(file.name);
}

bool run::contains(std::string file_name) {
  return file_names.count(file_name) == 1;
}




bool lazyLevel::contains(std::string file_name) {
  for (auto& run : runs) {
    if (run.contains(file_name)) {
      return true;
    }
  }
  return false;
}

int lazyLevel::size() const {
  int num_runs = 0;
  for (auto& run : runs) {
    num_runs += run.files.size() > 0;
  }
  return num_runs;
}

int lazyLevel::num_live_runs() {
  int num_live_runs = 0;
  for (auto& run : runs) {
    bool being_compacted = false;
    for (auto& file : run.files) {
      // Niv's previous statement not thread safe
      // being_compacted |= file.being_compacted;

      // use run level being_compacted measure
      being_compacted |= run.being_compacted;
    }
    num_live_runs += !being_compacted && !run.files.empty();
  }
  return num_live_runs;
}

int lazyLevel::size_in_bytes() const {
  int bytes = 0;
  for (auto& run : runs) {
    for (auto& file : run.files) {
      bytes += file.size;
    }
  }
  return bytes;
}



write_stats::write_stats() : bytes_written(0), keys_inserted(0) {}

void write_stats::record_write_stats(int bytes_written_now, int keys_inserted_now) {
  bytes_written += bytes_written_now;
  keys_inserted += keys_inserted;
}

void write_stats::print() {
  std::cerr << "bytes written: " << bytes_written << std::endl;
  std::cerr << "average FPR: " << keys_inserted << std::endl;
  std::cerr << "bytes per key: " << ((double)bytes_written / (double)keys_inserted) << std::endl;
}


stats_collector::stats_collector(FluidLSMInfrastructure const*const tree) :
    num_runs(0), FPR_sum(0), buffer_flushes(0), fluidLSM(tree),
    bytes_merged(0), bytes_flushed(0) {}

void stats_collector::OnFlushCompleted(DB* db, const FlushJobInfo& info) {
  int largest_lvl = fluidLSM->largestOccupiedLevel();
  int current_num_runs = fluidLSM->get_num_runs();
  double current_FPR_sum = fluidLSM->get_FPR_sum();
  num_runs = (num_runs * buffer_flushes + current_num_runs) / (1 + buffer_flushes);
  FPR_sum = (FPR_sum * buffer_flushes + current_FPR_sum) / (1 + buffer_flushes);
  buffer_flushes++;
  //print();
}

long long stats_collector::getBytesFlushed()
{
  return this->bytes_flushed;
}


long long stats_collector::getBytesMerged()
{
  return this->bytes_merged;
}


void stats_collector::OnTableFileCreated(const TableFileCreationInfo& info) {
  if (info.reason == TableFileCreationReason::kFlush) {
    bytes_flushed += info.file_size;
  }
  else if (info.reason == TableFileCreationReason::kCompaction) {
    bytes_merged += info.file_size;
  }
}

void stats_collector::print() {
  std::cerr << "average number of runs: " << num_runs << std::endl;
  std::cerr << "average FPR: " << FPR_sum << std::endl;
  std::cerr << "num flushes: " << buffer_flushes << std::endl;
  std::cerr << std::endl;
  std::cerr << "bytes flushed: " << bytes_flushed << std::endl;
  std::cerr << "bytes merged: " << bytes_merged << std::endl;
  std::cerr << "bytes written: " << (bytes_merged+bytes_flushed) << std::endl;
}

FluidLSMTree::FluidLSMTree(int T, int K, int Z, long file_size, const Options options) :
    FluidLSMInfrastructure(T, K, Z, file_size, options) {
      std::mutex(g_compaction_pick_mutex);
    }

FluidLSMInfrastructure::FluidLSMInfrastructure(int T, int K, int Z, long file_size, const Options options) : filters_policy(), options_(options), compact_options_(),
    T(T), K(K), Z(Z), file_size(file_size),
    num_parallel_compactions_allowed(1), num_parallel_compactions(0), debug_mode(false), db(nullptr) {
  compact_options_.compression = options_.compression;
  compact_options_.output_file_size_limit = options_.target_file_size_base;
  // lazyLevels = std::vector<lazyLevel>(options.num_levels);
  //the object is already defined so we simply resize
  g_lazyLevels_mutex.lock();
  lazyLevels.resize(options.num_levels);
  g_lazyLevels_mutex.unlock();
}

void FluidLSMInfrastructure::set_num_parallel_compactions_allowed(int num_allowed) {
  num_parallel_compactions_allowed = num_allowed;
}

void FluidLSMInfrastructure::OnFlushCompleted(DB* db, const FlushJobInfo& info) {
  //buildStructure(db);
  //printFluidLSM(db);
  //std::string file_name = info.file_path.substr( info.file_path.find_last_of("/") );
  PickCompaction(db, info.cf_name);

}

void FluidLSMInfrastructure::addRun(DB* db, std::vector<rocksdb::SstFileMetaData> const& file_names, int fluid_level, int RocksDB_level) {
  ColumnFamilyMetaData cf_meta;
  db->GetColumnFamilyMetaData(&cf_meta);
  run new_run(RocksDB_level);
  for (rocksdb::SstFileMetaData file : file_names) {
    new_run.addFile(file);
  }
  // protect this modification of the global structure of fluidLSM
  lazyLevels[fluid_level].addRun(new_run);
}

int FluidLSMInfrastructure::largestOccupiedLevel() const {
  int largest = 0;
  for (int i = 0; i < lazyLevels.size(); i++) 
  {
    for (auto& run : lazyLevels[i].runs) {
      if (run.files.size() > 0) {
        largest = i;
      }
    }
  }
  return largest;
}

int FluidLSMInfrastructure::get_num_runs() const {
  int runs = 0;
  int largest = largestOccupiedLevel();
  for (int i = 0; i <= largest; i++) {
    for (auto& run : lazyLevels[i].runs) {
      runs += !run.files.empty();
    }
  }
  return runs;
}

double FluidLSMInfrastructure::get_FPR_sum() const {
  double current_FPR_sum = 0;
  int largest = largestOccupiedLevel();
  for (int i = 0; i <= largest; i++) {
    current_FPR_sum += filters_policy == nullptr ? 1 : filters_policy->getFalsePosRateForLevel(i) * lazyLevels[i].size();
  }
  return current_FPR_sum;
}

void FluidLSMInfrastructure::setFiltersPolicy(CrimsonDBFiltersPolicy* fp) {
  filters_policy = fp;
}

void FluidLSMInfrastructure::CompactFiles(void* task_arg) {
  std::unique_ptr<CompactionTask> task(
      reinterpret_cast<CompactionTask*>(task_arg));
  assert(task);
  assert(task->db);
  std::vector<std::string>* output_file_names = new std::vector<std::string>();
  Status s = task->db->CompactFiles(
      task->compact_options,
      task->input_file_names,
      task->output_level,
      -1,
      output_file_names);

  if (task->debug_mode) {
    for (auto f : *output_file_names) {
      std::cerr << " ---- new file created:    " << f << endl;
    }

    std::cerr << "CompactFiles() finished with status " << s.ToString() << endl;
    for (auto i : task->input_file_names) 
    {
      std::cerr << "    " << i << endl;
    }
    std::cerr << "   -> level " << task->output_level << endl;
  }
  FluidLSMTree* tree = reinterpret_cast<FluidLSMTree*>(task->compactor);
  // TODO use mutex
  //return;

  //tree->printLSM(task->db);

  if (!s.IsIOError()) {
    tree->PickCompaction(task->db, task->column_family_name);
  }

  // pop task from compactionTasks

  tree->num_parallel_compactions_mutex.lock();
  tree->num_parallel_compactions--;
  tree->num_parallel_compactions_mutex.unlock();

}


/*bool FluidLSMTree::isNewFile(int level, SstFileMetaData const& file) {
  for (auto& run : lazyLevels[level].runs) {
    for (auto& r_file : run.files) {
      if (r_file.name.compare(file.name) == 0) {
        return false;
      }
    }
  }
  return true;
}*/

void FluidLSMInfrastructure::printLSM(DB* db) const {
  ColumnFamilyMetaData cf_meta;
  db->GetColumnFamilyMetaData(&cf_meta);
  int largest_used_level = 0;
  for (auto level : cf_meta.levels) {
    if (level.files.size() > 0) {
      largest_used_level = level.level;
    }
  }

  for (auto level : cf_meta.levels) {

    long level_size = 0;
    for (auto file : level.files) {
      level_size += file.size;
    }

    std::cerr << "level " << level.level << ".  Size " << level_size << endl;
    // for (auto file : level.files) {
      //printf("%s   ", file.name.c_str());
    // }
    std::cerr << endl;
    for (auto file : level.files) 
    {
      std::cerr << " \t " << file.size << " \t " << file.smallestkey << "-" << file.largestkey;
      std::cerr << "    " << filters_policy->getFalsePosRateForLevel(level.level) << " \t " << file.name << endl;
    }
    if (level.level == largest_used_level) {
      break;
    }
  }
  std::cerr << endl;
}

void FluidLSMInfrastructure::printFluidLSM(DB* db) const {
  ColumnFamilyMetaData cf_meta;
  db->GetColumnFamilyMetaData(&cf_meta);
  int num_levels = largestOccupiedLevel();
  for (int i = 0; i < num_levels + 1; i++) {
    std::cerr << "lvl: " << i << endl;
    for (int j = 0; j < lazyLevels[i].runs.size(); j++) {
      if (lazyLevels[i].runs[j].files.empty()){
        continue;
      }
      std::cerr << "\t run: " << j << "   (RocksDBLvl: " << lazyLevels[i].runs[j].RocksDBLevel << ")" << endl; 
      for (auto file : lazyLevels[i].runs[j].files) {
        if(&file && !file.being_compacted){
          std::cerr << "\t\t " << file.size;
          std::cerr << "\t " << file.smallestkey << "-" << file.largestkey;
          std::cerr << " " << filters_policy->getFalsePosRateForLevel(i) << "  " << filters_policy->getBitsPerEntryForLevel(i);
          std::cerr << " \t " << file.name << " \t " << (file.being_compacted ? "being compacted" : "") << endl;
        }
      }
    }
  }
  std::cerr << endl;
  
  // cerr << "ongoing compactions total: " << compactionTasks.size() << std::endl;

  // for(int i = 0; i < compactionTasks.size(); i++){
  //   std::cerr << "Compactions added to queue : " << std::endl;
  //   std::cerr << "\t Files: ";
  //   for(int j = 0; j < compactionTasks[i]->input_file_names.size(); j++){
  //     std::cerr << compactionTasks[i]->input_file_names[j] << ",";
  //   }
  //   std::cerr << std::endl;
  // }
}

int FluidLSMInfrastructure::get_size_ratio() const { return T; }
int FluidLSMInfrastructure::get_max_runs_at_largest_level() const { return Z; }
int FluidLSMInfrastructure::get_max_runs_at_smaller_levels() const { return K; }
long long FluidLSMInfrastructure::get_buffer_size() const { return options_.write_buffer_size; }

/*bool FluidLSMInfrastructure::is_compacting(DB* db) const {
  ColumnFamilyMetaData cf_meta;
  db->GetColumnFamilyMetaData(&cf_meta);
  for (auto level : cf_meta.levels) {
    for (auto file : level.files) {
      if (file.being_compacted) {
        return true;
      }
    }
  }
  return false;
}*/

void FluidLSMInfrastructure::finish_all_merge_operations(DB* db) {
  /*ColumnFamilyMetaData cf_meta;
  db->GetColumnFamilyMetaData(&cf_meta);
  do {
    PickCompaction(db, cf_meta.name);
    usleep(100);
  } while (is_compacting(db));
  usleep(100);*/

  
  //if (this->debug_mode)
  //  std::cerr << "#parallel compactions : " << num_parallel_compactions << std::endl;
  int count=0;
  while (num_parallel_compactions > 0) 
  {
    count++;
    // used to be 1000 micro sec
    usleep(100);
  }
  //if (this->debug_mode)
  //  std::cerr << "Waiting for parallel compactions to finish " << count << " times." << std::endl;

}

/*buildStructure(db) converts RocksDB to FluidLSM internal structure 
  */
void FluidLSMInfrastructure::buildStructure(DB* db) {
  g_lazyLevels_mutex.lock();
  ColumnFamilyMetaData cf_meta;
  db->GetColumnFamilyMetaData(&cf_meta);
  //int largest = ceil((double)largestOccupiedLevel() / (double)K);

  for (int i = 0; i < lazyLevels.size(); i++) {
    lazyLevels[i].runs.clear();
  }

// should we do this casting? are we going to ever need more than 32 bits?
  unsigned long num_runs_in_fluid_level_1 = cf_meta.levels[0].files.size() + !cf_meta.levels[1].files.empty();

  std::vector<rocksdb::SstFileMetaData> file_names;
  for (unsigned long i = num_runs_in_fluid_level_1; i < K; i++) {
    addRun(db, file_names, 0, 0);
  }

  for (auto file : cf_meta.levels[0].files) {
    file_names.push_back(file);
    addRun(db, file_names, 0, 0);
    file_names.clear();
  }

  for (auto file : cf_meta.levels[1].files) {
    file_names.push_back(file);
  }
  addRun(db, file_names, 0, 1);
  file_names.clear();

  for (int i = 2; i < cf_meta.levels.size(); i++) {
    LevelMetaData level = cf_meta.levels[i];
    int fluid_lvl = ceil(((double)level.level - 1) / ((double)K + 1.0));

    file_names.clear();
    for (auto file : level.files) {
      file_names.push_back(file);
    }
    addRun(db, file_names, fluid_lvl, level.level);

    /*for (auto file : level.files) {
			printf(" \t %lu \t %s-%s    %f \t %s \n", file.size, file.smallestkey.c_str(), file.largestkey.c_str(), filters_policy->getFalsePosRateForLevel(level.level), file.name.c_str());
		}*/
  }
  g_lazyLevels_mutex.unlock();
  //std::cerr << endl;
}

long FluidLSMInfrastructure::getCompactionSize( std::vector<rocksdb::SstFileMetaData*> const& input_file_names) const {
  long bytes = 0;
  for (auto& file : input_file_names) {
    // not thread safe access of the file's size
    if (file && !file->being_compacted){
      bytes += file->size;
    }
  }
  return bytes;
}

void FluidLSMInfrastructure::addFilesToCompaction(DB* db, int level, std::vector<rocksdb::SstFileMetaData*>& input_file_names) {
  ColumnFamilyMetaData cf_meta;
  db->GetColumnFamilyMetaData(&cf_meta);
  for (auto& run : lazyLevels[level].runs) {
    for (auto& file : run.files) {
      //input_file_hash.insert(file.name);
      if (&file && !file.being_compacted) {
        input_file_names.push_back(&file);
      }
    }
  }
}

void FluidLSMTree::PickCompaction(DB* db, const std::string& cf_name) 
{
  // convert rocksdb to Fluid representation
  buildStructure(db);
  if (debug_mode)
    printFluidLSM(db);

  std::vector<rocksdb::SstFileMetaData*> input_files;
  int largest_level = largestOccupiedLevel();
  int origin_level = 0;
  for (int lvl = largest_level; lvl >= 0; lvl--) {
    int runs = lazyLevels[lvl].num_live_runs();
    if ((lvl < largest_level && runs > K) || (lvl == largest_level && runs > Z)) {
      addFilesToCompaction(db, lvl, input_files);

      if(input_files.size() == 0){
        // no compaction to do
        std::cout << "skipping compaction (PickCompaction() inside FluidLSM)" << std::endl;
        return;
      }
      
      origin_level = lvl;
      int target_level = getCompactionTarget(origin_level, input_files);
      
      if(input_files.size() == 0){
        // no compaction to do
        std::cout << "skipping compaction (PickCompaction() inside FluidLSM)" << std::endl;
        return;
      }
      
      ScheduleCompaction(db, cf_name, origin_level, target_level, input_files);
      input_files.clear();
    }
  }
}

int FluidLSMTree::getCompactionTarget(int origin_level, std::vector<rocksdb::SstFileMetaData*> const& input_files) const {
  long projected_output_size = getCompactionSize(input_files);
  int target_level = origin_level;
  long origin_lvl_capacity = options_.write_buffer_size * pow(T, origin_level + 1) * (T-1) / T;
  if (projected_output_size > origin_lvl_capacity) {
    target_level = origin_level + 1;
  }
  return target_level;
}

unsigned long FluidLSMInfrastructure::get_fluidLSM_level(int rocksdb_level) const {
  if (rocksdb_level == 0) return 0;
  else return floor((rocksdb_level + (K-1) - 1.0) / (K + 1.0));
}

void FluidLSMInfrastructure::ScheduleCompaction(DB* db, const std::string& cf_name, int origin_level, int target_level, std::vector<rocksdb::SstFileMetaData*> input_files) {

  if (/*input_files.size() >= 2 &&*/ num_parallel_compactions_allowed > num_parallel_compactions) {
    // pick which run slot
    int RocksDBLevel = 1 + target_level * (K + 1);
    int slot = K;
    for (int i = 0; i <= K && target_level > origin_level; i++) {
      if (lazyLevels[target_level].runs[K - i].files.empty()) {
        RocksDBLevel = 1 + target_level * (K + 1) - i;
        slot = K - i;
        break;
      }
    }

    target_level = std::min(options_.num_levels - 1, target_level);
    int largest_level = largestOccupiedLevel();
    if (target_level > largest_level) {
      filters_policy->init_filters(target_level);
    }

    /*printFluidLSM(db);
    printf("\n merging to level %d slot %d, which is level %d in RocksDB\n ", target_level, slot, RocksDBLevel);
    for (auto f : input_files) {
      printf("\t %s \n ", f->name.c_str());
    }
    std::cerr << endl;*/

    // this is where the input file names are being built
    std::vector<std::string> input_file_names;
    for (auto f : input_files) {
      // only try to prepare a compaction task for files that are not being compacted
      if(f && !f->being_compacted){
        input_file_names.push_back(f->name);
      } else {
        std::cerr << "bad file compaction in FluidLSMInfrastructure::ScheduleCompaction";
      }
    }

    if(input_file_names.size() == 0){
      std:cout << "skipping compaction (ScheduleCompaction() inside FluidLSMInfra)" << std::endl;
      return;
    }

    num_parallel_compactions_mutex.lock();
    num_parallel_compactions++;
    num_parallel_compactions_mutex.unlock();

    CompactionTask* task = new CompactionTask(db, this, cf_name, input_file_names,  RocksDBLevel, compact_options_, false, debug_mode);
    task->compact_options.output_file_size_limit = file_size;
    // compactionTasks.push_back(task);
    options_.env->Schedule(&FluidLSMInfrastructure::CompactFiles, task);
  }
  
  // else if (debug_mode) {
  //   std::cerr << "skip compaction.  origin lvl: " << origin_level << "   num files: " << input_files.size();
  //   std::cerr << "   ongoing compactions: " << num_parallel_compactions << endl; 
  //   int runs = lazyLevels[origin_level].size();
  //   int live_runs = lazyLevels[origin_level].num_live_runs();
  //   std::cerr << "runs " << runs << "    live runs:  " << lazyLevels[origin_level].num_live_runs() << endl; 
  // }

}

// FluidLSMTreeBulkLoader::FluidLSMTreeBulkLoader(int T, int K, int Z, long file_size, const Options options) :
//     FluidLSMInfrastructure(T, K, Z, file_size, options), batch_size(100), num_levels_to_set_prefix_for(0), key_prefix("") {}

// void FluidLSMTreeBulkLoader::PickCompaction(DB* db, const std::string& cf_name) {

// }

// void FluidLSMTreeBulkLoader::OnFlushCompleted(DB* db, const FlushJobInfo& info) {
//   FluidLSMInfrastructure::OnFlushCompleted(db, info);
//   if (debug_mode) {
//     static int i = 0;
//     std::cerr << "flush " << i++ << endl; 
//   }
// }


// /*Function that implments bulk loading with fixed number of ENTRIES (N)*/
// long FluidLSMTreeBulkLoader::bulk_load_entries(DB* db, const std::string& cf_name, ExpEnv* _env) 
// {
//   //make sure we have already done this calculation
//   assert(_env->derived_num_levels!=-1);
//   long total_entries_bulk_loaded=0;
//   long entries_buffer = (_env->buffer_size/_env->entry_size);

//   std::vector <long> entries_per_level(_env->derived_num_levels+1);

//   //new bulk loading technique for number of entries (proportional per level)
//   long current_level_size=_env->N * (_env->T-1)/_env->T;
//   long current_level=_env->derived_num_levels;
//   entries_per_level[current_level]=current_level_size;
//   if (_env->show_progress)
//     std::cerr << " Level: " << current_level << " -> " << entries_per_level[current_level] << " entries" << endl;
//   total_entries_bulk_loaded+=current_level_size;
//   for (int L = _env->derived_num_levels - 1; L >= 0; L--) {
//     current_level_size /= T;
//     entries_per_level[L]=current_level_size;
    
//     if (L==0 && entries_per_level[L]<entries_buffer)
//     {
//       entries_per_level[L] = entries_buffer;
//       std::cerr << " Correction to fill the buffer: ";
//     }    
//     if (_env->show_progress)
//       std::cerr << " Level: " << L << " -> " << entries_per_level[L] << " entries" << endl;
//     total_entries_bulk_loaded+=current_level_size;
//   }

//   // long total_entries_to_bulk_loaded=_env->N;
//   // int current_level=0;
//   // long current_level_size=(_env->buffer_size/_env->entry_size)*(_env->T-1);

//   // //the idea would be to fill all levels from smaller (0) to larger and stop as we go
//   // while (total_entries_to_bulk_loaded>0)
//   // {
//   //   if (current_level>_env->derived_num_levels)
//   //   {
//   //     cerr << "Resizing the vector but this should not happen ideally" << endl;
//   //     entries_per_level.resize(current_level+1);
//   //   }

//   //   long entries_to_insert = (current_level_size<total_entries_to_bulk_loaded)?current_level_size:total_entries_to_bulk_loaded;
//   //   entries_per_level[current_level]=entries_to_insert;
//   //   if (_env->show_progress)
//   //     std::cerr << " Level: " << current_level << " -> " << entries_to_insert << " entries" << endl;
//   //   total_entries_to_bulk_loaded-=entries_to_insert;
//   //   current_level++;
//   //   current_level_size*=T;
//   // }
 
//   //TODO(manos) make the loading proportional to all levels when the last one is not full

//   _bulk_load(db, cf_name, _env->entry_size, _env->buffer_size, entries_per_level, _env->derived_num_levels, _env->verbosity);
  
//   return total_entries_bulk_loaded;
// }

// void FluidLSMTreeBulkLoader::_bulk_load(DB* db, const std::string& cf_name, int entry_size, long buffer_size, std::vector <long>& entries_per_level, int num_levels, int verbosity)
// {
//   // this is to account for the fact that SSTables have some metadata so the ideal number of entries
//   // it turned out that it is not needed (only increaing buffer size when bulk loading was enough)
//   // but I leave it here as a parameter
//   double overhead_factor=1.0; 
//   if (verbosity >= 1)
//     std::cerr << "Inserting " << entries_per_level[num_levels] << " --> " << overhead_factor*entries_per_level[num_levels] << " in level " << num_levels << endl;
//   std::cerr << "." << std::flush;
//   if (entries_per_level[num_levels]>0)
//     bulk_load_level(db, cf_name, entry_size, overhead_factor*entries_per_level[num_levels] , num_levels, Z);  

//   for (int L = num_levels - 1 ; L >= 0; L--) 
//   {
//     if (verbosity >= 1)
//       std::cerr << "Inserting " << entries_per_level[L] << " --> " << overhead_factor*entries_per_level[L] << " in level " << L << endl;
//     std::cerr << "." << std::flush;  
//     if (entries_per_level[L]>0)
//       bulk_load_level(db, cf_name, entry_size, overhead_factor*entries_per_level[L], L, K);  
//   }
//   std::cerr << endl << std::flush;
// }


// long FluidLSMTreeBulkLoader::bulk_load_levels(DB* db, const std::string& cf_name, int entry_size, int buffer_size, int num_levels, bool show_progress) 
// {
//   long total_entries_bulk_loaded=0;
//   double largest_level_size = buffer_size * pow(T, num_levels) * (T-1);
//   double num_entries_to_insert = largest_level_size / entry_size;
  
//   total_entries_bulk_loaded=num_entries_to_insert;

//   assert(num_levels!=-1);

//   std::vector <long> entries_per_level(num_levels+1);
//   entries_per_level[num_levels]=num_entries_to_insert;
//   if (show_progress)
//     std::cerr << " Level: " << num_levels << " -> " << entries_per_level[num_levels] << " entries" << endl;

//   for (int L = num_levels - 1; L >= 0; L--) {
//     num_entries_to_insert /= T;
//     entries_per_level[L]=num_entries_to_insert;
//     if (show_progress)
//       std::cerr << " Level: " << L << " -> " << entries_per_level[L] << " entries" << endl;
//     total_entries_bulk_loaded+=num_entries_to_insert;
//   }

//   _bulk_load(db, cf_name, entry_size, buffer_size, entries_per_level, num_levels, 0);

//   return total_entries_bulk_loaded;
// }

// long FluidLSMTreeBulkLoader::bulk_load_levels(DB* db, const std::string& cf_name, ExpEnv* _env) 
// {
//   //wrapper
//   return bulk_load_levels(db,cf_name,_env->entry_size,_env->buffer_size,_env->num_levels,_env->show_progress);
// }

// void FluidLSMTreeBulkLoader::load_batch(rocksdb::WriteBatch& batch, int batch_size, int entry_size, string key_prefix, string val_prefix) const {
//   for (int i = 0; i < batch_size; i++) {
//     pair<string, string> kv_pair = DataGenerator::generate_key_val_pair(entry_size, key_prefix, val_prefix);
//     batch.Put(kv_pair.first, kv_pair.second);
//   }
// }

// void FluidLSMTreeBulkLoader::set_batch_size(int size) {
//   batch_size = size;
// }

// void FluidLSMTreeBulkLoader::set_key_prefix_for_X_smallest_levels(int num_levels, string prefix) {
//   key_prefix = prefix;
//   num_levels_to_set_prefix_for = num_levels;
// }

void FluidLSMInfrastructure::set_debug_mode(bool debugging) {
  debug_mode = debugging;
}

void FluidLSMInfrastructure::set_db(DB* the_db) {
  db = the_db;
}

Options const& FluidLSMInfrastructure::getOptions() {
  return options_;
}

// /*int FluidLSMTreeBulkLoader::largestOccupiedLevel() const {
//   return num_levels;
// }*/

// void FluidLSMTreeBulkLoader::bulk_load_run(DB* db, const std::string& cf_name, int entry_size, long num_entries, int level) {
//   int num_failed = 0;
//   WriteOptions w_options;
//   w_options.sync = false;
//   w_options.disableWAL = true;
//   w_options.no_slowdown = false; // enabling this will make some insertions fail
//   //int value_size = entry_size - 4;
//   long buffer_size = entry_size * num_entries * 2; // inflate by 2x as there is some other overheads (which vary with #level)
//   Status s = db->SetOptions({{"write_buffer_size", to_string(buffer_size)}});
//   string val_prefix = std::to_string(level) + "-";
//   string key_prefix_to_use = num_levels_to_set_prefix_for >= level ? key_prefix : "";
//   for (long i = 0; i < num_entries; i += batch_size) {
//     if (batch_size == 1) {
//       pair<string, string> entry = DataGenerator::generate_key_val_pair(entry_size, key_prefix_to_use, val_prefix);
//       s = db->Put(w_options, entry.first, entry.second);
//     }
//     else {
//       rocksdb::WriteBatch batch;
//       load_batch(batch, batch_size, entry_size, key_prefix_to_use, val_prefix);
//       s = db->Write(w_options, &batch);
//     }
//     num_failed += !s.ok();
//     if (!s.ok())
//       std::cerr << s.ToString() << std::endl;
//   }
//   if (num_failed!=0)
//     std::cerr << "num_failed: " << num_failed << ", num_entries: " << num_entries << ", level: " << level << std::endl;
//   assert(num_failed < num_entries * 0.01);
//   FlushOptions fo;
//   fo.wait = true;
//   db->Flush(fo);

//   std::vector<rocksdb::SstFileMetaData*> input_files;
//   buildStructure(db);
//   if (debug_mode) {
//     printFluidLSM(db);
//   }
//   addFilesToCompaction(db, 0, input_files);
//   if (level!=0)
//     ScheduleCompaction(db, cf_name, 0, level, input_files);

//   finish_all_merge_operations(db);
//   buildStructure(db);

//   if (debug_mode) {
//     printFluidLSM(db);
//   }
// }

// void FluidLSMTreeBulkLoader::bulk_load_level(DB* db, const std::string& cf_name, int entry_size, long level_num_entries, int level, int num_runs) {
//   long num_entries_per_run = level_num_entries / num_runs;
//   for (long i = 0; i < num_runs; i++) {
//     if (this->debug_mode)
//       std::cerr << "************\nbulk loading run: " << i << " at level " << level << " with " << level_num_entries << " elements\n************\n" << std::endl;
//     bulk_load_run(db, cf_name, entry_size, num_entries_per_run, level);
//   }
// }

unsigned long long DataGenerator::KEY_DOMAIN_SIZE = 1000000000;

string DataGenerator::generate_key(const string key_prefix) {
  unsigned long long randomness = rand() % KEY_DOMAIN_SIZE;
  string key = key_prefix + std::to_string(randomness);
  return key;
}

string DataGenerator::generate_val(long val_size, const string val_prefix) {
  long randomness_size = val_size - val_prefix.size();
  string val = val_prefix + std::string(randomness_size, 'a' + (1 % 26));
  return val;
}

pair<string, string> DataGenerator::generate_key_val_pair(unsigned long kv_size, const string key_prefix, const string val_prefix) {
  string key = generate_key(key_prefix);
  long val_size = kv_size - key.size();
  string val = generate_val(kv_size - key.size(), val_prefix);
  return pair<string, string>(key, val);
}

//TODO
// -- parallel merge operations
// -- when merging from L-1 to L, including L in the merge in advance


