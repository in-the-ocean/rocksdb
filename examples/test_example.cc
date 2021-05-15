
#include <cstdio>
#include <string>

#include <thread>
#include <chrono>
#include <iostream>
#include <random>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"

using namespace ROCKSDB_NAMESPACE;

#if defined(OS_WIN)
std::string kDBPath = "C:\\Windows\\TEMP\\rocksdb_test_example";
#else
std::string kDBPath = "/tmp/rocksdb_test_example";
#endif

int main(int argc, char* argv[]) {
  DB* db;
  Options options;
  // create the DB if it's not already present
  options.create_if_missing = true;
  options.level_compaction_dynamic_level_bytes = false;
  options.level0_file_num_compaction_trigger = 20;
  options.curr_max_level = 0;
  options.max_background_jobs = 5;
  // options.min_write_buffer_number_to_merge = 2;
  options.max_bytes_for_level_base = 64 << 12;
  options.write_buffer_size = 64 << 10;

  // open DB
  Status s = DB::Open(options, kDBPath, &db);
  assert(s.ok());

  // s = db->SetOptions({{"write_buffer_size", "131072"},
  //                     {"max_bytes_for_level_base", "4000"}});
  assert(s.ok());
  std::this_thread::sleep_for (std::chrono::milliseconds(100));

  std::random_device rd;
  std::mt19937 mt(rd());
  std::uniform_int_distribution<int> dist(100000, 999999);

  std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();

  for (int j = 0; j < std::atoi(argv[1]); j++) {
    s = db->Put(WriteOptions(), "key" + std::to_string(dist(mt)), "vals");
    // if (!s.ok()) {
    //   std::cout << s.getState() << std::endl;
    // }
    assert(s.ok());
    // std::this_thread::sleep_for (std::chrono::milliseconds(10));
  }

  std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();

  std::cout << "Time difference = " << std::chrono::duration_cast<std::chrono::seconds>(end - begin).count() << "[s]" << std::endl;
  std::cout << "Time difference = " << std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count() << "[µs]" << std::endl;

  // std::this_thread::sleep_for (std::chrono::milliseconds(1000));

  delete db;

  return 0;
}
