
#include <cstdio>
#include <string>

#include <thread>
#include <chrono>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"

using namespace ROCKSDB_NAMESPACE;

#if defined(OS_WIN)
std::string kDBPath = "C:\\Windows\\TEMP\\rocksdb_simple_example";
#else
std::string kDBPath = "/tmp/rocksdb_test_example";
#endif

int main() {
  DB* db;
  Options options;
  // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
  // create the DB if it's not already present
  options.create_if_missing = true;
  options.level_compaction_dynamic_level_bytes = false;
  options.write_buffer_size = 18;

  // open DB
  Status s = DB::Open(options, kDBPath, &db);
  assert(s.ok());

  s = db->SetOptions({{"write_buffer_size", "18"}});
  assert(s.ok());
  s = db->SetOptions({{"max_bytes_for_level_base", "10000000"}});
  assert(s.ok());

  for (int i = 20; i < 40; i++) {
    s = db->Put(WriteOptions(), "key" + std::to_string(i), "value");
    assert(s.ok());
    std::this_thread::sleep_for (std::chrono::milliseconds(100));
  }


  // Put key-value
  s = db->Put(WriteOptions(), "key1", "value");
  assert(s.ok());
  std::string value;
  // get value
  s = db->Get(ReadOptions(), "key1", &value);
  assert(s.ok());
  assert(value == "value");

  std::this_thread::sleep_for (std::chrono::milliseconds(1000));

  delete db;

  return 0;
}
