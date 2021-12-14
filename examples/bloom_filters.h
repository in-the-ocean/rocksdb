/*
 * bloom_filters.h
 *
 *  Created on: Mar 24, 2018
 *      Author: niv
 */

#ifndef EXAMPLES_BLOOM_FILTERS_H_
#define EXAMPLES_BLOOM_FILTERS_H_

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/listener.h"
#include <cmath>

using namespace std;
using namespace rocksdb;

namespace dost {

class FluidLSMInfrastructure;

struct filter {
  filter();
  filter(double bits_per_entry);
  long long size;
  double false_positive_rate;
  double bits_per_element;
  long long mem;
  double calc();
  double calc_R();
  void print();
};

struct filter_stats {
  filter_stats();
  unsigned long long get_true_positives() const;
  unsigned long long get_false_positives() const;
  unsigned long long get_negatives() const;
  unsigned long long get_queries() const;
  void print() const;
  unsigned long long positives;
  unsigned long long true_positives;
  unsigned long long queries;
};

class CrimsonDBFiltersPolicy : public FilterPolicy {
public:
  explicit CrimsonDBFiltersPolicy(double bits_per_key, bool use_block_based_builder);
  virtual ~CrimsonDBFiltersPolicy();
  virtual void update_filters() {};
  virtual void init_filters(int num_levels) {};
  void set_db(DB* initialized_db, FluidLSMInfrastructure* tree);
  void set_stats_collection(filter_stats* const);
  bool KeyMayMatch(const Slice& key, const Slice& filter) const override;
  virtual void CreateFilter(const Slice* keys, int n, std::string* dst) const override;
  virtual void CreateFilterForLevel(const Slice* keys, int n, std::string* dst, int level) const override;
  double getFalsePosRateForLevel(int level) const;
  double getBitsPerEntryForLevel(int level) const;
  virtual FilterBitsBuilder* GetFilterBitsBuilder(int level) const override;
  virtual FilterBitsBuilder* GetFilterBitsBuilderWithParameters(double bits_per_entry, double num_probes) const override;
  virtual FilterBitsReader* GetFilterBitsReader(const Slice& contents) const override;
  const char* Name() const override;
  void print() const;
protected:
  void getBitsAndProbes(int rocksdb_level, double& bits_per_entry, double& num_probes) const;
  DB* db;
  FluidLSMInfrastructure* tree;
  rocksdb::BloomFilterPolicyWrapper* policy;
  double bits_per_key_;
  bool use_block_based_builder_;
  std::vector<filter> filters;
  filter_stats*  fs;

};

class FiltersOptimizer {
public:
  double eval_R(std::vector<filter>& filters, const int Z, const int K, const double v);
  double optimize_filters_memory_allocation(vector<filter>& filters, const int Z = 1, const int K = 1, const double v = 0);
  void print(vector<filter>& filters);
};


class MonkeyFilterPolicy : public CrimsonDBFiltersPolicy {
public:
  explicit MonkeyFilterPolicy(int bits_per_key, bool use_block_based_builder);
  void apply_formula_leveling(vector<filter>& filters, double L, double bits_per_key, double T) const;
  virtual void init_filters(int num_levels) override;
  void show_hypothetical_FPRs(int num_levels, double bits_per_key, int T) const;
private:
  bool apply_asymptotic_FPRs;
};

class CrimsonDBFilterBitsReader : public FilterBitsReader {
 public:
  CrimsonDBFilterBitsReader(const Slice& contents, filter_stats* stats);
  virtual ~CrimsonDBFilterBitsReader();
  virtual bool MayMatch(const Slice& entry);
 private:
  FilterBitsReaderWrapper* wrapper;
  filter_stats* stats;
};

};


#endif /* EXAMPLES_BLOOM_FILTERS_H_ */
