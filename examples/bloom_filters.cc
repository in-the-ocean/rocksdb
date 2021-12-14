/*
 * bloom_filters.cc
 *
 *  Created on: Apr 9, 2018
 *      Author: niv
 */

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/listener.h"
#include "bloom_filters.h"
#include "dostoevsky.h"

using namespace std;
using namespace rocksdb;
using namespace dost;

filter::filter() :
    size(0),  false_positive_rate(1),
    bits_per_element(0), mem(0) {}

filter::filter(double bits_per_entry) :
    size(0),  false_positive_rate(1),
    bits_per_element(bits_per_entry), mem(0) {
  double EULER = 2.71828182845904523536;
  double denom = 0.48045301391;
  false_positive_rate = pow(EULER, -bits_per_element * denom);
}

//filter(double bits_per_entry);

double filter::calc() {
  double denom = 0.48045301391;
  return false_positive_rate == 0 ? 0 : ( - size * std::log(false_positive_rate) ) / denom;
}

void filter::print() {
  fprintf(stderr,"\t%lld    %.10f : %f     %lld    \n", size, false_positive_rate, calc_R() / size, mem);
}

double filter::calc_R() {
  double EULER = 2.71828182845904523536;
  double denom = 0.48045301391;
  return size == 0 ? 0 : pow(EULER, -((long double)mem / (long double)size) * denom);
}



filter_stats::filter_stats() : positives(0), true_positives(0), queries(0) {}
unsigned long long filter_stats::get_true_positives() const { return true_positives; }
unsigned long long filter_stats::get_false_positives() const { return positives - true_positives; }
unsigned long long filter_stats::get_negatives() const { return queries - positives; }
unsigned long long filter_stats::get_queries() const { return queries; }

void filter_stats::print() const {
  fprintf(stderr,"  true  positives: %lld  \n", get_true_positives());
  fprintf(stderr,"  false positives: %lld  \n", get_false_positives());
  fprintf(stderr,"  negatives      : %lld  \n", get_negatives());
  fprintf(stderr,"  queries        : %lld  \n", get_queries());
}

CrimsonDBFiltersPolicy::~CrimsonDBFiltersPolicy() {
  delete policy;
}

CrimsonDBFiltersPolicy::CrimsonDBFiltersPolicy(double bits_per_key, bool use_block_based_builder) :
 db(nullptr), tree(nullptr),
 policy(new BloomFilterPolicyWrapper(bits_per_key, use_block_based_builder)),
 bits_per_key_(bits_per_key),
 use_block_based_builder_(use_block_based_builder), filters(), fs(nullptr) {}

void CrimsonDBFiltersPolicy::set_stats_collection(filter_stats*  new_fs) {
  fs = new_fs;
}

bool CrimsonDBFiltersPolicy::KeyMayMatch(const Slice& key, const Slice& filter) const  {
  bool positive = policy->KeyMayMatch(key, filter);
  if (fs) {
    fs->queries++;
    fs->positives += positive;
  }
  return positive;
}

void CrimsonDBFiltersPolicy::CreateFilter(const Slice* keys, int n, std::string* dst) const  {
  policy->CreateFilterForLevel(keys, n, dst, -1);
}

void CrimsonDBFiltersPolicy::CreateFilterForLevel(const Slice* keys, int n, std::string* dst, int rocksdb_level) const {
  double bits_per_entry = 0, num_probes = 0;
  getBitsAndProbes(rocksdb_level, bits_per_entry, num_probes);
  policy->CreateFilterWithParameters(keys, n, dst, bits_per_entry, num_probes);
}

double CrimsonDBFiltersPolicy::getFalsePosRateForLevel(int level) const {
  return ((unsigned int) level) >= filters.size() ? 0 : filters[level].false_positive_rate;
}

double CrimsonDBFiltersPolicy::getBitsPerEntryForLevel(int level) const {
  return ((unsigned int) level) >= filters.size() ? 0 : filters[level].bits_per_element;
}

void CrimsonDBFiltersPolicy::set_db(DB* initialized_db, FluidLSMInfrastructure* initialized_tree) {
  db = initialized_db;
  tree = initialized_tree;
}


MonkeyFilterPolicy::MonkeyFilterPolicy(int bits_per_key, bool use_block_based_builder) :
    CrimsonDBFiltersPolicy(bits_per_key, use_block_based_builder), apply_asymptotic_FPRs(false) {}

void MonkeyFilterPolicy::init_filters(int num_levels) {
  double T = tree->get_size_ratio();
  filters = std::vector<filter>(num_levels + 1);
  apply_formula_leveling(filters, filters.size(), bits_per_key_, T);
}


void MonkeyFilterPolicy::apply_formula_leveling(vector<filter>& new_filters, double L, double bits_per_key, double T) const {
  double EULER = 2.71828182845904523536;
  double denom = 0.48045301391;
  double R = pow(EULER, -(bits_per_key * denom)) * ( pow(T, T / (T-1)) / (T-1) );
  double constant_across = (pow(T, L-1) / (pow(T, L) - 1)) * (T-1);
  for (int i = 0; i < L; i++) {
    new_filters[i].false_positive_rate = (R * constant_across) / pow(T, L - i);
    new_filters[i].bits_per_element = - log(new_filters[i].false_positive_rate) / (log(2) * log(2));
  }
}

void MonkeyFilterPolicy::show_hypothetical_FPRs(int num_levels, double bits_per_key, int T) const {
  std::vector<filter> temp_filters = std::vector<filter>(num_levels + 1);
  apply_formula_leveling(temp_filters, num_levels + 1, bits_per_key, T);

  for (int i = 0; i < temp_filters.size(); i++) {
    fprintf(stderr,"level %d FPR: %f    bits per entry:  %f\n", i, temp_filters[i].false_positive_rate, temp_filters[i].bits_per_element);
  }
}


void CrimsonDBFiltersPolicy::getBitsAndProbes(int rocksdb_level, double& bits_per_entry, double& num_probes) const {
  assert(db != nullptr);
  assert(policy != nullptr);
  assert(tree != nullptr);
  assert(rocksdb_level >= 0);
  unsigned long level = tree->get_fluidLSM_level(rocksdb_level);
  // what is this assert, why is it caused?
  #ifndef NDEBUG
  assert(level >= 0 && level < filters.size());
  #endif

  bits_per_entry = filters[level].bits_per_element;
  num_probes = ceil(bits_per_entry * 0.69);
  //num_probes = num_probes < 1.0 ? 1 : num_probes;
  //  /num_probes = ceil(num_probes);
  //bits_per_entry = floor(bits_per_entry);
}

FilterBitsBuilder* CrimsonDBFiltersPolicy::GetFilterBitsBuilder(int rocksdb_level) const {
  if (use_block_based_builder_) {
    return nullptr;
  }
  double bits_per_entry = 0, num_probes = 0;
  getBitsAndProbes(rocksdb_level, bits_per_entry, num_probes);
  return policy->GetFilterBitsBuilderWithParameters(bits_per_entry, num_probes);
}

FilterBitsBuilder* CrimsonDBFiltersPolicy::GetFilterBitsBuilderWithParameters(double bits_per_entry, double num_probes) const  {
  return policy->GetFilterBitsBuilderWithParameters(bits_per_entry, num_probes);
}

FilterBitsReader* CrimsonDBFiltersPolicy::GetFilterBitsReader(const Slice& contents) const  {
  return new CrimsonDBFilterBitsReader(contents, fs);
}

const char* CrimsonDBFiltersPolicy::Name() const {
  return policy->Name();
}

void CrimsonDBFiltersPolicy::print() const {
  for (unsigned int i = 0; i < filters.size(); i++) {
    fprintf(stderr,"level %d FPR: %f    bits per entry:  %f\n", i, filters[i].false_positive_rate, filters[i].bits_per_element);
  }
}

CrimsonDBFilterBitsReader::CrimsonDBFilterBitsReader(const Slice& contents, filter_stats* stats)
 : wrapper(new FilterBitsReaderWrapper(contents)), stats(stats) {}

CrimsonDBFilterBitsReader::~CrimsonDBFilterBitsReader() {
  delete wrapper;
}

bool CrimsonDBFilterBitsReader::MayMatch(const Slice& entry) {
  bool positive = wrapper->MayMatch(entry);
  if (stats) {
    stats->queries++;
    stats->positives += positive;
  }
  return positive;
}

void FiltersOptimizer::print(vector<filter>& filters) {
  for (unsigned int i = 0; i < filters.size(); i++) {
    fprintf(stderr,"%.3f  ", filters[i].calc_R());
  }
  fprintf(stderr,"\n");
}

double FiltersOptimizer::eval_R(std::vector<filter>& filters, const int Z, const int K, const double v) {
  double R = 0;
  for (unsigned int i = 0; i < filters.size(); i++) {
    // fprintf(stderr,"%d   %d  %f \n", i,  rates[i].size, rates[i].false_positive_rate);
    double val = filters[i].calc_R();
    val *= (i == filters.size() - 1) ? Z : K;
    R += val;
  }
  double pL = filters.back().calc_R();
  double zero_result_cost = R;
  double existing_result_cost = 1 + R - pL * ((Z + 1) / 2);
  double weighted_read_cost = v * existing_result_cost + zero_result_cost;
  return weighted_read_cost;
}



double FiltersOptimizer::optimize_filters_memory_allocation(vector<filter>& filters, const int Z, const int K, const double v) {
  if (filters.empty()) { return 0; }

  long long diff = 1;
  for (auto&f : filters) {
      diff += f.size;
  }
  //print_detail(filters, 1);
  double current_R = eval_R(filters, Z, K, v);
  // double original = current_R;
//  /fprintf(stderr,"start val:  %f   diff: %ll \n", current_R, diff);
  //fprintf(stderr,"same mem strategy:  %f \n", current_R  );
  //print(filters, current, 0);
  int change = true;
  int iteration = 0;
  while (true) {
    change = false;
    for (int i = (int)filters.size() - 1; i >= 1; i--) {
      for (int j = i - 1; j >= 0 ; j--) {
        filters[i].mem += diff;
        filters[j].mem -= diff;
        double value = eval_R(filters, Z, K, v);
        if (value < current_R && value > 0 && filters[j].mem > 0 ) {
          //fprintf(stderr,"moving %d from %d to %d \n", diff, j, i);
          //print(filters);
          current_R = value;
          change = true;
          continue;
        }
        filters[i].mem -= diff * 2;
        filters[j].mem += diff * 2;
        value = eval_R(filters, Z, K, v);
        if (value < current_R && value > 0 && filters[i].mem > 0 ) {
          //fprintf(stderr,"moving %d from %d to %d \n", diff, i, j);
          //print(filters);
          current_R = value;
          change = true;
          continue;
        }
        filters[i].mem += diff;
        filters[j].mem -= diff;
      }
    }
    if (!change) {
      diff /= 2.0;
      if (diff < 1) {
        break;
      }
    }
    //print(filters, current, iteration);
    iteration++;
    //print_detail(filters, current);
  }
  for (unsigned int i = 0; i < filters.size(); i++) {
    filters[i].false_positive_rate = filters[i].calc_R();
    if (filters[i].size == 0) {
      filters[i].bits_per_element = 0;
    }
    else {
      filters[i].bits_per_element = (long double)filters[i].mem / (long double)filters[i].size;
    }
    //std::cout << "FPR  " << filters[i].false_positive_rate << "   bits per element  " << filters[i].bits_per_element << std::endl;
  }
  ////fprintf(stderr,"size ratio:  %f\n", size_ratio);
  ////print_detail(filters, current_R);
  //fprintf(stderr,"ratio saving  %f \n", current_R / original );
  //fprintf(stderr,"original\t%f  IO       \n", original);
  ////total_uniform_RAM3(limit_on_reads, size_ratio, filters.back().size, page_size);
  //fprintf(stderr,"current \t%f  IO   \n", current_R );
  ////fprintf(stderr,"size:  %ld\n", filters.back().size);
  return current_R;
}
