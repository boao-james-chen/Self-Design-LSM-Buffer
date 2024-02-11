#include <rocksdb/db.h>
#include <rocksdb/table.h>
// !YBS-sep01-XX!
#include <rocksdb/perf_context.h>
// !YBS-sep01-XX!
#include <rocksdb/iostats_context.h>

#include <iomanip>
#include <thread>

#include "aux_time.h"
#include "config_options.h"
#include "emu_environment.h"
#include "stats.h"

std::string kDBPath = "./db_working_home";
std::mutex mtx;
std::condition_variable cv;
bool compaction_complete = false;

struct operation_tracker {
  long _inserts_completed = 0;
  long _updates_completed = 0;
  long _point_deletes_completed = 0;
  long _range_deletes_completed = 0;
  long _point_queries_completed = 0;
  long _range_queries_completed = 0;
} op_track;

void printExperimentalSetup(EmuEnv* _env);
void printWorkloadStatistics(operation_tracker op_track);
inline void showProgress(const uint32_t& workload_size,
                         const uint32_t& counter);

class CompactionsListner : public rocksdb::EventListener {
 public:
  explicit CompactionsListner() {}

  void OnCompactionCompleted(
      rocksdb::DB* /*db*/, const rocksdb::CompactionJobInfo& /*ci*/) override {
    std::lock_guard<std::mutex> lock(mtx);
    compaction_complete = true;
    cv.notify_one();
  }
};

void WaitForCompactions(rocksdb::DB* db) {
  std::unique_lock<std::mutex> lock(mtx);
  uint64_t num_running_compactions;
  uint64_t pending_compaction_bytes;
  uint64_t num_pending_compactions;

  while (!compaction_complete) {
    // Check if there are ongoing or pending compactions
    db->GetIntProperty("rocksdb.num-running-compactions",
                       &num_running_compactions);
    db->GetIntProperty("rocksdb.estimate-pending-compaction-bytes",
                       &pending_compaction_bytes);
    db->GetIntProperty("rocksdb.compaction-pending", &num_pending_compactions);
    if (num_running_compactions == 0 && pending_compaction_bytes == 0 &&
        num_pending_compactions == 0) {
      break;
    }
    cv.wait(lock);
  }
}

int runWorkload(EmuEnv* _env) {
  DB* db;
  Options options;
  WriteOptions w_options;
  ReadOptions r_options;
  BlockBasedTableOptions table_options;
  FlushOptions f_options;

  configOptions(_env, &options, &table_options, &w_options, &r_options,
                &f_options);

  // options.allow_concurrent_memtable_write =
  // _env->allow_concurrent_memtable_write; options.create_if_missing =
  // _env->create_if_missing;

  if (_env->destroy_database) {
    DestroyDB(kDBPath, options);
    std::cout << "Destroying database ..." << std::endl;
  }

  std::shared_ptr<CompactionsListner> listener =
      std::make_shared<CompactionsListner>();
  options.listeners.emplace_back(listener);

  printExperimentalSetup(_env);  // !YBS-sep07-XX!
  std::cout << "Maximum #OpenFiles = " << options.max_open_files
            << std::endl;  // !YBS-sep07-XX!
  std::cout << "Maximum #ThreadsUsedToOpenFiles = "
            << options.max_file_opening_threads << std::endl;  // !YBS-sep07-XX!
  Status s = DB::Open(options, kDBPath, &db);
  if (!s.ok()) std::cerr << s.ToString() << std::endl;
  assert(s.ok());

  Stats* fade_stats = Stats::getInstance();
  fade_stats->db_open = true;

  int current_level = fade_stats->levels_in_tree;

  // opening workload file for the first time
  ifstream workload_file;
  workload_file.open("workload.txt");
  assert(workload_file);
  // doing a first pass to get the workload size
  uint32_t workload_size = 0;
  std::string line;
  while (std::getline(workload_file, line)) ++workload_size;
  workload_file.close();

  // !YBS-sep09-XX!
  // Clearing the system cache
  if (_env->clear_system_cache) {
    std::cout << "Clearing system cache ..." << std::endl;
    system("sudo sh -c 'echo 3 >/proc/sys/vm/drop_caches'");
  }
  // START stat collection
  if (_env->enable_rocksdb_perf_iostat == 1) {  // !YBS-feb15-XXI!
    // begin perf/iostat code
    rocksdb::SetPerfLevel(
        rocksdb::PerfLevel::kEnableTimeAndCPUTimeExceptForMutex);
    rocksdb::get_perf_context()->Reset();
    rocksdb::get_perf_context()->ClearPerLevelPerfContext();
    rocksdb::get_perf_context()->EnablePerLevelPerfContext();
    rocksdb::get_iostats_context()->Reset();
    // end perf/iostat code
  }
  if (options.compaction_style == kCompactionStyleUniversal)
    std::cout << "Compaction Eagerness: Tiering (kCompactionStyleUniversal)"
              << std::endl;
  else if (_env->compaction_pri == 9)  // !YBS-apr15-XXI!
    std::cout << "Compaction Eagerness: 1-Leveling (variation of "
                 "kCompactionStyleLeveling)"
              << std::endl;  // !YBS-apr06-XXI!
  else
    std::cout
        << "Compaction Eagerness: Hybrid leveling (kCompactionStyleLeveling)"
        << std::endl;
  // !END

  // re-opening workload file to execute workload
  workload_file.open("workload.txt");
  assert(workload_file);

  Iterator* it = db->NewIterator(r_options);  // for range reads
  uint32_t counter = 0;                       // for progress bar

#ifdef TIMER
  unsigned long long cum_time_all_operations = 0;
  unsigned long long cum_time_workload = 0;
  unsigned long long cum_time_inserts = 0;
  unsigned long long cum_time_updates = 0;
  unsigned long long cum_time_pointq = 0;
  unsigned long long cum_time_range_queries = 0;
  auto workload_start = std::chrono::high_resolution_clock::now();
  std::vector<unsigned long long> each_range_query_time;
#endif  // TIMER

  for (int i = 0; i < 20; ++i) {
    _env->level_delete_persistence_latency[i] = -1;
    _env->RR_level_last_file_selected[i] = -1;  // !YBS-sep06-XX!
  }
  // !YBS-sep09-XX!
  my_clock start_time, end_time;
  if (my_clock_get_time(&start_time) == -1)
    std::cerr << "Failed to get experiment start time" << std::endl;
  // !END
  // !YBS-apr15-XXI!
  if (_env->compaction_pri == 9) {
    _env->level0_file_num_compaction_trigger = _env->size_ratio;
    _env->level0_stop_writes_trigger = _env->size_ratio;
    // std::cout << "Compaction style = 1-Leveling :: Overloading
    // level0_file_num_compaction_trigger to " <<
    // _env->level0_file_num_compaction_trigger; std::cout << "Compaction style
    // = 1-Leveling :: Overloading level0_stop_writes_trigger to " <<
    // _env->level0_stop_writes_trigger;
  }
  // !END

  while (!workload_file.eof()) {
    char instruction;
    long key, start_key, end_key;
    string value;
    workload_file >> instruction;
    _env->current_op = instruction;  // !YBS-sep18-XX!
    switch (instruction) {
      case 'I':  // insert
        workload_file >> key >> value;
        // if (_env->verbosity == 2) std::cout << instruction << " " << key << "
        // " << value << "" << std::endl; Put key-value
        {
#ifdef TIMER
          auto start = std::chrono::high_resolution_clock::now();
#endif  // TIMER
          s = db->Put(w_options, to_string(key), value);
#ifdef TIMER
          auto stop = std::chrono::high_resolution_clock::now();
          auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(
              stop - start);
          cum_time_all_operations += duration.count();
          cum_time_inserts += duration.count();
#endif  // TIMER
        }
        if (!s.ok()) std::cerr << s.ToString() << std::endl;
        assert(s.ok());
        op_track._inserts_completed++;
        counter++;
        fade_stats->inserts_completed++;
        break;

      case 'U':  // update
        workload_file >> key >> value;
        // if (_env->verbosity == 2) std::cout << instruction << " " << key << "
        // " << value << "" << std::endl; Put key-value
        {
#ifdef TIMER
          auto start = std::chrono::high_resolution_clock::now();
#endif  // TIMER
          s = db->Put(w_options, to_string(key), value);
#ifdef TIMER
          auto stop = std::chrono::high_resolution_clock::now();
          auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(
              stop - start);
          cum_time_all_operations += duration.count();
          cum_time_updates += duration.count();
#endif  // TIMER
        }
        if (!s.ok()) std::cerr << s.ToString() << std::endl;
        assert(s.ok());
        op_track._updates_completed++;
        counter++;
        fade_stats->updates_completed++;
        break;

      case 'D':  // point delete
        workload_file >> key;
        // if (_env->verbosity == 2) std::cout << instruction << " " << key <<
        // "" << std::endl;
        s = db->Delete(w_options, to_string(key));
        assert(s.ok());
        op_track._point_deletes_completed++;
        counter++;
        fade_stats->point_deletes_completed++;
        break;

      case 'R':  // range delete
        workload_file >> start_key >> end_key;
        // if (_env->verbosity == 2) std::cout << instruction << " " <<
        // start_key << " " << end_key << "" << std::endl;
        s = db->DeleteRange(w_options, std::to_string(start_key),
                            std::to_string(end_key));
        assert(s.ok());
        op_track._range_deletes_completed++;
        counter++;
        fade_stats->range_deletes_completed++;
        break;

      case 'Q':  // probe: point query
        workload_file >> key;
        // if (_env->verbosity == 2) std::cout << instruction << " " << key <<
        // "" << std::endl;
        {
#ifdef TIMER
          auto start = std::chrono::high_resolution_clock::now();
#endif  // TIMER

          s = db->Get(r_options, to_string(key), &value);

#ifdef TIMER
          auto stop = std::chrono::high_resolution_clock::now();
          auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(
              stop - start);
          cum_time_all_operations += duration.count();
          cum_time_pointq += duration.count();
#endif  // TIMER
        }
        // if (!s.ok()) std::cerr << s.ToString() << "key = " << key <<
        // std::endl;
        //  assert(s.ok());
        op_track._point_queries_completed++;
        counter++;
        fade_stats->point_queries_completed++;
        break;

      case 'S':  // scan: range query
        workload_file >> start_key >> end_key;
        // std::cout << instruction << " " << start_key << " " << end_key << ""
        // << std::endl;
        {
#ifdef TIMER
          auto start = std::chrono::high_resolution_clock::now();
#endif  // TIMER
          it->Refresh();
          assert(it->status().ok());
          for (it->Seek(std::to_string(start_key)); it->Valid(); it->Next()) {
            // std::cout << "found key = " << it->key().ToString() << std::endl;
            if (it->key().ToString() == std::to_string(end_key)) {
              break;
            }
          }
          if (!it->status().ok()) {
            std::cerr << it->status().ToString() << std::endl;
          }

#ifdef TIMER
          auto stop = std::chrono::high_resolution_clock::now();
          auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(
              stop - start);
          cum_time_all_operations += duration.count();
          cum_time_range_queries += duration.count();
          each_range_query_time.push_back(duration.count());
#endif  // TIMER
        }

        op_track._range_queries_completed++;
        counter++;
        fade_stats->range_queries_completed++;
        break;

      default:
        std::cerr << "ERROR: Case match NOT found !!" << std::endl;
        break;
    }

    if (workload_size < 100) workload_size = 100;
    if (counter % (workload_size / 100) == 0 && _env->show_progress) {
      showProgress(workload_size, counter);
    }
  }

  fade_stats->completion_status = true;

  // // Close DB in a way of detecting errors
  // // followed by deleting the database object when examined to determine if
  // there were any errors.
  // // Regardless of errors, it will release all resources and is irreversible.
  // // Flush the memtable before close
  // Status CloseDB(DB *&db, const FlushOptions &flush_op);

  workload_file.close();

  {
    std::vector<std::string> live_files;
    uint64_t manifest_size;
    db->GetLiveFiles(live_files, &manifest_size, true /*flush_memtable*/);
    WaitForCompactions(db);
  }

  {
#ifdef TIMER
    auto workload_stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(
        workload_stop - workload_start);
    cum_time_workload += duration.count();
#endif  // TIMER
  }

  //   CompactionMayAllComplete(db);
  s = db->Close();
  if (!s.ok()) std::cerr << s.ToString() << std::endl;
  assert(s.ok());
  delete db;
  fade_stats->db_open = false;
  std::cout << "\n----------------------Closing DB-----------------------"
            << " completion_status = " << fade_stats->completion_status
            << std::endl;

  //   // sleep(5);

  //   // reopening (and closing the DB to flush LOG to .sst file)
  //   // _env->compaction_pri = kMinOverlappingRatio;
  //   std::cout << "Re-opening DB -- Re-setting compaction style to: "
  //             << _env->compaction_pri << "\n";
  //   s = DB::Open(options, kDBPath, &db);
  //   fade_stats->db_open = true;
  //   if (!s.ok()) std::cerr << s.ToString() << std::endl;
  //   assert(s.ok());
  //   s = db->Close();
  //   if (!s.ok()) std::cerr << s.ToString() << std::endl;
  //   assert(s.ok());

  std::cout << "End of experiment - TEST !!\n";

#ifdef TIMER
  std::cout << "=====================" << std::endl;
  std::cout << "total workload execution time: " << cum_time_workload
            << "(ns) / " << cum_time_workload / 1e9 << "(sec)" << std::endl;
  std::cout << "total time spent in all operations: " << cum_time_all_operations
            << "(ns) / " << cum_time_all_operations / 1e9 << "(sec)"
            << std::endl;
  std::cout << "total time spent for inserts: " << cum_time_inserts << "(ns) / "
            << cum_time_inserts / 1e9 << "(sec)" << std::endl;
  std::cout << "total time spent for updates: " << cum_time_updates << "(ns) / "
            << cum_time_updates / 1e9 << "(sec)" << std::endl;
  std::cout << "total time spent for point queries" << cum_time_pointq
            << "(ns) / " << cum_time_pointq / 1e9 << "(sec)" << std::endl;
  std::cout << "total time spent for range queries: " << cum_time_range_queries
            << "(ns) / " << cum_time_range_queries / 1e9 << "(sec)"
            << std::endl;

  std::cout << std::endl;
#endif  // TIMER

  // !YBS-sep09-XX!
  if (my_clock_get_time(&end_time) == -1)
    std::cerr << "Failed to get experiment end time" << std::endl;

  fade_stats->exp_runtime = getclock_diff_ns(start_time, end_time);
  // !END

  // !YBS-sep01-XX
  // STOP stat collection & PRINT stat
  if (_env->enable_rocksdb_perf_iostat == 1) {  // !YBS-feb15-XXI!
    // sleep(5);
    rocksdb::SetPerfLevel(rocksdb::PerfLevel::kDisable);
    std::cout << "RocksDB Perf Context : " << std::endl;
    std::cout << rocksdb::get_perf_context()->ToString() << std::endl;
    std::cout << "RocksDB Iostats Context : " << std::endl;
    std::cout << rocksdb::get_iostats_context()->ToString() << std::endl;
    // END ROCKS PROFILE
    // Print Full RocksDB stats
    std::cout << "RocksDB Statistics : " << std::endl;
    std::cout << options.statistics->ToString() << std::endl;
    std::cout << "----------------------------------------" << std::endl;
    // std::string tr_mem;
    // db->GetProperty("rocksdb.estimate-table-readers-mem", &tr_mem);
    // // Print Full RocksDB stats
    // std::cout << "RocksDB Estimated Table Readers Memory (index, filters) : "
    // << tr_mem << std::endl;
  }
  // !END

  printWorkloadStatistics(op_track);

  return 1;
}

// !YBS-sep07-XX
void printExperimentalSetup(EmuEnv* _env) {
  int l = 10;

  std::cout << std::setfill(' ') << std::setw(l)
            << "cmpt_sty"  // !YBS-sep07-XX!
            << std::setfill(' ') << std::setw(l) << "cmpt_pri"
            << std::setfill(' ') << std::setw(4) << "T" << std::setfill(' ')
            << std::setw(l) << "P" << std::setfill(' ') << std::setw(l) << "B"
            << std::setfill(' ') << std::setw(l) << "E" << std::setfill(' ')
            << std::setw(l)
            << "M"
            // << std::setfill(' ') << std::setw(l)  << "f"
            << std::setfill(' ') << std::setw(l) << "file_size"
            << std::setfill(' ') << std::setw(l) << "L1_size"
            << std::setfill(' ') << std::setw(l) << "blk_cch"  // !YBS-sep09-XX!
            << std::setfill(' ') << std::setw(l) << "BPK"
            << "\n";
  std::cout << std::setfill(' ') << std::setw(l)
            << _env->compaction_style;  // !YBS-sep07-XX!
  std::cout << std::setfill(' ') << std::setw(l) << _env->compaction_pri;
  std::cout << std::setfill(' ') << std::setw(4) << _env->size_ratio;
  std::cout << std::setfill(' ') << std::setw(l) << _env->buffer_size_in_pages;
  std::cout << std::setfill(' ') << std::setw(l) << _env->entries_per_page;
  std::cout << std::setfill(' ') << std::setw(l) << _env->entry_size;
  std::cout << std::setfill(' ') << std::setw(l) << _env->buffer_size;
  // std::cout << std::setfill(' ') << std::setw(l) <<
  // _env->file_to_memtable_size_ratio;
  std::cout << std::setfill(' ') << std::setw(l) << _env->file_size;
  std::cout << std::setfill(' ') << std::setw(l)
            << _env->max_bytes_for_level_base;
  std::cout << std::setfill(' ') << std::setw(l)
            << _env->block_cache;  // !YBS-sep09-XX!
  std::cout << std::setfill(' ') << std::setw(l) << _env->bits_per_key;
  // std::cout << std::setfill(' ') << std::setw(l) <<
  // _env->delete_persistence_latency; // !YBS-feb15-XXI!

  std::cout << std::endl;
}

void printWorkloadStatistics(operation_tracker op_track) {
  int l = 10;

  std::cout << std::setfill(' ') << std::setfill(' ') << std::setw(l) << "#I"
            << std::setfill(' ') << std::setw(l) << "#U" << std::setfill(' ')
            << std::setw(l) << "#PD" << std::setfill(' ') << std::setw(l)
            << "#RD" << std::setfill(' ') << std::setw(l) << "#PQ"
            << std::setfill(' ') << std::setw(l) << "#RQ"
            << "\n";

  std::cout << std::setfill(' ') << std::setw(l) << op_track._inserts_completed;
  std::cout << std::setfill(' ') << std::setw(l) << op_track._updates_completed;
  std::cout << std::setfill(' ') << std::setw(l)
            << op_track._point_deletes_completed;
  std::cout << std::setfill(' ') << std::setw(l)
            << op_track._range_deletes_completed;
  std::cout << std::setfill(' ') << std::setw(l)
            << op_track._point_queries_completed;
  std::cout << std::setfill(' ') << std::setw(l)
            << op_track._range_queries_completed;

  std::cout << std::endl;
}
// !END

inline void showProgress(const uint32_t& workload_size,
                         const uint32_t& counter) {
  // std::cout << "flag = " << flag << std::endl;
  // std::cout<<"2----";

  if (counter / (workload_size / 100) >= 1) {
    for (int i = 0; i < 104; i++) {
      std::cout << "\b";
      fflush(stdout);
    }
  }
  for (int i = 0; i < counter / (workload_size / 100); i++) {
    std::cout << "=";
    fflush(stdout);
  }
  std::cout << std::setfill(' ')
            << std::setw(101 - counter / (workload_size / 100));
  std::cout << counter * 100 / workload_size << "%";
  fflush(stdout);

  if (counter == workload_size) {
    std::cout << "\n";
    return;
  }
}
