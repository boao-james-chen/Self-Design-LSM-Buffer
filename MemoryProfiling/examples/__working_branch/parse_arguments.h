#include "args.hxx"
#include "emu_environment.h"

int parse_arguments(int argc, char* argv[], EmuEnv* _env) {
  args::ArgumentParser parser("RocksDB_parser.", "");

  args::Group group1(parser, "This group is all exclusive:",
                     args::Group::Validators::DontCare);
  /*
    args::Group group1(parser, "This group is all exclusive:",
    args::Group::Validators::AtMostOne); args::Group group2(parser, "Path is
    needed:", args::Group::Validators::All); args::Group group3(parser, "This
    group is all exclusive (either N or L):", args::Group::Validators::Xor);
    args::Group group4(parser, "Optional switches and parameters:",
    args::Group::Validators::DontCare); args::Group group5(parser, "Optional
    less frequent switches and parameters:", args::Group::Validators::DontCare);
  */

  args::ValueFlag<int> destroy_database_cmd(
      group1, "d", "Destroy and recreate the database [def: 1]",
      {'d', "destroy"});
  // !YBS-sep09-XX!
  args::ValueFlag<int> clear_system_cache_cmd(
      group1, "cc", "Clear system cache [def: 1]", {"cc"});

  args::ValueFlag<int> size_ratio_cmd(
      group1, "T",
      "The number of unique inserts to issue in the experiment [def: 10]",
      {'T', "size_ratio"});
  args::ValueFlag<int> buffer_size_in_pages_cmd(
      group1, "P",
      "The number of unique inserts to issue in the experiment [def: 4096]",
      {'P', "buffer_size_in_pages"});
  args::ValueFlag<int> entries_per_page_cmd(
      group1, "B",
      "The number of unique inserts to issue in the experiment [def: 4]",
      {'B', "entries_per_page"});
  args::ValueFlag<int> entry_size_cmd(
      group1, "E",
      "The number of unique inserts to issue in the experiment [def: 1024 B]",
      {'E', "entry_size"});
  args::ValueFlag<long> buffer_size_cmd(
      group1, "M",
      "The number of unique inserts to issue in the experiment [def: 16 MB]",
      {'M', "memory_size"});
  args::ValueFlag<int> file_to_memtable_size_ratio_cmd(
      group1, "file_to_memtable_size_ratio",
      "The number of unique inserts to issue in the experiment [def: 1]",
      {'f', "file_to_memtable_size_ratio"});
  args::ValueFlag<long> file_size_cmd(
      group1, "file_size",
      "The number of unique inserts to issue in the experiment [def: 256 KB]",
      {'F', "file_size"});
  args::ValueFlag<int> verbosity_cmd(
      group1, "verbosity", "The verbosity level of execution [0,1,2; def: 0]",
      {'V', "verbosity"});
  args::ValueFlag<int> compaction_pri_cmd(
      group1, "compaction_pri",
      "[Compaction priority: 1 for kMinOverlappingRatio, 2 for "
      "kByCompensatedSize, 3 for kOldestLargestSeqFirst, 4 for "
      "kOldestSmallestSeqFirst; def: 1]",
      {'c', "compaction_pri"});
  // !YBS-sep07-XX!
  args::ValueFlag<int> compaction_style_cmd(
      group1, "compaction_style",
      "[Compaction priority: 1 for kCompactionStyleLevel, 2 for "
      "kCompactionStyleUniversal, 3 for kCompactionStyleFIFO, 4 for "
      "kCompactionStyleNone; def: 1]",
      {'C', "compaction_style"});
  args::ValueFlag<int> bits_per_key_cmd(
      group1, "bits_per_key",
      "The number of bits per key assigned to Bloom filter [def: 10]",
      {'b', "bits_per_key"});
  // !YBS-sep09-XX!
  args::ValueFlag<int> block_cache_cmd(
      group1, "bb", "Block cache size in MB [def: 8 MB]", {"bb"});
  // !YBS-sep17-XX!
  args::ValueFlag<int> show_progress_cmd(group1, "show_progress",
                                         "Show progress [def: 0]", {'s', "sp"});
  // !YBS-feb15-XXI!
  args::ValueFlag<double> del_per_th_cmd(
      group1, "del_per_th", "Delete persistence threshold [def: -1]",
      {'t', "dpth"});
  // !YBS-feb15-XXI!
  args::ValueFlag<int> enable_rocksdb_perf_iostat_cmd(
      group1, "enable_rocksdb_perf_iostat",
      "Enable RocksDB's internal Perf and IOstat [def: 0]", {"stat"});

  args::ValueFlag<long> num_inserts_cmd(
      group1, "inserts",
      "The number of unique inserts to issue in the experiment [def: 1]",
      {'i', "inserts"});

  // !!AS-feb08-XXIV!!
  args::ValueFlag<int> memtable_factory_cmd(
      group1, "memtable_factory",
      "[Memtable Factory: 1 for Skiplist, 2 for Vector, 3 for Hash Skiplist, 4 "
      "for Hash Linkedlist; def: 1]",
      {'m', "memtable_factory"});

  // !!AS-feb15-XXIV!!
  args::ValueFlag<int> prefix_length_cmd(
      group1, "prefix_length",
      "[Prefix Length: Number of bytes of the key forming the prefix; def: 0]",
      {'X', "prefix_length"});

  args::ValueFlag<long> bucket_count_cmd(
      group1, "bucket_count",
      "[Bucket Count: Number of buckets for the hash table in HashSkipList & "
      "HashLinkList Memtables; def: 50000]",
      {'H', "bucket_count"});

  args::ValueFlag<long> threshold_use_skiplist_cmd(
      group1, "threshold_use_skiplist",
      "[Threshold Use SkipList: Threshold based on which the conversion will "
      "happen from HashLinkList to HashSkipList; def: 256]",
      {"threshold_use_skiplist", "threshold_use_skiplist"});

  args::ValueFlag<long> vector_pre_allocation_size_cmd(
      group1,
      "preallocation_vector_size",
      "[Preallocation Vector Size: Size to preallocation to vector memtable; "
      "def: 0]", {'A', "preallocation_size"});

  try {
    parser.ParseCLI(argc, argv);
  } catch (args::Help&) {
    std::cout << parser;
    exit(0);
    // return 0;
  } catch (args::ParseError& e) {
    std::cerr << e.what() << std::endl;
    std::cerr << parser;
    return 1;
  } catch (args::ValidationError& e) {
    std::cerr << e.what() << std::endl;
    std::cerr << parser;
    return 1;
  }

  _env->destroy_database =
      destroy_database_cmd ? args::get(destroy_database_cmd) : 1;
  // !YBS-sep09-XX!
  _env->clear_system_cache =
      clear_system_cache_cmd ? args::get(clear_system_cache_cmd) : 1;

  _env->size_ratio = size_ratio_cmd ? args::get(size_ratio_cmd) : 10;
  _env->buffer_size_in_pages =
      buffer_size_in_pages_cmd ? args::get(buffer_size_in_pages_cmd) : 4096;
  _env->entries_per_page =
      entries_per_page_cmd ? args::get(entries_per_page_cmd) : 64;
  _env->entry_size = entry_size_cmd ? args::get(entry_size_cmd) : 64;
  _env->buffer_size = buffer_size_cmd
                          ? args::get(buffer_size_cmd)
                          : _env->buffer_size_in_pages *
                                _env->entries_per_page * _env->entry_size;
  _env->file_to_memtable_size_ratio =
      file_to_memtable_size_ratio_cmd
          ? args::get(file_to_memtable_size_ratio_cmd)
          : 1;
  _env->file_size =
      file_size_cmd ? args::get(file_size_cmd) : _env->buffer_size;
  _env->verbosity = verbosity_cmd ? args::get(verbosity_cmd) : 0;
  _env->compaction_pri = compaction_pri_cmd ? args::get(compaction_pri_cmd) : 1;
  // !YBS-sep07-XX!
  _env->compaction_style =
      compaction_style_cmd ? args::get(compaction_style_cmd) : 1;
  _env->bits_per_key = bits_per_key_cmd ? args::get(bits_per_key_cmd) : 10;
  _env->block_cache =
      // !YBS-sep09-XX!
      block_cache_cmd ? args::get(block_cache_cmd) : 8;
  // !YBS-sep17-XX!
  _env->show_progress = show_progress_cmd ? args::get(show_progress_cmd) : 0;
  // !YBS-feb15-XXI!
  _env->delete_persistence_latency =
      del_per_th_cmd ? args::get(del_per_th_cmd) : 0;
  // !YBS-feb15-XXI!
  _env->enable_rocksdb_perf_iostat =
      enable_rocksdb_perf_iostat_cmd ? args::get(enable_rocksdb_perf_iostat_cmd)
                                     : 0;

  _env->num_inserts = num_inserts_cmd ? args::get(num_inserts_cmd) : 0;

  // !YBS-sep07-XX!
  _env->target_file_size_base = _env->buffer_size;
  // !YBS-sep07-XX!
  _env->max_bytes_for_level_base = _env->buffer_size * _env->size_ratio;

  _env->memtable_factory =
      memtable_factory_cmd ? args::get(memtable_factory_cmd) : 1;

  _env->prefix_length = prefix_length_cmd ? args::get(prefix_length_cmd) : 0;

  _env->bucket_count =
      bucket_count_cmd ? args::get(bucket_count_cmd) : _env->bucket_count;
  _env->threshold_use_skiplist = threshold_use_skiplist_cmd
                                     ? args::get(threshold_use_skiplist_cmd)
                                     : _env->threshold_use_skiplist;
  _env->vector_pre_allocation_size =
      vector_pre_allocation_size_cmd ? args::get(vector_pre_allocation_size_cmd)
                                     : _env->vector_pre_allocation_size;
  return 0;
}
