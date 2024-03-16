#include <rocksdb/db.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/options.h>
#include <rocksdb/slice_transform.h>
#include <rocksdb/statistics.h>
#include <rocksdb/table.h>

#include "emu_environment.h"

using namespace rocksdb;

void configOptions(EmuEnv *_env, Options *op, BlockBasedTableOptions *t_op,
                   WriteOptions *w_op, ReadOptions *r_op, FlushOptions *f_op) {
  // !YBS-sep01-XX!
  op->statistics = CreateDBStatistics();
  // !YBS-sep07-XX!
  op->write_buffer_size = _env->buffer_size;
  // min 2 // !YBS-sep07-XX!
  op->max_write_buffer_number = _env->max_write_buffer_number;

  // ============================================================ //
  //     Default arguments for HashSkipList and HashLinkList      //
  // ============================================================ //

  int32_t skiplist_height = 4;
  int32_t skiplist_branching_factor = 4;
  size_t huge_page_tlb_size = 0;
  int bucket_entries_logging_threshold = 4096;
  bool if_log_bucket_dist_when_flash = true;

  // ============================================================ //

  switch (_env->memtable_factory) {
    case 1:
      op->memtable_factory =
          std::shared_ptr<SkipListFactory>(new SkipListFactory);
      break;
    case 2:
      op->memtable_factory =
          std::shared_ptr<VectorRepFactory>(new VectorRepFactory(20000000));
      break;
    case 3:
      op->memtable_factory.reset(NewHashSkipListRepFactory(
          _env->bucket_count, skiplist_height, skiplist_branching_factor));
      op->prefix_extractor.reset(NewFixedPrefixTransform(_env->prefix_length));
      break;
    case 4:
      op->memtable_factory.reset(NewHashLinkListRepFactory(
          _env->bucket_count, huge_page_tlb_size,
          bucket_entries_logging_threshold, if_log_bucket_dist_when_flash,
          _env->threshold_use_skiplist));
      op->prefix_extractor.reset(NewFixedPrefixTransform(_env->prefix_length));
      break;
    default:
      std::cerr << "error: memtable_factory" << std::endl;
  }

  // Compaction
  switch (_env->compaction_pri) {
    case 9:
    case 1:
      op->compaction_pri = kMinOverlappingRatio;
      break;
    case 2:
      op->compaction_pri = kByCompensatedSize;
      break;
    case 3:
      op->compaction_pri = kOldestLargestSeqFirst;
      break;
    case 4:
      op->compaction_pri = kOldestSmallestSeqFirst;
      break;
      //   case 5:
      //     op->compaction_pri = kFADE; break;
      //   // !YBS-sep06-XX!
      //   case 6:
      //     // !YBS-sep06-XX!
      //     op->compaction_pri = kRoundRobin; break;
      //   // !YBS-sep07-XX!
      //   case 7:
      //     // !YBS-sep07-XX!
      //     op->compaction_pri = kMinOverlappingGrandparent; break;
      //   // !YBS-sep08-XX!
      //   case 8:
      //     // !YBS-sep08-XX!
      //     op->compaction_pri = kFullLevel; break;
    default:
      std::cerr << "ERROR: INVALID Data movement policy!" << std::endl;
  }

  // (shubham) enforce strict size for SST files
  // op->level_compaction_dynamic_file_size = false;  // deprecated
  // op->ignore_max_compaction_bytes_for_input = false;  // deprecated

  op->max_bytes_for_level_multiplier = _env->size_ratio;
  op->allow_concurrent_memtable_write = _env->allow_concurrent_memtable_write;
  op->create_if_missing = _env->create_if_missing;
  op->target_file_size_base = _env->target_file_size_base;
  op->level_compaction_dynamic_level_bytes =
      _env->level_compaction_dynamic_level_bytes;
  switch (_env->compaction_style) {
    case 1:
      op->compaction_style = kCompactionStyleLevel;
      break;
    case 2:
      op->compaction_style = kCompactionStyleUniversal;
      break;
    case 3:
      op->compaction_style = kCompactionStyleFIFO;
      break;
    case 4:
      op->compaction_style = kCompactionStyleNone;
      break;
    default:
      std::cerr << "ERROR: INVALID Compaction eagerness!" << std::endl;
  }

  op->disable_auto_compactions = _env->disable_auto_compactions;
  if (_env->compaction_filter == 0) {
    ;  // do nothing
  } else {
    ;  // invoke manual compaction_filter
  }
  if (_env->compaction_filter_factory == 0) {
    ;  // do nothing
  } else {
    ;  // invoke manual compaction_filter_factory
  }
  switch (_env->access_hint_on_compaction_start) {
    case 1:
      op->access_hint_on_compaction_start = DBOptions::AccessHint::NONE;
      break;
    case 2:
      op->access_hint_on_compaction_start = DBOptions::AccessHint::NORMAL;
      break;
    case 3:
      op->access_hint_on_compaction_start = DBOptions::AccessHint::SEQUENTIAL;
      break;
    case 4:
      op->access_hint_on_compaction_start = DBOptions::AccessHint::WILLNEED;
      break;
    default:
      std::cerr << "error: access_hint_on_compaction_start" << std::endl;
  }

  if (op->compaction_style != kCompactionStyleUniversal)  // !YBS-sep07-XX!
    op->level0_file_num_compaction_trigger =
        _env->level0_file_num_compaction_trigger;  // !YBS-sep07-XX!
  op->target_file_size_multiplier = _env->target_file_size_multiplier;
  op->max_background_jobs = _env->max_background_jobs;
  op->max_compaction_bytes = _env->max_compaction_bytes;
  op->max_bytes_for_level_base = _env->buffer_size * _env->size_ratio;
  // std::cout << "printing: max_bytes_for_level_base = "
  //           << op->max_bytes_for_level_base
  //           << " buffer_size = " << _env->buffer_size
  //           << " size_ratio = " << _env->size_ratio << std::endl;
  if (_env->merge_operator == 0) {
    ;  // do nothing
  } else {
    ;  // use custom merge operator
  }
  op->soft_pending_compaction_bytes_limit =
      _env->soft_pending_compaction_bytes_limit;  // No pending compaction
                                                  // anytime, try and see
  op->hard_pending_compaction_bytes_limit =
      _env->hard_pending_compaction_bytes_limit;  // No pending compaction
                                                  // anytime, try and see
  op->periodic_compaction_seconds = _env->periodic_compaction_seconds;
  op->use_direct_io_for_flush_and_compaction =
      _env->use_direct_io_for_flush_and_compaction;
  if (op->compaction_style != kCompactionStyleUniversal)  // !YBS-sep07-XX!
    op->num_levels = _env->num_levels;                    // !YBS-sep07-XX!

  // Compression
  switch (_env->compression) {
    case 1:
      op->compression = kNoCompression;
      break;
    case 2:
      op->compression = kSnappyCompression;
      break;
    case 3:
      op->compression = kZlibCompression;
      break;
    case 4:
      op->compression = kBZip2Compression;
      break;
    case 5:
      op->compression = kLZ4Compression;
      break;
    case 6:
      op->compression = kLZ4HCCompression;
      break;
    case 7:
      op->compression = kXpressCompression;
      break;
    case 8:
      op->compression = kZSTD;
      break;
    case 9:
      op->compression = kZSTDNotFinalCompression;
      break;
    case 10:
      op->compression = kDisableCompressionOption;
      break;

    default:
      std::cerr << "error: compression" << std::endl;
  }

  // table_options.enable_index_compression = kNoCompression;

  // Other CFOptions
  switch (_env->comparator) {
    case 1:
      op->comparator = BytewiseComparator();
      break;
    case 2:
      op->comparator = ReverseBytewiseComparator();
      break;
    case 3:
      // use custom comparator
      break;
    default:
      std::cerr << "error: comparator" << std::endl;
  }

  op->max_sequential_skip_in_iterations =
      _env->max_sequential_skip_in_iterations;
  op->memtable_prefix_bloom_size_ratio =
      _env->memtable_prefix_bloom_size_ratio;  // disabled
  op->level0_stop_writes_trigger = _env->level0_stop_writes_trigger;
  op->paranoid_file_checks = _env->paranoid_file_checks;
  op->optimize_filters_for_hits = _env->optimize_filters_for_hits;
  op->inplace_update_support = _env->inplace_update_support;
  op->inplace_update_num_locks = _env->inplace_update_num_locks;
  op->report_bg_io_stats = _env->report_bg_io_stats;
  op->max_successive_merges =
      _env->max_successive_merges;  // read-modified-write related

  // Other DBOptions
  op->create_if_missing = _env->create_if_missing;
  op->delayed_write_rate = _env->delayed_write_rate;
  op->max_open_files = _env->max_open_files;
  op->max_file_opening_threads = _env->max_file_opening_threads;
  op->bytes_per_sync = _env->bytes_per_sync;
  op->stats_persist_period_sec = _env->stats_persist_period_sec;
  op->enable_thread_tracking = _env->enable_thread_tracking;
  op->stats_history_buffer_size = _env->stats_history_buffer_size;
  // op->allow_concurrent_memtable_write =
  // _env->allow_concurrent_memtable_write;
  op->dump_malloc_stats = _env->dump_malloc_stats;
  op->use_direct_reads = _env->use_direct_reads;
  op->avoid_flush_during_shutdown = _env->avoid_flush_during_shutdown;
  op->advise_random_on_open = _env->advise_random_on_open;
  op->delete_obsolete_files_period_micros =
      _env->delete_obsolete_files_period_micros;  // 6 hours
  op->allow_mmap_reads = _env->allow_mmap_reads;
  op->allow_mmap_writes = _env->allow_mmap_writes;

  // TableOptions
  //  !YBS-sep09-XX
  if (_env->block_cache == 0) {
    t_op->no_block_cache = true;
    t_op->cache_index_and_filter_blocks = false;
  }  // TBC
  else {
    t_op->no_block_cache = false;
    std::shared_ptr<Cache> cache =
        NewLRUCache(_env->block_cache * 1024 * 1024, -1, false,
                    _env->block_cache_high_priority_ratio);
    t_op->block_cache = cache;
    t_op->cache_index_and_filter_blocks = _env->cache_index_and_filter_blocks;
  }
  _env->no_block_cache = t_op->no_block_cache;
  // !END

  if (_env->bits_per_key == 0) {
    ;  // do nothing
  } else {
    t_op->filter_policy.reset(NewBloomFilterPolicy(
        _env->bits_per_key,
        false));  // currently build full filter instead of block-based filter
  }

  t_op->cache_index_and_filter_blocks_with_high_priority =
      _env->cache_index_and_filter_blocks_with_high_priority;  // Deprecated by
                                                               // no_block_cache
  t_op->read_amp_bytes_per_bit = _env->read_amp_bytes_per_bit;

  switch (_env->data_block_index_type) {
    case 1:
      t_op->data_block_index_type =
          BlockBasedTableOptions::kDataBlockBinarySearch;
      break;
    case 2:
      t_op->data_block_index_type =
          BlockBasedTableOptions::kDataBlockBinaryAndHash;
      break;
    default:
      std::cerr << "error: TableOptions::data_block_index_type" << std::endl;
  }
  switch (_env->index_type) {
    case 1:
      t_op->index_type = BlockBasedTableOptions::kBinarySearch;
      break;
    case 2:
      t_op->index_type = BlockBasedTableOptions::kHashSearch;
      break;
    case 3:
      t_op->index_type = BlockBasedTableOptions::kTwoLevelIndexSearch;
      break;
    case 4:
      t_op->index_type = BlockBasedTableOptions::kBinarySearchWithFirstKey;
      break;
    default:
      std::cerr << "error: TableOptions::index_type" << std::endl;
  }
  t_op->partition_filters = _env->partition_filters;
  t_op->block_size = _env->entries_per_page * _env->entry_size;
  t_op->metadata_block_size = _env->metadata_block_size;
  t_op->pin_top_level_index_and_filter = _env->pin_top_level_index_and_filter;

  switch (_env->index_shortening) {
    case 1:
      t_op->index_shortening =
          BlockBasedTableOptions::IndexShorteningMode::kNoShortening;
      break;
    case 2:
      t_op->index_shortening =
          BlockBasedTableOptions::IndexShorteningMode::kShortenSeparators;
      break;
    case 3:
      t_op->index_shortening = BlockBasedTableOptions::IndexShorteningMode::
          kShortenSeparatorsAndSuccessor;
      break;
    default:
      std::cerr << "error: TableOptions::index_shortening" << std::endl;
  }
  t_op->block_size_deviation = _env->block_size_deviation;
  t_op->enable_index_compression = _env->enable_index_compression;
  // Set all table options
  op->table_factory.reset(NewBlockBasedTableFactory(*t_op));

  // WriteOptions
  w_op->sync =
      _env->sync;  // make every write wait for sync with log (so we see real
                   // perf impact of insert) -- DOES NOT CAUSE SLOWDOWN
  w_op->low_pri = _env->low_pri;  // every insert is less important than
                                  // compaction -- CAUSES SLOWDOWN
  w_op->disableWAL = _env->disableWAL;
  w_op->no_slowdown =
      _env->no_slowdown;  // enabling this will make some insertions fail --
                          // DOES NOT CAUSE SLOWDOWN
  w_op->ignore_missing_column_families = _env->ignore_missing_column_families;

  // ReadOptions
  r_op->verify_checksums = _env->verify_checksums;
  r_op->fill_cache = _env->fill_cache;
  //   r_op->iter_start_seqnum = _env->iter_start_seqnum;
  r_op->ignore_range_deletions = _env->ignore_range_deletions;
  switch (_env->read_tier) {
    case 1:
      r_op->read_tier = kReadAllTier;
      break;
    case 2:
      r_op->read_tier = kBlockCacheTier;
      break;
    case 3:
      r_op->read_tier = kPersistedTier;
      break;
    case 4:
      r_op->read_tier = kMemtableTier;
      break;
    default:
      std::cerr << "error: ReadOptions::read_tier" << std::endl;
  }

  // FlushOptions
  f_op->wait = _env->wait;
  f_op->allow_write_stall = _env->allow_write_stall;

  // op->max_write_buffer_number_to_maintain = 0;    // immediately freed after
  // flushed op->db_write_buffer_size = 0;   // disable op->arena_block_size =
  // 0; op->memtable_huge_page_size = 0;

  // Compaction options
  // op->min_write_buffer_number_to_merge = 1;
  // op->compaction_readahead_size = 0;
  // op->max_bytes_for_level_multiplier_additional =
  // std::vector<int>(op->num_levels, 1); op->max_subcompactions = 1;   // no
  // subcomapctions op->avoid_flush_during_recovery = false; op->atomic_flush =
  // false; op->new_table_reader_for_compaction_inputs = false;   // forced to
  // true when using direct_IO_read compaction_options_fifo;

  // Compression options
  // // L0 - L6: noCompression
  // op->compression_per_level = {CompressionType::kNoCompression,
  //                                   CompressionType::kNoCompression,
  //                                   CompressionType::kNoCompression,
  //                                   CompressionType::kNoCompression,
  //                                   CompressionType::kNoCompression,
  //                                   CompressionType::kNoCompression,
  //                                   CompressionType::kNoCompression};

  // op->sample_for_compression = 0;    // disabled
  // op->bottommost_compression = kDisableCompressionOption;

  // Log Options
  // op->max_total_wal_size = 0;
  // op->db_log_dir = "";
  // op->max_log_file_size = 0;
  // op->wal_bytes_per_sync = 0;
  // op->strict_bytes_per_sync = false;
  // op->manual_wal_flush = false;
  // op->WAL_ttl_seconds = 0;
  // op->WAL_size_limit_MB = 0;
  // op->keep_log_file_num = 1000;
  // op->log_file_time_to_roll = 0;
  // op->recycle_log_file_num = 0;
  // op->info_log_level = nullptr;

  // Other CFOptions
  // op->prefix_extractor = nullptr;
  // op->bloom_locality = 0;
  // op->memtable_whole_key_filtering = false;
  // op->snap_refresh_nanos = 100 * 1000 * 1000;
  // op->memtable_insert_with_hint_prefix_extractor = nullptr;
  // op->force_consistency_checks = false;

  // Other DBOptions
  // op->stats_dump_period_sec = 600;   // 10min
  // op->persist_stats_to_disk = false;
  // op->enable_pipelined_write = false;
  // op->table_cache_numshardbits = 6;
  // op->fail_if_options_file_error = false;
  // op->writable_file_max_buffer_size = 1024 * 1024;
  // op->write_thread_slow_yield_usec = 100;
  // op->enable_write_thread_adaptive_yield = true;
  // op->unordered_write = false;
  // op->preserve_deletes = false;
  // op->paranoid_checks = true;
  // op->two_write_queues = false;
  // op->use_fsync = true;
  // op->random_access_max_buffer_size = 1024 * 1024;
  // op->skip_stats_update_on_db_open = false;
  // op->error_if_exists = false;
  // op->manifest_preallocation_size = 4 * 1024 * 1024;
  // op->max_manifest_file_size = 1024 * 1024 * 1024;
  // op->is_fd_close_on_exec = true;
  // op->use_adaptive_mutex = false;
  // op->create_missing_column_families = false;
  // op->allow_ingest_behind = false;
  // op->avoid_unnecessary_blocking_io = false;
  // op->allow_fallocate = true;
  // op->allow_2pc = false;
  // op->write_thread_max_yield_usec = 100;*/
}
