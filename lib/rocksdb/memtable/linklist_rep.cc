// linklist_rep.cc
// Unsorted Doubly Linked List MemTableRep implementation for RocksDB

#define PROFILE

#ifdef PROFILE
#include <chrono>
#include <iostream>
#endif  // PROFILE

#include <assert.h>
#include <stddef.h>

#include <atomic>

#include "db/memtable.h"
#include "memory/arena.h"
#include "port/port.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/utilities/options_type.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {

namespace {

class LinkListRep : public MemTableRep {
 public:
  explicit LinkListRep(const MemTableRep::KeyComparator& compare,
                       Allocator* allocator, const SliceTransform* transform,
                       Logger* logger);

  KeyHandle Allocate(const size_t len, char** buf) override;

  void Insert(KeyHandle handle) override;

  bool Contains(const char* key) const override;

  size_t ApproximateMemoryUsage() override;

  void Get(const LookupKey& k, void* callback_args,
           bool (*callback_func)(void* arg, const char* entry)) override;

  ~LinkListRep() override;

  MemTableRep::Iterator* GetIterator(Arena* arena = nullptr) override;

  MemTableRep::Iterator* GetDynamicPrefixIterator(
      Arena* arena = nullptr) override;

 private:
  friend class Iterator;

  struct Node {
    Node* next;
    Node* prev;
    char key_data[1];  // Flexible array member for key data

    // Returns the key stored in this node
    const char* Key() const { return key_data; }

    // Returns mutable key data pointer
    char* MutableKey() { return key_data; }
  };

  Node* head_;
  Node* tail_;

  Allocator* const allocator_;
  const MemTableRep::KeyComparator& compare_;
  const SliceTransform* const transform_;
  Logger* const logger_;

  mutable port::Mutex mutex_;  // Mutex for thread safety

  // Disable copying
  LinkListRep(const LinkListRep&) = delete;
  LinkListRep& operator=(const LinkListRep&) = delete;

  class Iterator : public MemTableRep::Iterator {
   public:
    explicit Iterator(const LinkListRep* rep, Arena* arena = nullptr);

    ~Iterator() override = default;

    bool Valid() const override;

    const char* key() const override;

    void Next() override;

    void Prev() override;

    void Seek(const Slice& internal_key, const char* memtable_key) override;

    void SeekForPrev(const Slice& internal_key,
                     const char* memtable_key) override;

    void SeekToFirst() override;

    void SeekToLast() override;

   private:
    const LinkListRep* rep_;
    Node* current_;
    IterKey iter_key_;
  };
};

LinkListRep::LinkListRep(const MemTableRep::KeyComparator& compare,
                         Allocator* allocator, const SliceTransform* transform,
                         Logger* logger)
    : MemTableRep(allocator),
      head_(nullptr),
      tail_(nullptr),
      allocator_(allocator),
      compare_(compare),
      transform_(transform),
      logger_(logger) {}

LinkListRep::~LinkListRep() = default;

KeyHandle LinkListRep::Allocate(const size_t len, char** buf) {
#ifdef PROFILE
  auto start_time = std::chrono::high_resolution_clock::now();
#endif  // PROFILE

  size_t total_size = sizeof(Node) + len - 1;  // -1 because key_data[1]
  char* mem = allocator_->AllocateAligned(total_size);
  Node* node = reinterpret_cast<Node*>(mem);
  node->next = nullptr;
  node->prev = nullptr;
  *buf = node->MutableKey();

#ifdef PROFILE
  auto end_time = std::chrono::high_resolution_clock::now();
  auto duration =
      std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time)
          .count();
  std::cout << "AllocateTime: " << duration << " ns" << std::endl;
#endif  // PROFILE

  return static_cast<void*>(node);
}

void LinkListRep::Insert(KeyHandle handle) {
#ifdef PROFILE
  auto start_time = std::chrono::high_resolution_clock::now();
#endif  // PROFILE

  Node* node = reinterpret_cast<Node*>(handle);

  MutexLock lock(&mutex_);

  // Insert at the tail for unsorted insertion
  if (tail_ == nullptr) {
    // List is empty
    head_ = node;
    tail_ = node;
  } else {
    tail_->next = node;
    node->prev = tail_;
    tail_ = node;
  }

#ifdef PROFILE
  auto end_time = std::chrono::high_resolution_clock::now();
  auto duration =
      std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time)
          .count();
  std::cout << "InsertTime: " << duration << " ns" << std::endl;
#endif  // PROFILE
}

bool LinkListRep::Contains(const char* key) const {
#ifdef PROFILE
  auto start_time = std::chrono::high_resolution_clock::now();
#endif  // PROFILE

  MutexLock lock(&mutex_);
  Node* current = head_;
  const Slice target_key = GetLengthPrefixedSlice(key);

  while (current != nullptr) {
    if (compare_(current->Key(), target_key) == 0) {
#ifdef PROFILE
      auto end_time = std::chrono::high_resolution_clock::now();
      auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(
                          end_time - start_time)
                          .count();
      std::cout << "ContainsTime (found): " << duration << " ns" << std::endl;
#endif  // PROFILE
      return true;
    }
    current = current->next;
  }

#ifdef PROFILE
  auto end_time = std::chrono::high_resolution_clock::now();
  auto duration =
      std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time)
          .count();
  std::cout << "ContainsTime (not found): " << duration << " ns" << std::endl;
#endif  // PROFILE

  return false;
}

size_t LinkListRep::ApproximateMemoryUsage() { return 0; }

void LinkListRep::Get(const LookupKey& k, void* callback_args,
                      bool (*callback_func)(void* arg, const char* entry)) {
#ifdef PROFILE
  auto start_time = std::chrono::high_resolution_clock::now();
#endif  // PROFILE

  MutexLock lock(&mutex_);
  Node* current = head_;
  const Slice target_key = k.internal_key();

  while (current != nullptr) {
    if (compare_(current->Key(), target_key) == 0) {
      if (!callback_func(callback_args, current->Key())) {
        break;  // Stop if callback returns false
      }
    }
    current = current->next;
  }

#ifdef PROFILE
  auto end_time = std::chrono::high_resolution_clock::now();
  auto duration =
      std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time)
          .count();
  std::cout << "GetTime: " << duration << " ns" << std::endl;
#endif  // PROFILE
}

MemTableRep::Iterator* LinkListRep::GetIterator(Arena* arena) {
  if (arena == nullptr) {
    return new Iterator(this);
  } else {
    auto mem = arena->AllocateAligned(sizeof(Iterator));
    return new (mem) Iterator(this);
  }
}

MemTableRep::Iterator* LinkListRep::GetDynamicPrefixIterator(Arena* arena) {
  return GetIterator(arena);
}

// Iterator implementation

LinkListRep::Iterator::Iterator(const LinkListRep* rep, Arena* /* arena */)
    : rep_(rep), current_(nullptr) {}

bool LinkListRep::Iterator::Valid() const { return current_ != nullptr; }

const char* LinkListRep::Iterator::key() const {
  assert(Valid());
  return current_->Key();
}

void LinkListRep::Iterator::Next() {
  assert(Valid());
  current_ = current_->next;
}

void LinkListRep::Iterator::Prev() {
  assert(Valid());
  current_ = current_->prev;
}

void LinkListRep::Iterator::Seek(const Slice& internal_key,
                                 const char* /* memtable_key */) {
  MutexLock lock(&rep_->mutex_);
  current_ = rep_->head_;

  while (current_ != nullptr) {
    if (rep_->compare_(current_->Key(), internal_key) >= 0) {
      break;
    }
    current_ = current_->next;
  }
}

void LinkListRep::Iterator::SeekForPrev(const Slice& internal_key,
                                        const char* /* memtable_key */) {
  MutexLock lock(&rep_->mutex_);
  current_ = rep_->tail_;

  while (current_ != nullptr) {
    if (rep_->compare_(current_->Key(), internal_key) <= 0) {
      break;
    }
    current_ = current_->prev;
  }
}

void LinkListRep::Iterator::SeekToFirst() {
  MutexLock lock(&rep_->mutex_);
  current_ = rep_->head_;
}

void LinkListRep::Iterator::SeekToLast() {
  MutexLock lock(&rep_->mutex_);
  current_ = rep_->tail_;
}

// Factory class to create instances of LinkListRep
class LinkListRepFactory : public MemTableRepFactory {
 public:
  explicit LinkListRepFactory() {}

  using MemTableRepFactory::CreateMemTableRep;

  MemTableRep* CreateMemTableRep(const MemTableRep::KeyComparator& compare,
                                 Allocator* allocator,
                                 const SliceTransform* transform,
                                 Logger* logger) override {
    return new LinkListRep(compare, allocator, transform, logger);
  }

  const char* Name() const override { return "LinkListRepFactory"; }
};

}  // namespace

MemTableRepFactory* NewLinkListRepFactory() {
  return new LinkListRepFactory();
}

}  // namespace ROCKSDB_NAMESPACE