// Unsorted Doubly Linked List MemTableRep implementation for RocksDB

#define PROFILE

#ifdef PROFILE
#include <chrono>
#include <iostream>
#endif  // PROFILE

#include <assert.h>
#include <stddef.h>

#include <vector>
#include <algorithm>

#include "db/memtable.h"
#include "memory/arena.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/utilities/options_type.h"
#include "util/coding.h"  // PutLengthPrefixedSlice

namespace ROCKSDB_NAMESPACE {

namespace {

//  each entry in the linked list
struct Node {
  Node* next;
  Node* prev;
  // Flexible array member for key data
  char key_data[1];  

  // Returns the key stored in this node
  const char* Key() const { return key_data; }

  // Returns mutable key data pointer
  char* MutableKey() { return key_data; }
};

// Inherit from MemTableRep
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

  // expose the Iterator class to public
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
    // Vector to hold pointers to all nodes
    std::vector<Node*> entries_;
    // Current index in the sorted entries vector
    size_t current_index_;

    // Comparator for sorting entries for range query 
    struct NodeComparator {
      const MemTableRep::KeyComparator& compare_;

      explicit NodeComparator(const MemTableRep::KeyComparator& cmp)
          : compare_(cmp) {}

      bool operator()(Node* a, Node* b) const {
        return compare_(a->Key(), b->Key()) < 0;
      }
    };
  };

 private:
  // Two pointers pointing to the start and end of the linked list
  Node* head_;
  Node* tail_;

  Allocator* const allocator_;
  const MemTableRep::KeyComparator& compare_;
  const SliceTransform* const transform_;
  Logger* const logger_;

 
  LinkListRep(const LinkListRep&) = delete;
  LinkListRep& operator=(const LinkListRep&) = delete;
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
  size_t total_size = sizeof(Node) + len - 1;  // -1 because the flexible array memeber for the key data (key_data[1])
  char* mem = allocator_->AllocateAligned(total_size);
  Node* node = reinterpret_cast<Node*>(mem);
  node->next = nullptr;
  node->prev = nullptr;
  *buf = node->MutableKey();
  return static_cast<void*>(node);
}

void LinkListRep::Insert(KeyHandle handle) {
#ifdef PROFILE
  auto start_time = std::chrono::high_resolution_clock::now();
#endif  // PROFILE

  Node* node = reinterpret_cast<Node*>(handle);

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
  std::cout << "InsertTime: "
            << std::chrono::duration_cast<std::chrono::nanoseconds>(
                   end_time - start_time)
                   .count()
            << std::endl
            << std::flush;
#endif  // PROFILE
}

bool LinkListRep::Contains(const char* key) const {
  Node* current = tail_;  // Start from tail to find the most recent entry

  while (current != nullptr) {
    if (compare_(current->Key(), key) == 0) {
      return true;
    }
    current = current->prev;
  }
  return false;
}

size_t LinkListRep::ApproximateMemoryUsage() { return 0; }

void LinkListRep::Get(const LookupKey& k, void* callback_args,
                      bool (*callback_func)(void* arg, const char* entry)) {
#ifdef PROFILE
  auto start_time = std::chrono::high_resolution_clock::now();
#endif  // PROFILE

  Node* current = tail_;  // Start from tail to get the most recent entry
  const char* target_key = k.memtable_key().data();

  while (current != nullptr) {
    if (compare_(current->Key(), target_key) == 0) {
      if (!callback_func(callback_args, current->Key())) {
        break;  
      }
      // the most recent entry should be the first one we found since shallower level always contain most recent valid entry. 
      break;
    }
    current = current->prev;
  }

#ifdef PROFILE
  auto end_time = std::chrono::high_resolution_clock::now();
  std::cout << "PointQueryTime: "
            << std::chrono::duration_cast<std::chrono::nanoseconds>(
                   end_time - start_time)
                   .count()
            << std::endl
            << std::flush;
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
    : rep_(rep), current_index_(0) {
  // Collect all nodes into entries_
  Node* node = rep_->head_;
  while (node != nullptr) {
    entries_.push_back(node);
    node = node->next;
  }

  // Sort the entries using the comparator
  NodeComparator cmp(rep_->compare_);
  std::sort(entries_.begin(), entries_.end(), cmp);

  // Initialize currentindex to invalid position to start
  current_index_ = entries_.size();  
}

bool LinkListRep::Iterator::Valid() const {
  return current_index_ < entries_.size();
}

const char* LinkListRep::Iterator::key() const {
  assert(Valid());
  return entries_[current_index_]->Key();
}

void LinkListRep::Iterator::Next() {
  assert(Valid());
  ++current_index_;
}

void LinkListRep::Iterator::Prev() {
  if (current_index_ == 0) {
    current_index_ = entries_.size();  // Invalid index
  } else {
    --current_index_;
  }
}

void LinkListRep::Iterator::Seek(const Slice& target_key,
                                 const char* memtable_key) {
  const char* target_key_data = memtable_key;
  std::string encoded_key;
  if (target_key_data == nullptr) {
    // Encode the target_key to get a length-prefixed key
    PutLengthPrefixedSlice(&encoded_key, target_key);
    target_key_data = encoded_key.data();
  }

  NodeComparator cmp(rep_->compare_);
  auto it = std::lower_bound(
      entries_.begin(), entries_.end(), target_key_data,
      [&](Node* node, const char* key_data) {
        return rep_->compare_(node->Key(), key_data) < 0;
      });
  current_index_ = it - entries_.begin();
}

void LinkListRep::Iterator::SeekForPrev(const Slice& target_key,
                                        const char* memtable_key) {
  const char* target_key_data = memtable_key;
  std::string encoded_key;
  if (target_key_data == nullptr) {
    // encode the target key to get a length-prefixed key
    PutLengthPrefixedSlice(&encoded_key, target_key);
    target_key_data = encoded_key.data();
  }

  NodeComparator cmp(rep_->compare_);
  auto it = std::upper_bound(
      entries_.begin(), entries_.end(), target_key_data,
      [&](const char* key_data, Node* node) {
        return rep_->compare_(key_data, node->Key()) < 0;
      });
  if (it == entries_.begin()) {
    current_index_ = entries_.size();  // Invalid index
  } else {
    current_index_ = (it - entries_.begin()) - 1;
  }
}

void LinkListRep::Iterator::SeekToFirst() {
  current_index_ = 0;
}

void LinkListRep::Iterator::SeekToLast() {
  if (entries_.empty()) {
    current_index_ = entries_.size();  // Invalid index
  } else {
    current_index_ = entries_.size() - 1;
  }
}

// Factor LinkListRep
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