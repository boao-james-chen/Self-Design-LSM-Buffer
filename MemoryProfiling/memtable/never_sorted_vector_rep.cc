#include <db/memtable.h>
#include <rocksdb/utilities/options_type.h>

#define PROFILE

#ifdef PROFILE
#include <chrono>
#include <iostream>
#endif // PROFILE

namespace ROCKSDB_NAMESPACE {
namespace {
class NeverSortedVectorRep : public MemTableRep {
 public:
  NeverSortedVectorRep(const KeyComparator& compare, Allocator* allocator,
                       size_t count);

  void Insert(KeyHandle handle) override;

  bool Contains(const char* key) const override;

  void MarkReadOnly() override;

  size_t ApproximateMemoryUsage() override;

  void Get(const LookupKey& k, void* callback_args,
           bool (*callback_func)(void* arg, const char* entry)) override;

  ~NeverSortedVectorRep() override = default;

  class Iterator : public MemTableRep::Iterator {
    NeverSortedVectorRep* vrep_;
    std::shared_ptr<std::vector<const char*>> bucket_;
    std::vector<const char*>::const_iterator mutable cit_;
    const KeyComparator& compare_;
    std::string tmp_;

   public:
    explicit Iterator(NeverSortedVectorRep* vrep,
                      const std::shared_ptr<std::vector<const char*>>& bucket,
                      const KeyComparator& compare);

    ~Iterator() override = default;

    bool Valid() const override;

    const char* key() const override;

    void Next() override;

    void Prev() override;

    void Seek(const Slice& user_key, const char* memtable_key) override;

    void SeekForPrev(const Slice& internal_key,
                     const char* memtable_key) override;

    void SeekToFirst() override;

    void SeekToLast() override;
  };

  MemTableRep::Iterator* GetIterator(Arena* arena) override;

 private:
  // friend class Iterator;
  using Bucket = std::vector<const char*>;
  std::shared_ptr<Bucket> bucket_;
  mutable port::RWMutex rwlock_;
  bool immutable_;
  bool sorted_;
  const KeyComparator& compare_;
};

void NeverSortedVectorRep::Insert(KeyHandle handle) {
#ifdef PROFILE
  auto start = std::chrono::high_resolution_clock::now();
#endif  // PROFILE
  const auto* key = static_cast<char*>(handle);
  WriteLock l(&rwlock_);
  assert(!immutable_);
  bucket_->push_back(key);
#ifdef PROFILE
  auto end = std::chrono::high_resolution_clock::now();
  std::cout << "InsertTime: "
            << std::chrono::duration_cast<std::chrono::nanoseconds>(end -
                                                                     start)
                   .count()
            << std::endl
            << std::flush;
#endif  // PROFILE
}

bool NeverSortedVectorRep::Contains(const char* key) const {
  ReadLock l(&rwlock_);
  return std::find(bucket_->begin(), bucket_->end(), key) != bucket_->end();
}

void NeverSortedVectorRep::MarkReadOnly() {
  WriteLock l(&rwlock_);
  immutable_ = true;
}

size_t NeverSortedVectorRep::ApproximateMemoryUsage() {
  return sizeof(bucket_) + sizeof(*bucket_) +
         bucket_->size() *
             sizeof(std::remove_reference_t<decltype(*bucket_)>::value_type);
}

NeverSortedVectorRep::NeverSortedVectorRep(const KeyComparator& compare,
                                           Allocator* allocator, size_t count)
    : MemTableRep(allocator),
      bucket_(new Bucket()),
      immutable_(false),
      sorted_(false),
      compare_(compare) {
  bucket_->reserve(count);
}

NeverSortedVectorRep::Iterator::Iterator(
    NeverSortedVectorRep* vrep,
    const std::shared_ptr<std::vector<const char*>>& bucket,
    const KeyComparator& compare)
    : vrep_(vrep), bucket_(bucket), cit_(bucket->end()), compare_(compare) {}

bool NeverSortedVectorRep::Iterator::Valid() const {
  return cit_ != bucket_->end();
}

const char* NeverSortedVectorRep::Iterator::key() const { return *cit_; }

void NeverSortedVectorRep::Iterator::Next() {
  if (cit_ == bucket_->end()) {
    return;
  }
  ++cit_;
}

void NeverSortedVectorRep::Iterator::Prev() {
  if (cit_ == bucket_->begin()) {
    cit_ = bucket_->end();
  } else {
    --cit_;
  }
}

void NeverSortedVectorRep::Iterator::Seek(const Slice& user_key,
                                          const char* memtable_key) {
  const char* encoded_key =
      memtable_key != nullptr ? memtable_key : EncodeKey(&tmp_, user_key);

  cit_ = std::find_if(bucket_->begin(), bucket_->end(),
                      [this, &encoded_key](const char* a) {
                        bool ret = compare_(a, encoded_key) >= 0;
                        // LOG(ret);
                        return ret;
                      });
  // cit_ = std::find_if(bucket_->rbegin(), bucket_->rend(),
  //                     [this, &encoded_key](const char* a) {
  //                       bool ret = compare_(a, encoded_key) > 0;
  //                       LOG(ret);
  //                       return ret;
  //                     }).base();
}

void NeverSortedVectorRep::Iterator::SeekForPrev(const Slice& /* user_key */,
                                                 const char* /* memtable_key */) {
  assert(false);
}

void NeverSortedVectorRep::Iterator::SeekToFirst() { cit_ = bucket_->begin(); }

void NeverSortedVectorRep::Iterator::SeekToLast() {
  cit_ = bucket_->end();
  if (!bucket_->empty()) {
    --cit_;
  }
}

void NeverSortedVectorRep::Get(const LookupKey& k, void* callback_args,
                               bool (*callback_func)(void* args,
                                                     const char* entry)) {
#ifdef PROFILE
  auto start = std::chrono::high_resolution_clock::now();
#endif  // PROFILE
  rwlock_.ReadLock();
  NeverSortedVectorRep* vector_rep;
  std::shared_ptr<Bucket> bucket;
  if (immutable_) {
    if (!sorted_) {
      std::sort(this->bucket_->begin(), this->bucket_->end());
      sorted_ = true;
    }
    vector_rep = this;
  } else {
    vector_rep = nullptr;
    bucket.reset(new Bucket(*bucket_));
  }

  Iterator iter(vector_rep, immutable_ ? bucket_ : bucket, compare_);
  rwlock_.ReadUnlock();

  for (iter.Seek(k.user_key(), k.memtable_key().data());
       iter.Valid() && callback_func(callback_args, iter.key()); iter.Next()) {
  }

#ifdef PROFILE
  auto end = std::chrono::high_resolution_clock::now();
  std::cout << "PointQueryTime: "
            << std::chrono::duration_cast<std::chrono::nanoseconds>(end -
                                                                     start)
                   .count()
            << std::endl
            << std::flush;
#endif  // PROFILE
}

MemTableRep::Iterator* NeverSortedVectorRep::GetIterator(Arena* arena) {
  char* mem = nullptr;
  if (arena != nullptr) {
    mem = arena->AllocateAligned(sizeof(Iterator));
  }
  ReadLock l(&rwlock_);

  if (immutable_) {
    if (arena == nullptr) {
      return new Iterator(this, bucket_, compare_);
    } else {
      return new (mem) Iterator(this, bucket_, compare_);
    }
  } else {
    std::shared_ptr<Bucket> tmp;
    tmp.reset(new Bucket(*bucket_));
    if (arena == nullptr) {
      return new Iterator(nullptr, tmp, compare_);
    } else {
      return new (mem) Iterator(nullptr, tmp, compare_);
    }
  }
}

}  // namespace

static std::unordered_map<std::string, OptionTypeInfo>
    never_sorted_vector_rep_table_info = {
        {"count",
         {0, OptionType::kSizeT, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
};

NeverSortedVectorRepFactory::NeverSortedVectorRepFactory(size_t count)
    : count_(count) {
  RegisterOptions("NeverSortedVectorRepFactoryOptions", &count_,
                  &never_sorted_vector_rep_table_info);
}

MemTableRep* NeverSortedVectorRepFactory::CreateMemTableRep(
    const MemTableRep::KeyComparator& compare, Allocator* allocator,
    const SliceTransform*, Logger* /* logger */) {
  return new NeverSortedVectorRep(compare, allocator, count_);
}

}  // namespace ROCKSDB_NAMESPACE
