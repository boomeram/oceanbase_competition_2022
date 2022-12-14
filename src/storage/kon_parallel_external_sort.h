#pragma once
#include "lib/compress/ob_compressor_pool.h"
#include "lib/queue/ob_lighty_queue.h"
#include "src/sql/engine/cmd/kon_load_data_row.h"
#include "storage/kon_thread_pool.h"
#include "storage/ob_parallel_external_sort.h"
#include <atomic>
#include <libaio.h>
#include <malloc.h>
#include <thread>

namespace oceanbase {
namespace storage {

constexpr ObCompressorType KonCompressType = ZSTD_COMPRESSOR;
constexpr char KonTmpFileFolder[] = "/data/";

template <typename T> class KonMacroBufferReader {
public:
  KonMacroBufferReader();
  ~KonMacroBufferReader();
  int init(const common::ObArray<const share::schema::ObColumnSchemaV2 *>
               &column_schemas) {
    column_schemas_ = column_schemas;
    return OB_SUCCESS;
  }
  OB_INLINE int read_item(T &item);
  OB_INLINE int deserialize_header();
  void assign(const int64_t buf_pos, const int64_t buf_cap, const char *buf);
  TO_STRING_KV(KP(buf_), K(buf_pos_), K(buf_len_), K(buf_cap_));

private:
  common::ObArray<const share::schema::ObColumnSchemaV2 *> column_schemas_;
  const char *buf_;
  int64_t buf_pos_;
  int64_t buf_len_;
  int64_t buf_cap_;
};

template <typename T>
KonMacroBufferReader<T>::KonMacroBufferReader()
    : buf_(NULL), buf_pos_(-1), buf_len_(-1), buf_cap_(0) {}

template <typename T> KonMacroBufferReader<T>::~KonMacroBufferReader() {}

template <typename T> int KonMacroBufferReader<T>::read_item(T &item) {
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(0 == buf_len_)) {
    if (OB_FAIL(deserialize_header())) {
      KON_LOG(WARN, "fail to deserialize header");
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(buf_pos_ == buf_len_)) {
      ret = common::OB_EAGAIN;
    } else if (OB_FAIL(item.deserialize(column_schemas_, buf_, buf_len_,
                                        buf_pos_))) {
      KON_LOG(WARN, "fail to deserialize buffer", K(ret), K(buf_len_),
              K(buf_pos_));
    }
  }
  return ret;
}

template <typename T> int KonMacroBufferReader<T>::deserialize_header() {
  int ret = common::OB_SUCCESS;
  const int64_t header_size = ObExternalSortConstant::BUF_HEADER_LENGTH;
  if (OB_FAIL(common::serialization::decode_i64(buf_, header_size, buf_pos_,
                                                &buf_len_))) {
    KON_LOG(WARN, "fail to encode macro block buffer header", K(ret),
            K(buf_pos_), K(header_size), K(buf_len_));
  }
  return ret;
}

template <typename T>
void KonMacroBufferReader<T>::assign(const int64_t buf_pos,
                                     const int64_t buf_cap, const char *buf) {
  buf_pos_ = buf_pos;
  buf_cap_ = buf_cap;
  buf_len_ = 0;
  buf_ = buf;
}

template <typename T> class KonMacroBufferDesc {
public:
  T *sample_item_;
  int64_t offset_;
  int64_t len_;
  TO_STRING_KV(K_(offset), K_(len));
};

template <typename T> class KonMemorySortFileDesc {
public:
  typedef KonMacroBufferDesc<T> MacroBufferDesc;

public:
  int fd_;
  char *buf_;
  int64_t buf_len_;
  common::ObVector<MacroBufferDesc> descs_;
};

template <typename T> class KonFragmentWriter {
  typedef KonMacroBufferDesc<T> MacroBufferDesc;

public:
  KonFragmentWriter();
  ~KonFragmentWriter();
  int open(const uint64_t tenant_id, const int64_t macro_buf_size,
           char *write_buf, const int64_t write_buf_size);
  int write_item(const T &item);
  int write_item_end();
  void reset();
  int64_t get_offset() const { return offset_; }
  const common::ObVector<MacroBufferDesc> &get_buf_desc() {
    return macro_buf_descs_;
  }

private:
  int flush_buffer();

private:
  char *buf_;
  int64_t buf_size_;
  char *write_buf_;
  int64_t write_buf_size_;
  ObCompressor *compressor_;
  common::ObArenaAllocator allocator_;
  common::ObArenaAllocator sample_allocator_;
  ObMacroBufferWriter<T> macro_buffer_writer_;
  int64_t offset_;
  bool new_macro_buffer_;
  common::ObVector<MacroBufferDesc> macro_buf_descs_;
  DISALLOW_COPY_AND_ASSIGN(KonFragmentWriter);
};

template <typename T>
KonFragmentWriter<T>::KonFragmentWriter()
    : buf_(NULL), buf_size_(0), write_buf_(NULL), write_buf_size_(0),
      compressor_(NULL),
      allocator_(common::ObNewModIds::OB_ASYNC_EXTERNAL_SORTER,
                 common::OB_MALLOC_BIG_BLOCK_SIZE),
      sample_allocator_(common::ObNewModIds::OB_ASYNC_EXTERNAL_SORTER,
                        common::OB_MALLOC_BIG_BLOCK_SIZE),
      macro_buffer_writer_(), offset_(0), new_macro_buffer_(true),
      macro_buf_descs_() {}

template <typename T> KonFragmentWriter<T>::~KonFragmentWriter() { reset(); }

template <typename T>
int KonFragmentWriter<T>::open(const uint64_t tenant_id,
                               const int64_t macro_buf_size, char *write_buf,
                               const int64_t write_buf_size) {
  int ret = common::OB_SUCCESS;
  allocator_.set_tenant_id(tenant_id);
  sample_allocator_.set_tenant_id(tenant_id);
  if (OB_ISNULL(buf_ = static_cast<char *>(allocator_.alloc(macro_buf_size)))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    KON_LOG(WARN, "fail to allocate buffer", K(ret), K(macro_buf_size));
  } else if (OB_FAIL(ObCompressorPool::get_instance().get_compressor(
                 KonCompressType, compressor_))) {
    KON_LOG(WARN, "fail to get compressor");
  } else {
    buf_size_ = macro_buf_size;
    write_buf_ = write_buf;
    write_buf_size_ = write_buf_size;
    offset_ = 0;
    macro_buffer_writer_.assign(ObExternalSortConstant::BUF_HEADER_LENGTH,
                                buf_size_, buf_);
  }
  return ret;
}

template <typename T> int KonFragmentWriter<T>::write_item(const T &item) {
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(macro_buffer_writer_.write_item(item))) {
    if (common::OB_EAGAIN == ret) {
      if (OB_FAIL(flush_buffer())) {
        KON_LOG(WARN, "switch next macro buffer failed", K(ret));
      } else if (OB_FAIL(macro_buffer_writer_.write_item(item))) {
        KON_LOG(WARN, "fail to write item", K(ret));
      }
    } else {
      KON_LOG(WARN, "fail to write item", K(ret));
    }
  }
  if (OB_UNLIKELY(new_macro_buffer_)) {
    char *buf = NULL;
    int64_t buf_len = sizeof(T) + item.get_deep_copy_critical_size();
    int64_t pos = sizeof(T);
    T *sample_item = NULL;
    if (OB_ISNULL(buf =
                      static_cast<char *>(sample_allocator_.alloc(buf_len)))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      KON_LOG(WARN, "failed to alloc buf", K(ret));
    } else if (OB_ISNULL(sample_item = new (buf) T())) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      KON_LOG(WARN, "failed to new sample item", K(ret));
    } else if (sample_item->deep_copy_critical(item, buf, buf_len, pos)) {
      KON_LOG(WARN, "failed to deep copy item", K(ret));
    } else {
      new_macro_buffer_ = false;
      macro_buf_descs_.push_back(
          MacroBufferDesc{.sample_item_ = sample_item, .offset_ = offset_});
    }
  }
  return ret;
}

template <typename T> int KonFragmentWriter<T>::flush_buffer() {
  int ret = common::OB_SUCCESS;
  int64_t compress_len = 0;
  int64_t write_len = 0;
  if (OB_FAIL(macro_buffer_writer_.serialize_header())) {
    KON_LOG(WARN, "fail to serialize header", K(ret));
  } else if (OB_FAIL(compressor_->compress(
                 buf_, buf_size_, write_buf_ + offset_,
                 write_buf_size_ - offset_, compress_len))) {
    KON_LOG(WARN, "fail to compress buffer", KR(ret));
  } else {
    write_len = upper_align(compress_len, DIO_ALIGN_SIZE);
    offset_ += write_len;
    macro_buf_descs_.last()->len_ = compress_len;
    new_macro_buffer_ = true;
    macro_buffer_writer_.assign(ObExternalSortConstant::BUF_HEADER_LENGTH,
                                buf_size_, buf_);
  }
  return ret;
}

template <typename T> int KonFragmentWriter<T>::write_item_end() {
  int ret = common::OB_SUCCESS;
  if (macro_buffer_writer_.has_item() && OB_FAIL(flush_buffer())) {
    KON_LOG(WARN, "fail to flush buffer", K(ret));
  }
  return ret;
}

template <typename T> void KonFragmentWriter<T>::reset() {
  buf_ = NULL;
  buf_size_ = 0;
  write_buf_ = NULL;
  write_buf_size_ = 0;
  compressor_ = NULL;
  allocator_.reuse();
  macro_buffer_writer_.assign(0, 0, NULL);
  offset_ = 0;
  new_macro_buffer_ = true;
  macro_buf_descs_.reset();
}

template <typename T, typename CompactT, typename Compare>
class KonFragmentReader {
public:
  typedef KonMacroBufferDesc<CompactT> MacroBufferDesc;

public:
  KonFragmentReader();
  ~KonFragmentReader();
  int init(const int fd, const char *data_buf, const uint64_t tenant_id,
           const int64_t buf_size, const CompactT *begin_item,
           const CompactT *end_item, Compare *compare, int macro_buf_idx,
           const common::ObVector<MacroBufferDesc> &macro_buf_descs,
           const common::ObArray<const share::schema::ObColumnSchemaV2 *>
               &column_schemas);
  int open() { return OB_SUCCESS; }
  int get_next_item(const T *&item);
  int clean_up() {
    reset();
    return OB_SUCCESS;
  }
  int prefetch();
  TO_STRING_KV(K(""));

private:
  int wait();
  int read_next_buffer();
  OB_INLINE int get_next_buffer_item(const T *&item);
  void reset();

private:
  common::ObArenaAllocator allocator_;
  KonMacroBufferReader<T> macro_buffer_reader_;
  int fd_;
  const char *data_buf_;
  char *buf_;
  int64_t buf_size_;
  int curr_index_;
  char *decompress_buf_[2];
  io_context_t ctx_;
  T curr_item_;
  ObCompressor *compressor_;
  bool valid_item_;
  const CompactT *begin_item_;
  const CompactT *end_item_;
  Compare *compare_;
  int macro_buf_idx_;
  common::ObVector<MacroBufferDesc> macro_buf_descs_;
};
template <typename T, typename CompactT, typename Compare>
KonFragmentReader<T, CompactT, Compare>::KonFragmentReader()
    : allocator_(common::ObNewModIds::OB_ASYNC_EXTERNAL_SORTER,
                 common::OB_MALLOC_BIG_BLOCK_SIZE),
      macro_buffer_reader_(), fd_(-1), data_buf_(NULL), buf_(NULL),
      buf_size_(0), curr_index_(0), decompress_buf_(), ctx_(), curr_item_(),
      compressor_(NULL), valid_item_(false), begin_item_(NULL), end_item_(NULL),
      compare_(NULL), macro_buf_idx_(0), macro_buf_descs_() {}

template <typename T, typename CompactT, typename Compare>
KonFragmentReader<T, CompactT, Compare>::~KonFragmentReader() {
  reset();
}

template <typename T, typename CompactT, typename Compare>
int KonFragmentReader<T, CompactT, Compare>::init(
    const int fd, const char *data_buf, const uint64_t tenant_id,
    const int64_t buf_size, const CompactT *begin_item,
    const CompactT *end_item, Compare *compare, int macro_buf_idx,
    const common::ObVector<MacroBufferDesc> &macro_buf_descs,
    const common::ObArray<const share::schema::ObColumnSchemaV2 *>
        &column_schemas) {
  int ret = OB_SUCCESS;
  int64_t compress_overflow_size = 0;
  int64_t decompress_buf_len = 0;
  memset(&ctx_, 0, sizeof(ctx_));
  allocator_.set_tenant_id(tenant_id);
  if ((fd < 0 && data_buf == NULL) || OB_INVALID_ID == tenant_id ||
      buf_size % DIO_ALIGN_SIZE != 0 || buf_size < 0 ||
      (begin_item == NULL && end_item == NULL) || compare == NULL) {
    ret = OB_INVALID_ARGUMENT;
    KON_LOG(WARN, "invalid argument", KR(ret), K(fd), K(data_buf), K(tenant_id),
            K(buf_size), K(begin_item), K(end_item), K(compare));
  } else if (OB_FAIL(macro_buffer_reader_.init(column_schemas))) {
    KON_LOG(WARN, "fail to init macro buffer reader");
  } else if (OB_ISNULL(buf_ =
                           static_cast<char *>(allocator_.alloc(buf_size)))) {
    KON_LOG(WARN, "fail to allocate buf");
  } else if (OB_FAIL(ObCompressorPool::get_instance().get_compressor(
                 KonCompressType, compressor_))) {
    KON_LOG(WARN, "fail to get compressor");
  } else if (OB_FAIL(compressor_->get_max_overflow_size(
                 buf_size, compress_overflow_size))) {
    KON_LOG(WARN, "fail to get compress overflow size");
  } else {
    decompress_buf_len =
        upper_align(buf_size + compress_overflow_size, DIO_ALIGN_SIZE);
    if (fd >= 0) {
      char *decompress_buf;
      if (OB_ISNULL(decompress_buf = static_cast<char *>(
                        ::memalign(DIO_ALIGN_SIZE, 2 * decompress_buf_len)))) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        KON_LOG(WARN, "fail to allocate compress buffer", K(ret));
      } else if (OB_UNLIKELY(io_setup(1, &ctx_) == -1)) {
        ret = OB_IO_ERROR;
        KON_LOG(WARN, "fail to io setup");
      } else {
        curr_index_ = 0;
        decompress_buf_[0] = decompress_buf;
        decompress_buf_[1] = decompress_buf + decompress_buf_len;
      }
    } else {
      decompress_buf_[0] = decompress_buf_[1] = NULL;
    }
  }

  if (OB_SUCC(ret)) {
    fd_ = fd;
    data_buf_ = data_buf;
    buf_size_ = buf_size;
    valid_item_ = (begin_item == NULL);
    begin_item_ = begin_item;
    end_item_ = end_item;
    compare_ = compare;
    macro_buf_idx_ = macro_buf_idx;
    macro_buf_descs_ = macro_buf_descs;
  }
  return ret;
}

template <typename T, typename CompactT, typename Compare>
int KonFragmentReader<T, CompactT, Compare>::prefetch() {
  int ret = OB_SUCCESS;
  if (fd_ >= 0) {
    struct iocb iocb;
    struct iocb *iocbs[1];

    int64_t offset = macro_buf_descs_[macro_buf_idx_].offset_;
    int64_t len = macro_buf_descs_[macro_buf_idx_].len_;
    int64_t read_len = upper_align(len, DIO_ALIGN_SIZE);
    io_prep_pread(&iocb, fd_, decompress_buf_[curr_index_], read_len, offset);
    iocbs[0] = &iocb;
    if (OB_UNLIKELY(io_submit(ctx_, 1, iocbs) != 1)) {
      ret = OB_IO_ERROR;
      KON_LOG(WARN, "fail to io submit", K(decompress_buf_[curr_index_]),
              K(read_len), K(offset));
    }
  }
  return ret;
}

template <typename T, typename CompactT, typename Compare>
int KonFragmentReader<T, CompactT, Compare>::wait() {
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(macro_buf_idx_ >= macro_buf_descs_.size())) {
    return OB_ITER_END;
  }
  if (fd_ >= 0) {
    struct io_event events[1];
    int64_t len = macro_buf_descs_[macro_buf_idx_].len_;
    int64_t decompress_len = 0;
    if (OB_UNLIKELY(io_getevents(ctx_, 1, 1, events, NULL) != 1)) {
      ret = OB_IO_ERROR;
      KON_LOG(WARN, "fail to io_getevents");
    } else if (OB_UNLIKELY(events[0].res < 0)) {
      ret = OB_IO_ERROR;
      KON_LOG(WARN, "fail to aio pread");
    } else if (OB_FAIL(compressor_->decompress(decompress_buf_[curr_index_],
                                               len, buf_, buf_size_,
                                               decompress_len))) {
      KON_LOG(WARN, "fail to decompress macro buffer");
    } else {
      macro_buffer_reader_.assign(0, buf_size_, buf_);
      macro_buf_idx_++;
      curr_index_ = (curr_index_ + 1) % 2;
    }
  } else {
    int64_t offset = macro_buf_descs_[macro_buf_idx_].offset_;
    int64_t len = macro_buf_descs_[macro_buf_idx_].len_;
    int64_t read_len = upper_align(len, DIO_ALIGN_SIZE);
    int64_t decompress_len = 0;
    if (OB_FAIL(compressor_->decompress(data_buf_ + offset, len, buf_,
                                        buf_size_, decompress_len))) {
      KON_LOG(WARN, "fail to decompress macro buffer");
    }
    macro_buffer_reader_.assign(0, buf_size_, buf_);
    macro_buf_idx_++;
  }
  return ret;
}

template <typename T, typename CompactT, typename Compare>
int KonFragmentReader<T, CompactT, Compare>::read_next_buffer() {
  int ret = OB_SUCCESS;
  if (OB_FAIL(wait())) {
    if (OB_ITER_END != ret) {
      KON_LOG(WARN, "fail to wait", KR(ret));
    }
  } else if (OB_LIKELY(macro_buf_idx_ < macro_buf_descs_.size()) &&
             OB_FAIL(prefetch())) {
    KON_LOG(WARN, "fail to prefetch", KR(ret));
  }
  return ret;
}

template <typename T, typename CompactT, typename Compare>
int KonFragmentReader<T, CompactT, Compare>::get_next_buffer_item(
    const T *&item) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(macro_buffer_reader_.read_item(curr_item_))) {
    if (OB_EAGAIN == ret) {
      if (OB_FAIL(read_next_buffer())) {
        if (OB_ITER_END != ret) {
          KON_LOG(WARN, "fail to switch next buffer", KR(ret));
        }
      } else if (OB_FAIL(macro_buffer_reader_.read_item(curr_item_))) {
        KON_LOG(WARN, "fail to read item", KR(ret));
      }
    }
  }
  item = &curr_item_;
  return ret;
}

template <typename T, typename CompactT, typename Compare>
int KonFragmentReader<T, CompactT, Compare>::get_next_item(const T *&item) {
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!valid_item_)) {
    do {
      if (OB_FAIL(get_next_buffer_item(item))) {
        if (OB_ITER_END != ret) {
          KON_LOG(WARN, "fail to get next item from reader");
        }
        return ret;
      }
      valid_item_ = !compare_->operator()(item, begin_item_);
    } while (!valid_item_);
  } else if (OB_FAIL(get_next_buffer_item(item))) {
    if (OB_ITER_END != ret) {
      KON_LOG(WARN, "fail to get next item from reader");
    }
    return ret;
  }

  if (end_item_ != NULL &&
      OB_UNLIKELY(!compare_->operator()(item, end_item_))) {
    ret = OB_ITER_END;
  }

  return ret;
}

template <typename T, typename CompactT, typename Compare>
void KonFragmentReader<T, CompactT, Compare>::reset() {
  allocator_.reset();
  macro_buffer_reader_.assign(0, 0, NULL);
  fd_ = -1;
  data_buf_ = NULL;
  buf_ = NULL;
  buf_size_ = 0;
  if (decompress_buf_[0]) {
    ::free(decompress_buf_[0]);
  }
  decompress_buf_[0] = NULL;
  decompress_buf_[1] = NULL;
  io_destroy(ctx_);
  compressor_ = NULL;
  valid_item_ = false;
  begin_item_ = NULL;
  end_item_ = NULL;
  compare_ = NULL;
  macro_buf_idx_ = 0;
  macro_buf_descs_.reset();
}

template <typename T, typename CompactT, typename Compare>
class KonFragmentMerge {
public:
  typedef KonFragmentReader<T, CompactT, Compare> FragmentIterator;
  static const int64_t DEFAULT_ITERATOR_NUM = 128;
  KonFragmentMerge();
  ~KonFragmentMerge();
  int init(const common::ObIArray<FragmentIterator *> &readers,
           Compare *compare);
  int open();
  int get_next_item(const T *&item);
  void reset();
  TO_STRING_KV(K(""));

private:
  int heap_get_next_item(const T *&item);
  int build_heap();

private:
  struct HeapItem {
    const T *item_;
    int64_t idx_;
    HeapItem() : item_(NULL), idx_(0) {}
    void reset() {
      item_ = NULL;
      idx_ = 0;
    }
    TO_STRING_KV(K_(item), K_(idx));
  };
  class HeapCompare {
  public:
    HeapCompare();
    ~HeapCompare();
    bool operator()(const HeapItem &left_item,
                    const HeapItem &right_item) const;
    void set_compare(Compare *compare) { compare_ = compare; }
    int get_error_code() { return OB_SUCCESS; }

  private:
    Compare *compare_;
  };

private:
  HeapCompare compare_;
  common::ObSEArray<FragmentIterator *, DEFAULT_ITERATOR_NUM> iters_;
  int64_t last_iter_idx_;
  common::ObBinaryHeap<HeapItem, HeapCompare, DEFAULT_ITERATOR_NUM> heap_;
};

template <typename T, typename CompactT, typename Compare>
KonFragmentMerge<T, CompactT, Compare>::HeapCompare::HeapCompare()
    : compare_(NULL) {}

template <typename T, typename CompactT, typename Compare>
KonFragmentMerge<T, CompactT, Compare>::HeapCompare::~HeapCompare() {}

template <typename T, typename CompactT, typename Compare>
void KonFragmentMerge<T, CompactT, Compare>::reset() {
  iters_.reset();
  last_iter_idx_ = -1;
  heap_.reset();
}

template <typename T, typename CompactT, typename Compare>
bool KonFragmentMerge<T, CompactT, Compare>::HeapCompare::operator()(
    const HeapItem &left_item, const HeapItem &right_item) const {
  return !compare_->operator()(left_item.item_, right_item.item_);
}

template <typename T, typename CompactT, typename Compare>
KonFragmentMerge<T, CompactT, Compare>::KonFragmentMerge()
    : iters_(), last_iter_idx_(-1), heap_(compare_) {}

template <typename T, typename CompactT, typename Compare>
KonFragmentMerge<T, CompactT, Compare>::~KonFragmentMerge() {}

template <typename T, typename CompactT, typename Compare>
int KonFragmentMerge<T, CompactT, Compare>::init(
    const common::ObIArray<FragmentIterator *> &iters, Compare *compare) {
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(iters_.assign(iters))) {
    KON_LOG(WARN, "fail to assign iterators", K(ret));
  } else {
    compare_.set_compare(compare);
  }
  return ret;
}

template <typename T, typename CompactT, typename Compare>
int KonFragmentMerge<T, CompactT, Compare>::open() {
  int ret = common::OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < iters_.count(); ++i) {
    if (OB_FAIL(iters_.at(i)->prefetch())) {
      KON_LOG(WARN, "fail to prefetch", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(build_heap())) {
      KON_LOG(WARN, "fail to build heap", K(ret));
    }
  }
  return ret;
}

template <typename T, typename CompactT, typename Compare>
int KonFragmentMerge<T, CompactT, Compare>::build_heap() {
  int ret = common::OB_SUCCESS;
  const T *item = NULL;
  HeapItem heap_item;

  for (int64_t i = 0; OB_SUCC(ret) && i < iters_.count(); ++i) {
    if (OB_FAIL(iters_.at(i)->get_next_item(item))) {
      if (common::OB_ITER_END != ret) {
        KON_LOG(WARN, "fail to get next item", K(ret), K(i));
      } else {
        ret = common::OB_SUCCESS;
      }
    } else {
      heap_item.item_ = item;
      heap_item.idx_ = i;
      if (OB_FAIL(heap_.push(heap_item))) {
        KON_LOG(WARN, "fail to push heap", K(ret));
      }
    }
  }

  return ret;
}

template <typename T, typename CompactT, typename Compare>
int KonFragmentMerge<T, CompactT, Compare>::heap_get_next_item(const T *&item) {
  int ret = common::OB_SUCCESS;
  HeapItem heap_item;
  if (OB_LIKELY(last_iter_idx_ >= 0 && last_iter_idx_ < iters_.count())) {
    FragmentIterator *iter = iters_.at(last_iter_idx_);
    if (OB_FAIL(iter->get_next_item(heap_item.item_))) {
      if (common::OB_ITER_END != ret) {
        KON_LOG(WARN, "fail to get next item", K(ret));
      } else if (OB_FAIL(heap_.pop())) { // overwrite OB_ITER_END
        KON_LOG(WARN, "fail to pop heap item", K(ret));
      }
    } else {
      heap_item.idx_ = last_iter_idx_;
      if (OB_FAIL(heap_.replace_top(heap_item))) {
        KON_LOG(WARN, "fail to replace heap top", K(ret));
      }
    }
    last_iter_idx_ = -1;
  }

  if (OB_SUCC(ret) && heap_.empty()) {
    ret = common::OB_ITER_END;
  }

  if (OB_SUCC(ret)) {
    const HeapItem *item_ptr = NULL;
    if (OB_FAIL(heap_.top(item_ptr))) {
      KON_LOG(WARN, "fail to get heap top item", K(ret));
    } else {
      last_iter_idx_ = item_ptr->idx_;
      item = item_ptr->item_;
    }
  }

  return ret;
}

template <typename T, typename CompactT, typename Compare>
int KonFragmentMerge<T, CompactT, Compare>::get_next_item(const T *&item) {
  int ret = common::OB_SUCCESS;
  item = NULL;
  if (OB_FAIL(heap_get_next_item(item))) {
    if (common::OB_ITER_END != ret) {
      KON_LOG(WARN, "fail to get next item from heap", K(ret));
    }
  }
  return ret;
}

class KonFileWriteReq {
public:
  int fd;
  char *buf;
  int64_t len;
  ObLightyQueue complete;
};

template <typename T, typename CompactT, typename Compare>
class KonMemorySortRound {
private:
  typedef KonFragmentReader<T, CompactT, Compare> FragmentReader;
  typedef FragmentReader FragmentIterator;
  typedef common::ObArray<FragmentIterator *> FragmentIteratorList;
  typedef KonFragmentWriter<CompactT> FragmentWriter;
  typedef KonMemorySortFileDesc<CompactT> FileDesc;
  typedef common::ObVector<FileDesc *> FileDescList;

  static constexpr int WRITE_BUF_NUM = 5;
  static constexpr int WRITE_BUF_PER = 10;
  static constexpr int CRITICAL_BUF_PER = 10;
  static constexpr int NON_CRITICAL_BUF_PER = 20;
  static constexpr int TOTAL_PER =
      WRITE_BUF_NUM * WRITE_BUF_PER + CRITICAL_BUF_PER + NON_CRITICAL_BUF_PER;

public:
  KonMemorySortRound();
  ~KonMemorySortRound();
  int init(const int64_t mem_limit, const int64_t file_buf_size,
           const uint64_t tenant_id, Compare *compare,
           common::ObLightyQueue *queue);
  int add_item(const T &item);
  int add_item_end();
  const FileDescList &file_desc() const { return files_; }
  TO_STRING_KV(K(""));

private:
  int build_fragment();

private:
  char *critical_buf_;
  int64_t critical_pos_;
  int64_t critical_mem_limit_;

  char *non_critical_buf_;
  int64_t non_critical_pos_;
  int64_t non_critical_mem_limit_;

  int cur_write_index_;
  char *write_buf_[WRITE_BUF_NUM];
  int write_buf_desc_[WRITE_BUF_NUM];
  int64_t write_mem_limit_;

  int64_t file_buf_size_;
  uint64_t tenant_id_;
  Compare *compare_;
  common::ObArenaAllocator file_desc_allocator_;
  common::ObLightyQueue *queue_;
  KonFileWriteReq write_req_;

  FragmentWriter writer_;
  FileDescList files_;
};

template <typename T, typename CompactT, typename Compare>
KonMemorySortRound<T, CompactT, Compare>::KonMemorySortRound()
    : critical_buf_(NULL), critical_pos_(0), critical_mem_limit_(0),
      non_critical_buf_(NULL), non_critical_pos_(0), non_critical_mem_limit_(0),
      cur_write_index_(0), write_buf_(), write_mem_limit_(0), file_buf_size_(0),
      tenant_id_(common::OB_INVALID_ID), compare_(NULL),
      file_desc_allocator_(common::ObNewModIds::OB_ASYNC_EXTERNAL_SORTER),
      queue_(NULL), write_req_(), writer_(), files_() {}

template <typename T, typename CompactT, typename Compare>
KonMemorySortRound<T, CompactT, Compare>::~KonMemorySortRound() {
  for (FileDesc *file : files_) {
    ::close(file->fd_);
  }
  munmap(write_buf_[0], write_mem_limit_ * WRITE_BUF_NUM);
}

template <typename T, typename CompactT, typename Compare>
int KonMemorySortRound<T, CompactT, Compare>::init(
    const int64_t mem_limit, const int64_t file_buf_size,
    const uint64_t tenant_id, Compare *compare, common::ObLightyQueue *queue) {
  int ret = OB_SUCCESS;
  file_desc_allocator_.set_tenant_id(tenant_id);

  critical_mem_limit_ =
      lower_align(mem_limit / TOTAL_PER * CRITICAL_BUF_PER, DIO_ALIGN_SIZE);
  non_critical_mem_limit_ =
      lower_align(mem_limit / TOTAL_PER * NON_CRITICAL_BUF_PER, DIO_ALIGN_SIZE);
  write_mem_limit_ =
      lower_align(mem_limit / TOTAL_PER * WRITE_BUF_PER, DIO_ALIGN_SIZE);

  if (OB_ISNULL(critical_buf_ = static_cast<char *>(
                    mmap(NULL, critical_mem_limit_ + non_critical_mem_limit_,
                         PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS,
                         -1, 0)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    KON_LOG(WARN, "fail to mmap memory");
  } else {
    non_critical_buf_ = critical_buf_ + critical_mem_limit_;
    critical_pos_ = non_critical_pos_ = 0;
  }

  if (OB_ISNULL(write_buf_[0] = static_cast<char *>(
                    mmap(NULL, write_mem_limit_ * WRITE_BUF_NUM,
                         PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS,
                         -1, 0)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    KON_LOG(WARN, "fail to mmap memory");
  } else {
    for (int i = 1; i < WRITE_BUF_NUM; ++i) {
      write_buf_[i] = write_buf_[i - 1] + write_mem_limit_;
    }
    for (int i = 0; i < WRITE_BUF_NUM; ++i) {
      write_buf_desc_[i] = -1;
    }
    cur_write_index_ = -1;
  }

  queue_ = queue;
  write_req_.complete.init(1);
  write_req_.fd = -1;
  file_buf_size_ = file_buf_size;
  tenant_id_ = tenant_id;
  compare_ = compare;

  return ret;
}

template <typename T, typename CompactT, typename Compare>
int KonMemorySortRound<T, CompactT, Compare>::add_item(const T &item) {
  int ret = OB_SUCCESS;
  char *buf = NULL;
  CompactT *compact_item = NULL;
  const int64_t critical_size =
      sizeof(CompactT) + item.get_compact_critical_size();
  const int64_t non_critical_size = item.get_compact_non_critical_size();
  if (OB_UNLIKELY(
          (critical_pos_ + critical_size > critical_mem_limit_) ||
          (non_critical_pos_ + non_critical_size > non_critical_mem_limit_)) &&
      OB_FAIL(build_fragment())) {
    KON_LOG(WARN, "fail to build fragment", KR(ret));
  } else {
    compact_item = new (critical_buf_ + critical_pos_) CompactT();
    critical_pos_ += critical_size;
    if (OB_FAIL(compact_item->deep_copy_non_critical(item, non_critical_buf_,
                                                     non_critical_mem_limit_,
                                                     non_critical_pos_))) {
      KON_LOG(WARN, "fail to deep copy item", KR(ret));
    }
  }

  return ret;
}

template <typename T, typename CompactT, typename Compare>
int KonMemorySortRound<T, CompactT, Compare>::add_item_end() {
  int ret = build_fragment();
  munmap(critical_buf_, critical_mem_limit_ + non_critical_mem_limit_);
  return ret;
}

template <typename T, typename CompactT, typename Compare>
int KonMemorySortRound<T, CompactT, Compare>::build_fragment() {
  int ret = OB_SUCCESS;
  cur_write_index_ = (cur_write_index_ + 1) % WRITE_BUF_NUM;
  if (write_req_.fd != -1) {
    void *dummy;
    while (write_req_.complete.pop(dummy, 1000) == OB_ENTRY_NOT_EXIST)
      ;
  }
  int next_write_index = (cur_write_index_ + 1) % WRITE_BUF_NUM;
  if (write_buf_desc_[next_write_index] != -1) {
    FileDesc *file = files_[write_buf_desc_[next_write_index]];
    int fd = ::open(KonTmpFileFolder, O_TMPFILE | O_RDWR | O_DIRECT, S_IRUSR | S_IWUSR);
    if (OB_UNLIKELY(fd == -1)) {
      ret = OB_FILE_NOT_OPENED;
      KON_LOG(WARN, "fail to open file", KR(ret));
    } else {
      write_req_.fd = fd;
      write_req_.buf = file->buf_;
      write_req_.len = file->buf_len_;
      file->fd_ = fd;
      file->buf_ = NULL;
      queue_->push(&write_req_);
    }
  }

  switch (compare_->get_rowkey_column_num()) {
  case 2: {
    auto begin =
        reinterpret_cast<sql::KonLoadDatumCompactRealRow<2> *>(critical_buf_);
    auto end = reinterpret_cast<sql::KonLoadDatumCompactRealRow<2> *>(
        critical_buf_ + critical_pos_);
    std::sort(begin, end, *compare_);
    break;
  }
  default: {
    ret = OB_NOT_SUPPORTED;
  }
  }

  if (OB_FAIL(writer_.open(tenant_id_, file_buf_size_,
                           write_buf_[cur_write_index_], write_mem_limit_))) {
    KON_LOG(WARN, "fail to open writer", KR(ret), K(tenant_id_),
            K(file_buf_size_), K(write_buf_), K(write_mem_limit_));
  } else {
    switch (compare_->get_rowkey_column_num()) {
    case 2: {
      auto begin =
          reinterpret_cast<sql::KonLoadDatumCompactRealRow<2> *>(critical_buf_);
      auto end = reinterpret_cast<sql::KonLoadDatumCompactRealRow<2> *>(
          critical_buf_ + critical_pos_);
      for (; begin < end; ++begin) {
        if (OB_FAIL(writer_.write_item(*begin))) {
          KON_LOG(WARN, "fail to write item", KR(ret));
        }
      }
      break;
    }
    default: {
      ret = OB_NOT_SUPPORTED;
    }
    }
    if (OB_SUCC(ret) && OB_FAIL(writer_.write_item_end())) {
      KON_LOG(WARN, "fail to end writer", KR(ret));
    }
  }

  if (OB_SUCC(ret)) {
    void *buf = NULL;
    FileDesc *file_desc = NULL;
    if (OB_ISNULL(buf = file_desc_allocator_.alloc(sizeof(FileDesc)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      KON_LOG(WARN, "fail to alloc memory", KR(ret));
    } else if (OB_ISNULL(file_desc = new (buf) FileDesc())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      KON_LOG(WARN, "fail to new FileDesc", KR(ret));
    } else {
      int64_t len = writer_.get_offset();
      file_desc->fd_ = -1;
      file_desc->buf_ = write_buf_[cur_write_index_];
      file_desc->buf_len_ = len;
      file_desc->descs_ = writer_.get_buf_desc();
      write_buf_desc_[cur_write_index_] = files_.size();
      files_.push_back(file_desc);

      KON_LOG(INFO, "build fragment", K(len), K(write_mem_limit_),
              K_(critical_pos), K_(critical_mem_limit), K_(non_critical_pos),
              K_(non_critical_mem_limit));
    }
  }

  writer_.reset();
  critical_pos_ = 0;
  non_critical_pos_ = 0;

  return ret;
}

template <typename T, typename CompactT, typename Compare>
class KonParallelMemorySortRound {
public:
  typedef KonFragmentReader<T, CompactT, Compare> FragmentReader;
  typedef FragmentReader FragmentIterator;
  typedef common::ObArray<FragmentIterator *> FragmentIteratorList;
  typedef KonMemorySortRound<T, CompactT, Compare> MemorySortRound;
  typedef KonMemorySortFileDesc<CompactT> FileDesc;
  typedef KonMacroBufferDesc<CompactT> MacroBufferDesc;
  typedef std::function<int(int, const T *&)> GetNextItemFunc;
  KonParallelMemorySortRound();
  ~KonParallelMemorySortRound();
  int init(const int64_t thread_num, const int64_t mem_limit,
           const int64_t file_buf_size, Compare *compare,
           GetNextItemFunc get_next_item,
           const common::ObArray<const share::schema::ObColumnSchemaV2 *>
               &column_schemas);
  int run();
  void generate_partition(int partition);
  int get_partition_iters(int idx, FragmentIteratorList &iters);

private:
  void background_write();

private:
  int64_t file_buf_size_;
  common::ObArenaAllocator orig_allocator_;
  common::ObSafeArenaAllocator allocator_;
  KonThreadPool thread_pool_;
  GetNextItemFunc get_next_item_;
  Compare *compare_;
  common::ObSEArray<MemorySortRound *, 8> memory_sort_rounds_;
  common::ObSEArray<CompactT *, 8> partition_items_;
  common::ObLightyQueue queue_;
  std::thread write_thread_;
  std::atomic_bool close_;
  common::ObArray<const share::schema::ObColumnSchemaV2 *> column_schemas_;
};

template <typename T, typename CompactT, typename Compare>
KonParallelMemorySortRound<T, CompactT, Compare>::KonParallelMemorySortRound()
    : orig_allocator_(common::ObNewModIds::OB_ASYNC_EXTERNAL_SORTER),
      allocator_(orig_allocator_) {}

template <typename T, typename CompactT, typename Compare>
KonParallelMemorySortRound<T, CompactT,
                           Compare>::~KonParallelMemorySortRound() {
  for (MemorySortRound *round : memory_sort_rounds_) {
    round->~MemorySortRound();
  }
  if (write_thread_.joinable()) {
    close_.store(true);
    write_thread_.join();
  }
}

template <typename T, typename CompactT, typename Compare>
void KonParallelMemorySortRound<T, CompactT, Compare>::background_write() {
  void *ptr = NULL;
  while (true) {
    int ret = queue_.pop(ptr, 10000);
    if (ret == OB_ENTRY_NOT_EXIST) {
      if (close_.load(std::memory_order_relaxed)) {
        return;
      }
    } else {
      KonFileWriteReq *req = static_cast<KonFileWriteReq *>(ptr);
      if (OB_UNLIKELY(::write(req->fd, req->buf, req->len) == -1)) {
        KON_LOG(WARN, "fail to direct append", K(req->fd), KP(req->buf), K(req->len), K(errno));
      }
      req->complete.push(ptr);
    }
  }
}

template <typename T, typename CompactT, typename Compare>
int KonParallelMemorySortRound<T, CompactT, Compare>::init(
    const int64_t thread_num, const int64_t total_mem_limit,
    const int64_t file_buf_size, Compare *compare,
    GetNextItemFunc get_next_item,
    const common::ObArray<const share::schema::ObColumnSchemaV2 *>
        &column_schemas) {
  int ret = OB_SUCCESS;
  file_buf_size_ = file_buf_size;
  get_next_item_ = get_next_item;
  compare_ = compare;
  column_schemas_ = column_schemas;
  const int64_t round_mem_limit = total_mem_limit / thread_num;
  orig_allocator_.set_tenant_id(MTL_ID());
  close_ = false;
  queue_.init(thread_num);
  for (int i = 0; OB_SUCC(ret) && i < thread_num; ++i) {
    void *buf = NULL;
    MemorySortRound *memory_sort = NULL;
    if (OB_ISNULL(buf = orig_allocator_.alloc(sizeof(MemorySortRound)))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      KON_LOG(WARN, "fail to allocate buf");
    } else {
      memory_sort = new (buf) MemorySortRound();
      if (OB_ISNULL(memory_sort)) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        KON_LOG(WARN, "fail to new KonMemorySortRound");
      } else if (OB_FAIL(memory_sort->init(round_mem_limit, file_buf_size,
                                           MTL_ID(), compare, &queue_))) {
        KON_LOG(WARN, "fail to init memory sort round", KR(ret));
      } else {
        memory_sort_rounds_.push_back(memory_sort);
      }
    }
  }

  KonThreadPool::KonFuncType thread_func = [&](uint64_t idx) -> int {
    int ret = OB_SUCCESS;
    const T *item = nullptr;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(get_next_item_(idx, item))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          KON_LOG(WARN, "fail to get next item", KR(ret));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else if (OB_FAIL(memory_sort_rounds_[idx]->add_item(*item))) {
        KON_LOG(WARN, "fail to add item to memory sort round");
      }
    }
    if (OB_SUCC(ret)) {
      memory_sort_rounds_[idx]->add_item_end();
    }
    return ret;
  };

  if (OB_SUCC(ret)) {
    thread_pool_.set_func(thread_func);
    if (OB_FAIL(thread_pool_.set_thread_num(thread_num))) {
      KON_LOG(WARN, "fail to set thread num");
    }
  }

  return ret;
}

template <typename T, typename CompactT, typename Compare>
int KonParallelMemorySortRound<T, CompactT, Compare>::run() {
  int ret = OB_SUCCESS;
  KON_LOG(INFO, "Parallel Memory Sort BEGIN");
  write_thread_ = std::thread([&]() { background_write(); });
  if (OB_FAIL(thread_pool_.execute())) {
    KON_LOG(WARN, "Parallel Memory Sort FAIL");
  } else {
    KON_LOG(INFO, "Parallel Memory Sort END");
  }
  close_.store(true);
  return ret;
}

template <typename T, typename CompactT, typename Compare>
void KonParallelMemorySortRound<T, CompactT, Compare>::generate_partition(
    int partition) {
  ObVector<CompactT *> macro_buffer_items;
  for (MemorySortRound *memory_sort : memory_sort_rounds_) {
    for (FileDesc *file : memory_sort->file_desc()) {
      for (MacroBufferDesc &desc : file->descs_) {
        macro_buffer_items.push_back(desc.sample_item_);
      }
    }
  }
  std::sort(macro_buffer_items.begin(), macro_buffer_items.end(), *compare_);

  int32_t item_size = macro_buffer_items.size();
  for (int i = 1; i < partition; i++) {
    partition_items_.push_back(macro_buffer_items[item_size / partition * i]);
  }
}

template <typename T, typename CompactT, typename Compare>
int KonParallelMemorySortRound<T, CompactT, Compare>::get_partition_iters(
    int idx, FragmentIteratorList &iters) {
  int ret = OB_SUCCESS;
  int total_partition = partition_items_.count() + 1;
  CompactT *begin_item = NULL;
  CompactT *end_item = NULL;
  if (idx != 0) {
    begin_item = partition_items_[idx - 1];
  }
  if (idx != total_partition - 1) {
    end_item = partition_items_[idx];
  }
  auto desc_compare = [&](const MacroBufferDesc &lhs,
                          const MacroBufferDesc &rhs) {
    return compare_->operator()(lhs.sample_item_, rhs.sample_item_);
  };
  MacroBufferDesc comp_desc{.sample_item_ = begin_item};
  for (MemorySortRound *memory_sort : memory_sort_rounds_) {
    for (FileDesc *file : memory_sort->file_desc()) {
      int macro_buffer_idx = 0;
      if (begin_item != NULL) {
        auto begin = file->descs_.begin();
        auto end = file->descs_.end();
        auto iter = std::upper_bound(begin, end, comp_desc, desc_compare);
        macro_buffer_idx = iter - begin;
        if (macro_buffer_idx > 0) {
          macro_buffer_idx -= 1;
        }
      }
      void *buf = NULL;
      FragmentReader *reader = NULL;
      if (OB_ISNULL(buf = allocator_.alloc(sizeof(FragmentReader)))) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        KON_LOG(WARN, "fail to allocate memory", KR(ret));
      } else if (OB_ISNULL(reader = new (buf) FragmentReader())) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        KON_LOG(WARN, "fail to placement new FragmentReader", KR(ret));
      } else if (OB_FAIL(reader->init(file->fd_, file->buf_, MTL_ID(),
                                      file_buf_size_, begin_item, end_item,
                                      compare_, macro_buffer_idx, file->descs_,
                                      column_schemas_))) {
        KON_LOG(WARN, "fail to init FragmentReader", KR(ret));
      } else {
        iters.push_back(reader);
      }
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase