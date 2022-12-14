#pragma once

#include "common/object/ob_object.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/ob_define.h"
#include "sql/engine/cmd/ob_load_data_impl.h"

namespace oceanbase {
namespace sql {

OB_INLINE void EncodeVarint32(char *buf, int64_t &pos, uint32_t v) {
  // Operate on characters as unsigneds
  uint8_t *ptr = reinterpret_cast<uint8_t *>(buf + pos);
  static const int B = 128;
  if (v < (1 << 7)) {
    *(ptr++) = v;
    pos += 1;
  } else if (v < (1 << 14)) {
    *(ptr++) = v | B;
    *(ptr++) = v >> 7;
    pos += 2;
  } else if (v < (1 << 21)) {
    *(ptr++) = v | B;
    *(ptr++) = (v >> 7) | B;
    *(ptr++) = v >> 14;
    pos += 3;
  } else if (v < (1 << 28)) {
    *(ptr++) = v | B;
    *(ptr++) = (v >> 7) | B;
    *(ptr++) = (v >> 14) | B;
    *(ptr++) = v >> 21;
    pos += 4;
  } else {
    *(ptr++) = v | B;
    *(ptr++) = (v >> 7) | B;
    *(ptr++) = (v >> 14) | B;
    *(ptr++) = (v >> 21) | B;
    *(ptr++) = v >> 28;
    pos += 5;
  }
}

OB_INLINE uint32_t GetVarint32Fallback(const char *buf, int64_t &pos) {
  uint32_t result = 0;
  for (uint32_t shift = 0; shift <= 28; shift += 7) {
    uint32_t byte = *(reinterpret_cast<const uint8_t *>(buf + pos));
    pos += 1;
    if (byte & 128) {
      // More bytes are present
      result |= ((byte & 127) << shift);
    } else {
      result |= (byte << shift);
      return result;
    }
  }
  return 0;
}

OB_INLINE uint32_t GetVarint32(const char *buf, int64_t &pos) {
  uint32_t result = *(reinterpret_cast<const uint8_t *>(buf + pos));
  if ((result & 128) == 0) {
    pos += 1;
    return result;
  }
  return GetVarint32Fallback(buf, pos);
}

OB_INLINE void EncodeVarint64(char *buf, int64_t &pos, uint64_t v) {
  static const int B = 128;
  uint8_t *ptr = reinterpret_cast<uint8_t *>(buf + pos);
  while (v >= B) {
    *(ptr++) = v | B;
    pos += 1;
    v >>= 7;
  }
  *(ptr++) = static_cast<uint8_t>(v);
  pos += 1;
}

OB_INLINE uint64_t GetVarint64(const char *buf, int64_t &pos) {
  uint64_t result = 0;
  for (uint32_t shift = 0; shift <= 63; shift += 7) {
    uint64_t byte = *(reinterpret_cast<const uint8_t *>(buf + pos));
    pos += 1;
    if (byte & 128) {
      // More bytes are present
      result |= ((byte & 127) << shift);
    } else {
      result |= (byte << shift);
      return result;
    }
  }
  return 0;
}

OB_INLINE int64_t KonObjSerializeSize(const common::ObObj &obj) {
  switch (obj.get_type()) {
  case common::ObIntType: {
    return 8;
  }
  case common::ObInt32Type:
  case ObObjType::ObDateType: {
    return 4;
  }
  case common::ObCharType:
  case common::ObVarcharType: {
    return 4 + obj.val_len_;
  }
  case common::ObNumberType: {
    return sizeof(*obj.v_.nmb_digits_) * obj.nmb_desc_.len_ + 4;
  }
  default: {
    return obj.get_serialize_size();
  }
  }
}

OB_INLINE int KonObjSerialize(const common::ObObj &obj, SERIAL_PARAMS) {
  int ret = OB_SUCCESS;
  switch (obj.get_type()) {
  case common::ObIntType: {
    EncodeVarint64(buf, pos, static_cast<uint64_t>(obj.v_.int64_));
    break;
  }
  case common::ObInt32Type: {
    EncodeVarint32(buf, pos, static_cast<uint32_t>(obj.v_.int64_));
    break;
  }
  case ObObjType::ObDateType: {
    EncodeVarint32(buf, pos, static_cast<uint32_t>(obj.v_.date_));
    break;
  }
  case common::ObCharType:
  case common::ObVarcharType: {
    EncodeVarint32(buf, pos, static_cast<uint32_t>(obj.val_len_));
    MEMCPY(buf + pos, obj.v_.string_, obj.val_len_);
    pos += obj.val_len_;
    break;
  }
  case common::ObNumberType: {
    *(int32_t *)(buf + pos) = obj.val_len_;
    pos += 4;
    MEMCPY(buf + pos, obj.v_.nmb_digits_,
           sizeof(*obj.v_.nmb_digits_) * obj.nmb_desc_.len_);
    pos += sizeof(*obj.v_.nmb_digits_) * obj.nmb_desc_.len_;
    break;
  }
  default: {
    ret = obj.serialize(buf, buf_len, pos);
  }
  }
  return ret;
}

OB_INLINE int KonObjDeserialize(common::ObObj &obj, ObObjType expect_type,
                                DESERIAL_PARAMS) {
  int ret = OB_SUCCESS;
  switch (expect_type) {
  case common::ObIntType: {
    obj.set_int(static_cast<int64_t>(GetVarint64(buf, pos)));
    break;
  }
  case common::ObInt32Type: {
    obj.set_int32(static_cast<int32_t>(GetVarint32Fallback(buf, pos)));
    break;
  }
  case ObObjType::ObDateType: {
    obj.set_date(static_cast<int32_t>(GetVarint32Fallback(buf, pos)));
    break;
  }
  case common::ObCharType:
  case common::ObVarcharType: {
    int32_t len = static_cast<int32_t>(GetVarint32(buf, pos));
    obj.set_string(expect_type, buf + pos, len);
    pos += len;
    break;
  }
  case common::ObNumberType: {
    number::ObNumber::Desc desc =
        *(reinterpret_cast<const number::ObNumber::Desc *>(buf + pos));
    pos += 4;
    uint32_t *nmb_digits =
        const_cast<uint32_t *>(reinterpret_cast<const uint32_t *>(buf + pos));
    obj.set_number(desc, nmb_digits);
    pos += sizeof(uint32_t) * desc.len_;
    break;
  }
  default: {
    ret = obj.deserialize(buf, data_len, pos);
  }
  }
  return ret;
}

class KonLoadDatumRow {
public:
  KonLoadDatumRow()
      : capacity_(0), count_(0), rowkey_count_(0), self_alloc_(false),
        objs_(nullptr) {}

  ~KonLoadDatumRow() { reset(); }

  OB_INLINE void reset() {
    capacity_ = 0;
    count_ = 0;
    if (self_alloc_ && objs_ != nullptr) {
      ob_free(objs_);
      objs_ = nullptr;
      self_alloc_ = false;
    }
  }

  int init(int64_t capacity, int64_t rowkey_count) {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(capacity <= 0)) {
      ret = OB_INVALID_ARGUMENT;
      KON_LOG(WARN, "invalid args", KR(ret), K(capacity));
    } else {
      reset();
      ObMemAttr attr(MTL_ID(), ObModIds::OB_SQL_LOAD_DATA);
      if (OB_ISNULL(objs_ = static_cast<ObObj *>(
                        ob_malloc(sizeof(ObObj) * capacity, attr)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        KON_LOG(WARN, "fail to alloc memory", KR(ret));
      } else {
        self_alloc_ = true;
        new (objs_) ObObj[capacity];
        capacity_ = capacity;
        count_ = capacity;
        rowkey_count_ = rowkey_count;
      }
    }
    return ret;
  }

  OB_INLINE int64_t get_compact_critical_size() const {
    return sizeof(ObObj) * rowkey_count_;
  }

  OB_INLINE int64_t get_compact_non_critical_size() const {
    int64_t size = 0;
    for (int64_t i = 0; i < rowkey_count_; ++i) {
      size += objs_[i].get_deep_copy_size();
    }
    for (int64_t i = rowkey_count_; i < count_; ++i) {
      size += KonObjSerializeSize(objs_[i]);
    }
    return size;
  }

  OB_INLINE int
  deserialize(const common::ObArray<const share::schema::ObColumnSchemaV2 *>
                  &column_schemas,
              DESERIAL_PARAMS) {
    int ret = OB_SUCCESS;
    int column_size = column_schemas.size();
    if (OB_UNLIKELY(capacity_ < column_size) && OB_FAIL(init(column_size, 0))) {
      KON_LOG(WARN, "fail to init", KR(ret));
    }
    for (int i = 0; OB_SUCC(ret) && i < column_size; ++i) {
      if (OB_FAIL(KonObjDeserialize(
              objs_[i], column_schemas[i]->get_meta_type().get_type(), buf,
              data_len, pos))) {
        KON_LOG(WARN, "fail to deserialize", KR(ret));
      }
    }
    return ret;
  }

  OB_INLINE bool is_valid() const { return count_ > 0 && nullptr != objs_; }

  TO_STRING_KV(K_(capacity), K_(count));

public:
  int16_t capacity_;
  int16_t count_;
  int16_t rowkey_count_;
  bool self_alloc_;
  common::ObObj *objs_;
};

class KonLoadDatumCompactRow {
public:
  KonLoadDatumCompactRow() = default;
  ~KonLoadDatumCompactRow() = default;
  OB_INLINE SERIALIZE_SIGNATURE(serialize) {
    int ret = OB_SUCCESS;
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_count_; ++i) {
      KonObjSerialize(objs_[i], buf, buf_len, pos);
    }
    MEMCPY(buf + pos, compact_, compact_len_);
    pos += compact_len_;
    return ret;
  }
  OB_INLINE int get_serialize_size() const {
    int64_t len = 0;
    for (int64_t i = 0; i < rowkey_count_; ++i) {
      len += KonObjSerializeSize(objs_[i]);
    }
    len += compact_len_;
    return len;
  }
  OB_INLINE int deep_copy_non_critical(const KonLoadDatumRow &src, char *buf,
                                       int64_t len, int64_t &pos) {
    int ret = OB_SUCCESS;
    rowkey_count_ = src.rowkey_count_;
    total_count_ = src.count_;

    for (int16_t i = 0; OB_SUCC(ret) && i < rowkey_count_; ++i) {
      if (OB_FAIL(objs_[i].deep_copy(src.objs_[i], buf, len, pos))) {
        KON_LOG(WARN, "fail to deep copy obj", KR(ret), K(src.objs_[i]));
      }
    }
    if (OB_SUCC(ret)) {
      compact_ = buf + pos;
      int64_t prev_pos = pos;
      for (int i = rowkey_count_; OB_SUCC(ret) && i < total_count_; ++i) {
        if (OB_FAIL(KonObjSerialize(src.objs_[i], buf, len, pos))) {
          KON_LOG(WARN, "fail to serialize obj", KR(ret), K(src.objs_[i]));
        }
      }
      compact_len_ = pos - prev_pos;
    }
    return ret;
  }
  OB_INLINE int deep_copy_critical(const KonLoadDatumCompactRow &src, char *buf,
                                   int64_t len, int64_t &pos) {
    // only deep copy rowkey
    int ret = OB_SUCCESS;
    rowkey_count_ = src.rowkey_count_;
    total_count_ = src.total_count_;
    compact_len_ = src.compact_len_;

    pos += sizeof(ObObj) * rowkey_count_;
    for (int16_t i = 0; OB_SUCC(ret) && i < rowkey_count_; ++i) {
      if (OB_FAIL(objs_[i].deep_copy(src.objs_[i], buf, len, pos))) {
        KON_LOG(WARN, "fail to deep copy obj", KR(ret), K(src.objs_[i]));
      }
    }
    return ret;
  }
  OB_INLINE int get_deep_copy_critical_size() const {
    // only deep copy rowkey
    int64_t size = sizeof(ObObj) * rowkey_count_;
    for (int64_t i = 0; i < rowkey_count_; ++i) {
      size += objs_[i].get_deep_copy_size();
    }
    return size;
  }
  TO_STRING_KV(K_(rowkey_count), K_(total_count), K_(compact_len));

public:
  int16_t rowkey_count_;
  int16_t total_count_;
  int32_t compact_len_;
  char *compact_;
  common::ObObj objs_[0];
};

template <unsigned int N>
class KonLoadDatumCompactRealRow : public KonLoadDatumCompactRow {
public:
  common::ObObj real_objs_[N];
};

class KonLoadDatumRowCompare {
public:
  KonLoadDatumRowCompare() = default;
  ~KonLoadDatumRowCompare() = default;
  int init(int64_t rowkey_column_num, const ObTableSchema *table_schema) {
    int ret = OB_SUCCESS;
    rowkey_column_num_ = rowkey_column_num;
    if (OB_FAIL(table_schema->get_column_ids(col_descs_))) {
      KON_LOG(WARN, "fail to get ObColDesc");
    }
    return ret;
  }
  OB_INLINE int64_t get_rowkey_column_num() { return rowkey_column_num_; }
  OB_INLINE bool operator()(const KonLoadDatumRow *const lhs,
                            const KonLoadDatumRow *const rhs) {
    int cmp_ret = 0;
    for (int i = 0; i < rowkey_column_num_ && cmp_ret == 0; i++) {
      switch (lhs->objs_[i].get_type()) {
      case ObObjType::ObInt32Type: {
        cmp_ret = lhs->objs_[i].get_int32() - rhs->objs_[i].get_int32();
        break;
      }
      case ObObjType::ObIntType: {
        cmp_ret = lhs->objs_[i].get_int() - rhs->objs_[i].get_int();
        break;
      }
      default: {
        const share::schema::ObColDesc &col_desc = col_descs_[i];
        ObObjCmpFuncs::compare(lhs->objs_[i], rhs->objs_[i],
                               col_desc.col_type_.get_collation_type(),
                               cmp_ret);
      }
      }
    }
    return cmp_ret < 0;
  }

  OB_INLINE bool operator()(const KonLoadDatumCompactRow &lhs,
                            const KonLoadDatumCompactRow &rhs) {
    int cmp_ret = 0;
    for (int i = 0; i < rowkey_column_num_ && cmp_ret == 0; i++) {
      switch (lhs.objs_[i].get_type()) {
      case ObObjType::ObInt32Type: {
        cmp_ret = lhs.objs_[i].get_int32() - rhs.objs_[i].get_int32();
        break;
      }
      case ObObjType::ObIntType: {
        cmp_ret = lhs.objs_[i].get_int() - rhs.objs_[i].get_int();
        break;
      }
      default: {
        const share::schema::ObColDesc &col_desc = col_descs_[i];
        ObObjCmpFuncs::compare(lhs.objs_[i], rhs.objs_[i],
                               col_desc.col_type_.get_collation_type(),
                               cmp_ret);
      }
      }
    }
    return cmp_ret < 0;
  }

  OB_INLINE bool operator()(const KonLoadDatumCompactRow *const lhs,
                            const KonLoadDatumCompactRow *const rhs) {
    return operator()(*lhs, *rhs);
  }

  OB_INLINE bool operator()(const KonLoadDatumRow *const lhs,
                            const KonLoadDatumCompactRow *const rhs) {
    int cmp_ret = 0;
    for (int i = 0; i < rowkey_column_num_ && cmp_ret == 0; i++) {
      switch (lhs->objs_[i].get_type()) {
      case ObObjType::ObInt32Type: {
        cmp_ret = lhs->objs_[i].get_int32() - rhs->objs_[i].get_int32();
        break;
      }
      case ObObjType::ObIntType: {
        cmp_ret = lhs->objs_[i].get_int() - rhs->objs_[i].get_int();
        break;
      }
      default: {
        const share::schema::ObColDesc &col_desc = col_descs_[i];
        ObObjCmpFuncs::compare(lhs->objs_[i], rhs->objs_[i],
                               col_desc.col_type_.get_collation_type(),
                               cmp_ret);
      }
      }
    }
    return cmp_ret < 0;
  }

public:
  const int result_code_ = OB_SUCCESS;

private:
  int64_t rowkey_column_num_;
  ObSEArray<ObColDesc, 32> col_descs_;
};

} // namespace sql
} // namespace oceanbase