#include "common/object/ob_object.h"
#include "lib/charset/ob_charset.h"
#include "lib/container/ob_se_array.h"
#include "lib/string/ob_string.h"
#include "sql/resolver/cmd/ob_load_data_stmt.h"

#ifndef _KON_LOAD_DATA_PARSER_H_
#define _KON_LOAD_DATA_PARSER_H_

namespace oceanbase {
namespace sql {

class KonCSVSpecialParser {
public:
  struct FieldValue {
    FieldValue() : ptr_(nullptr), len_(0), flags_(0) {}
    char *ptr_;
    int32_t len_;
    union {
      struct {
        uint32_t is_null_ : 1;
        uint32_t reserved_ : 31;
      };
      uint32_t flags_;
    };
    TO_STRING_KV(KP(ptr_), K(len_), K(flags_), "string",
                 common::ObString(len_, ptr_));
  };

public:
  KonCSVSpecialParser() {}
  OB_INLINE int init(const ObDataInFileStruct &format, int64_t file_column_nums,
                     common::ObCollationType file_cs_type) {
    int ret = OB_SUCCESS;

    if (OB_SUCC(ret) &&
        OB_FAIL(fields_per_line_.prepare_allocate(file_column_nums))) {
      KON_LOG(WARN, "fail to allocate memory", K(ret), K(file_column_nums));
    }

    return ret;
  }

  OB_INLINE int scan(const char *&str, const char *end, int64_t &nrows) {
    int ret = OB_SUCCESS;
    constexpr char field_enclosed_char_ = '|';
    constexpr char field_escaped_char_ = '\n';

    bool find_new_line = false;
    int field_idx = 0;
    const char *field_begin = str;
    for (; !find_new_line && str < end; str++) {
      if (field_enclosed_char_ == *str) {
        gen_new_field(field_begin, str, field_idx++);
        field_begin = str + 1;
      } else if (field_escaped_char_ == *str) {
        find_new_line = true;
      }
    }

    nrows = find_new_line ? 1 : 0;
    return ret;
  }

  OB_INLINE void gen_new_field(const char *field_begin, const char *field_end,
                               int field_idx) {
    int str_len = field_end - field_begin;
    FieldValue &new_field = fields_per_line_[field_idx];
    if (OB_UNLIKELY(0 == str_len)) {
      new_field.is_null_ = 1;
    } else {
      new_field.ptr_ = const_cast<char *>(field_begin);
      new_field.len_ = str_len;
      new_field.is_null_ = 0;
    }
  }
  OB_INLINE common::ObIArray<FieldValue> &get_fields_per_line() {
    return fields_per_line_;
  }

private:
  common::ObSEArray<FieldValue, 16> fields_per_line_;
};

} // namespace sql
} // namespace oceanbase

#endif // _KON_LOAD_DATA_PARSER_H_