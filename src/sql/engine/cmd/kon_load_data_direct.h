#pragma once

#include "lib/timezone/ob_timezone_info.h"
#include "sql/engine/cmd/kon_load_data_parser.h"
#include "sql/engine/cmd/kon_load_data_row.h"
#include "sql/engine/cmd/ob_load_data_impl.h"
#include "storage/blocksstable/ob_index_block_builder.h"
#include "storage/kon_parallel_external_sort.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include <libaio.h>

namespace oceanbase {
namespace sql {

class KonLoadDataBuffer {
public:
  KonLoadDataBuffer();
  ~KonLoadDataBuffer();
  void reset();
  int create(int64_t buffer_size);
  void switch_next(int64_t new_size);
  OB_INLINE char *begin() const { return data_[cur_index_] + begin_pos_; }
  OB_INLINE char *end() const { return data_[cur_index_] + end_pos_; }
  OB_INLINE bool empty() const { return end_pos_ == begin_pos_; }
  OB_INLINE void consume(int64_t size) { begin_pos_ += size; }
  OB_INLINE char *next_buffer() const {
    return data_[(cur_index_ + 1) % 2] + DIO_ALIGN_SIZE;
  }
  TO_STRING_KV(K_(cur_index), K_(begin_pos), K_(end_pos), K_(buffer_size));

private:
  char *data_[2];
  int cur_index_;
  int64_t begin_pos_;
  int64_t end_pos_;
  int64_t buffer_size_;
};

class KonLoadCSVParser {
public:
  KonLoadCSVParser();
  ~KonLoadCSVParser();
  void reset();
  int init(const ObDataInFileStruct &format, int64_t column_count,
           common::ObCollationType collation_type);
  int get_next_row(KonLoadDataBuffer &buffer, const common::ObNewRow *&row,
                   int64_t &pos);

private:
  common::ObArenaAllocator allocator_;
  common::ObCollationType collation_type_;
  KonCSVSpecialParser csv_parser_;
  common::ObNewRow row_;
};

class KonLoadRowCaster {
public:
  KonLoadRowCaster();
  ~KonLoadRowCaster();
  int init(const share::schema::ObTableSchema *table_schema,
           const common::ObIArray<ObLoadDataStmt::FieldOrVarStruct>
               &field_or_var_list);
  int get_casted_row(const common::ObNewRow &new_row,
                     const KonLoadDatumRow *&datum_row);
  TO_STRING_KV(K_(column_count));
  const common::ObArray<const share::schema::ObColumnSchemaV2 *> &
  get_column_schemas() {
    return column_schemas_;
  };

private:
  int init_column_schemas_and_idxs(
      const share::schema::ObTableSchema *table_schema,
      const common::ObIArray<ObLoadDataStmt::FieldOrVarStruct>
          &field_or_var_list);
  OB_INLINE int cast_obj(const share::schema::ObColumnSchemaV2 *column_schema,
                         const common::ObObj &src_obj,
                         common::ObObj &dest_obj) {
    int ret = OB_SUCCESS;
    ObDataTypeCastParams cast_params(&tz_info_);
    ObCastCtx cast_ctx(&cast_allocator_, &cast_params, CM_NONE,
                       collation_type_);
    const ObObjType expect_type = column_schema->get_meta_type().get_type();
    if (OB_UNLIKELY(src_obj.is_null())) {
      dest_obj.set_null();
    } else if (OB_UNLIKELY(is_oracle_mode() && (src_obj.is_null_oracle() ||
                                                0 == src_obj.get_val_len()))) {
      dest_obj.set_null();
    } else if (OB_UNLIKELY(is_mysql_mode() && 0 == src_obj.get_val_len() &&
                           !ob_is_string_tc(expect_type))) {
      ObObj zero_obj;
      zero_obj.set_int(0);
      if (OB_FAIL(ObObjCaster::to_type(expect_type, cast_ctx, zero_obj,
                                       dest_obj))) {
        KON_LOG(WARN, "fail to do to type", KR(ret), K(zero_obj),
                K(expect_type));
      }
    } else {
      switch (expect_type) {
      case ObObjType::ObCharType: {
        dest_obj.set_char(src_obj.get_varchar());
        break;
      }
      case ObObjType::ObVarcharType: {
        dest_obj.set_varchar(src_obj.get_varchar());
        break;
      }
      case ObObjType::ObInt32Type: {
        dest_obj.set_int32(atoi(src_obj.get_varchar().ptr()));
        break;
      }
      case ObObjType::ObIntType: {
        dest_obj.set_int(atol(src_obj.get_varchar().ptr()));
        break;
      }
      case ObObjType::ObDateType: {
        // format YYYY-MM-DD
        ObTime time;
        char *ptr = src_obj.get_varchar().ptr();
        time.parts_[DT_YEAR] = (*ptr - '0') * 1000 + (*(ptr + 1) - '0') * 100 +
                               (*(ptr + 2) - '0') * 10 + (*(ptr + 3) - '0');
        time.parts_[DT_MON] = (*(ptr + 5) - '0') * 10 + (*(ptr + 6) - '0');
        time.parts_[DT_MDAY] = (*(ptr + 8) - '0') * 10 + (*(ptr + 9) - '0');
        int date = ObTimeConverter::ob_time_to_date(time);
        dest_obj.set_date(date);
        break;
      }
      default: {
        if (OB_FAIL(ObObjCaster::to_type(expect_type, cast_ctx, src_obj,
                                         dest_obj))) {
          KON_LOG(WARN, "fail to do to type", KR(ret), K(src_obj),
                  K(expect_type));
        }
      }
      }
    }
    return ret;
  }

private:
  common::ObArray<const share::schema::ObColumnSchemaV2 *> column_schemas_;
  common::ObArray<int64_t>
      column_idxs_; // Mapping of store columns to source data columns
  int64_t column_count_;
  common::ObCollationType collation_type_;
  KonLoadDatumRow datum_row_;
  common::ObArenaAllocator cast_allocator_;
  common::ObTimeZoneInfo tz_info_;
};

class KonLoadCSVSubReader {
  static const int64_t BUFFER_SIZE = (2LL << 20); // 2M
public:
  KonLoadCSVSubReader();
  ~KonLoadCSVSubReader();
  int init(const ObString &filepath, int idx, int total,
           const ObDataInFileStruct &format, int64_t column_count,
           common::ObCollationType collation_type);
  int get_next_row(const common::ObNewRow *&row);
  void reset();
  TO_STRING_KV(K_(offset), K_(end));

private:
  int init_file(const ObString &filepath, int partition_idx,
                int total_partition);
  int init_csv_parser(const ObDataInFileStruct &format, int64_t column_count,
                      common::ObCollationType collation_type);
  int read_next_buffer();
  int get_next_csv_row(const common::ObNewRow *&row);
  int prefetch();
  int wait();

private:
  int fd_;
  io_context_t ctx_;
  KonLoadDataBuffer buffer_;
  KonLoadCSVParser csv_parser_;

  int64_t scan_offset_;
  int64_t offset_;
  int64_t end_;
};

class KonLoadSSTableBuilder {
public:
  KonLoadSSTableBuilder();
  ~KonLoadSSTableBuilder();
  int init(const share::schema::ObTableSchema *table_schema);
  int init_macro_block_writer(int idx,
                              blocksstable::ObMacroBlockWriter *writer);
  int init_datum_row(blocksstable::ObDatumRow *row);

  int close();

  const share::schema::ObTableSchema *get_table_schema() const {
    return table_schema_;
  }

private:
  int init_sstable_index_builder(
      const share::schema::ObTableSchema *table_schema);
  int create_sstable();

private:
  const share::schema::ObTableSchema *table_schema_;
  common::ObTabletID tablet_id_;
  storage::ObTabletHandle tablet_handle_;
  share::ObLSID ls_id_;
  storage::ObLSHandle ls_handle_;
  int64_t rowkey_column_num_;
  int64_t extra_rowkey_column_num_;
  int64_t column_count_;
  storage::ObITable::TableKey table_key_;
  blocksstable::ObSSTableIndexBuilder sstable_index_builder_;
  blocksstable::ObDataStoreDesc data_store_desc_;
  bool is_closed_;
  bool is_inited_;
};

class KonLoadMacroBlockWriter {
public:
  KonLoadMacroBlockWriter();
  ~KonLoadMacroBlockWriter();
  int init(int idx, KonLoadSSTableBuilder *builder);
  int append_row(const KonLoadDatumRow &datum_row);
  int close();
  TO_STRING_KV(K_(column_count));

private:
  blocksstable::ObMacroBlockWriter macro_block_writer_;
  blocksstable::ObDatumRow datum_row_;
  int64_t column_count_;
  int64_t rowkey_column_num_;
  int64_t extra_rowkey_column_num_;
};

class KonLoadParallelSSTableBuilder {
public:
  typedef KonLoadDatumRow RowType;
  typedef std::function<int(int, const RowType *&)> GetNextItemFunc;
  KonLoadParallelSSTableBuilder();
  ~KonLoadParallelSSTableBuilder();
  int init(int thread_num, const share::schema::ObTableSchema *table_schema);
  int run(GetNextItemFunc get_next_item_func);

private:
  common::ObArenaAllocator allocator_;
  common::ObSEArray<KonLoadMacroBlockWriter *, 8> writers_;
  KonLoadSSTableBuilder builder_;
  KonThreadPool thread_pool_;
};

class KonLoadDataDirect : public ObLoadDataBase {
  static const int MEMORY_SORT_THREAD_NUM = 10;
  static const int SSTABLE_THREAD_NUM = 8;
  static const int64_t MEM_BUFFER_SIZE = (11LL << 30); // 11G
  static const int64_t FILE_BUFFER_SIZE = (1LL << 20); // 1M
public:
  KonLoadDataDirect();
  virtual ~KonLoadDataDirect();
  int execute(ObExecContext &ctx, ObLoadDataStmt &load_stmt) override;

private:
  int inner_init(ObLoadDataStmt &load_stmt);
  int do_load();

private:
  typedef KonLoadDatumRow RowType;
  typedef KonLoadDatumCompactRow CompactRowType;
  typedef KonLoadDatumRowCompare RowCompareType;
  typedef KonParallelMemorySortRound<RowType, CompactRowType, RowCompareType>
      MemorySortRound;
  typedef KonFragmentMerge<RowType, CompactRowType, RowCompareType>
      FragmentMerge;

  common::ObSEArray<KonLoadCSVSubReader *, MEMORY_SORT_THREAD_NUM> readers_;
  common::ObSEArray<KonLoadRowCaster *, MEMORY_SORT_THREAD_NUM> casters_;
  MemorySortRound memory_sort_round_;

  common::ObSEArray<MemorySortRound::FragmentIteratorList *, SSTABLE_THREAD_NUM>
      iters_;
  common::ObSEArray<FragmentMerge *, SSTABLE_THREAD_NUM> mergers_;
  KonLoadParallelSSTableBuilder sstable_builder_;

  common::ObArenaAllocator allocator_;
  blocksstable::ObStorageDatumUtils datum_utils_;
  KonLoadDatumRowCompare compare_;
};

} // namespace sql
} // namespace oceanbase