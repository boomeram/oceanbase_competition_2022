#include <stdlib.h>
#include <sys/stat.h>

#include "observer/omt/ob_tenant_timezone_mgr.h"
#include "sql/engine/cmd/kon_load_data_direct.h"
#include "storage/tablet/ob_tablet_create_delete_helper.h"
#include "storage/tx_storage/ob_ls_service.h"

namespace oceanbase {
namespace sql {

/**
 * KonLoadDataBuffer
 */

KonLoadDataBuffer::KonLoadDataBuffer()
    : data_(), cur_index_(0), begin_pos_(0), end_pos_(0), buffer_size_(0) {}

KonLoadDataBuffer::~KonLoadDataBuffer() { reset(); }

void KonLoadDataBuffer::reset() {
  if (data_[0] != NULL) {
    ::free(data_[0]);
    data_[0] = NULL;
  }
  if (data_[1] != NULL) {
    ::free(data_[1]);
    data_[1] = NULL;
  }
  cur_index_ = 0;
  begin_pos_ = 0;
  end_pos_ = 0;
  buffer_size_ = 0;
}

int KonLoadDataBuffer::create(int64_t buffer_size) {
  int ret = OB_SUCCESS;
  int64_t capacity = DIO_ALIGN_SIZE + buffer_size;
  if (OB_ISNULL(data_[0] = static_cast<char *>(
                    ::aligned_alloc(DIO_ALIGN_SIZE, capacity)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    KON_LOG(WARN, "fail to allocate memory");
  } else if (OB_ISNULL(data_[1] = static_cast<char *>(
                           ::aligned_alloc(DIO_ALIGN_SIZE, capacity)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    KON_LOG(WARN, "fail to allocate memory");
  } else {
    cur_index_ = 0;
    begin_pos_ = DIO_ALIGN_SIZE;
    end_pos_ = DIO_ALIGN_SIZE + buffer_size;
    buffer_size_ = buffer_size;
  }
  return ret;
}

void KonLoadDataBuffer::switch_next(int64_t new_size) {
  const int64_t data_size = end_pos_ - begin_pos_;
  int next_index = (cur_index_ + 1) % 2;
  MEMCPY(data_[next_index] + DIO_ALIGN_SIZE - data_size, begin(), data_size);
  begin_pos_ = DIO_ALIGN_SIZE - data_size;
  end_pos_ = DIO_ALIGN_SIZE + new_size;
  cur_index_ = next_index;
}

/**
 * KonLoadCSVParser
 */

KonLoadCSVParser::KonLoadCSVParser()
    : allocator_(ObModIds::OB_SQL_LOAD_DATA), collation_type_(CS_TYPE_INVALID) {
}

KonLoadCSVParser::~KonLoadCSVParser() { reset(); }

void KonLoadCSVParser::reset() {
  allocator_.reset();
  collation_type_ = CS_TYPE_INVALID;
  row_.reset();
}

int KonLoadCSVParser::init(const ObDataInFileStruct &format,
                           int64_t column_count,
                           ObCollationType collation_type) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(csv_parser_.init(format, column_count, collation_type))) {
    KON_LOG(WARN, "fail to init csv parser", KR(ret));
  } else {
    allocator_.set_tenant_id(MTL_ID());
    ObObj *objs = nullptr;
    if (OB_ISNULL(objs = static_cast<ObObj *>(
                      allocator_.alloc(sizeof(ObObj) * column_count)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      KON_LOG(WARN, "fail to alloc memory", KR(ret));
    } else {
      new (objs) ObObj[column_count];
      row_.cells_ = objs;
      row_.count_ = column_count;
      collation_type_ = collation_type;
    }
  }
  return ret;
}

int KonLoadCSVParser::get_next_row(KonLoadDataBuffer &buffer,
                                   const ObNewRow *&row, int64_t &pos) {
  int ret = OB_SUCCESS;
  row = nullptr;
  if (OB_UNLIKELY(buffer.empty())) {
    ret = OB_ITER_END;
  } else {
    const char *str = buffer.begin();
    const char *end = buffer.end();
    int64_t nrows = 0;
    if (OB_FAIL(csv_parser_.scan(str, end, nrows))) {
      KON_LOG(WARN, "fail to scan buffer", KR(ret), K(buffer));
    } else if (OB_UNLIKELY(0 == nrows)) {
      ret = OB_ITER_END;
    } else {
      int64_t len = str - buffer.begin();
      buffer.consume(len);
      pos += len;
      const ObIArray<KonCSVSpecialParser::FieldValue> &field_values_in_file =
          csv_parser_.get_fields_per_line();
      for (int64_t i = 0; i < row_.count_; ++i) {
        const KonCSVSpecialParser::FieldValue &str_v =
            field_values_in_file.at(i);
        ObObj &obj = row_.cells_[i];
        if (OB_UNLIKELY(str_v.is_null_)) {
          obj.set_null();
        } else {
          obj.set_string(ObVarcharType, ObString(str_v.len_, str_v.ptr_));
          obj.set_collation_type(collation_type_);
        }
      }
      row = &row_;
    }
  }
  return ret;
}

/**
 * KonLoadRowCaster
 */

KonLoadRowCaster::KonLoadRowCaster()
    : column_count_(0), collation_type_(CS_TYPE_INVALID),
      cast_allocator_(ObModIds::OB_SQL_LOAD_DATA) {}

KonLoadRowCaster::~KonLoadRowCaster() {}

int KonLoadRowCaster::init(
    const ObTableSchema *table_schema,
    const ObIArray<ObLoadDataStmt::FieldOrVarStruct> &field_or_var_list) {
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == table_schema || field_or_var_list.empty())) {
    ret = OB_INVALID_ARGUMENT;
    KON_LOG(WARN, "invalid args", KR(ret), KP(table_schema),
            K(field_or_var_list));
  } else if (OB_FAIL(OTTZ_MGR.get_tenant_tz(MTL_ID(),
                                            tz_info_.get_tz_map_wrap()))) {
    KON_LOG(WARN, "fail to get tenant time zone", KR(ret));
  } else if (OB_FAIL(init_column_schemas_and_idxs(table_schema,
                                                  field_or_var_list))) {
    KON_LOG(WARN, "fail to init column schemas and idxs", KR(ret));
  } else if (OB_FAIL(datum_row_.init(table_schema->get_column_count(),
                                     table_schema->get_rowkey_column_num()))) {
    KON_LOG(WARN, "fail to init datum row", KR(ret));
  } else {
    column_count_ = table_schema->get_column_count();
    collation_type_ = table_schema->get_collation_type();
    cast_allocator_.set_tenant_id(MTL_ID());
  }
  return ret;
}

int KonLoadRowCaster::init_column_schemas_and_idxs(
    const ObTableSchema *table_schema,
    const ObIArray<ObLoadDataStmt::FieldOrVarStruct> &field_or_var_list) {
  int ret = OB_SUCCESS;
  ObSEArray<ObColDesc, 32> column_descs;
  if (OB_FAIL(table_schema->get_column_ids(column_descs))) {
    KON_LOG(WARN, "fail to get column descs", KR(ret), KPC(table_schema));
  } else {
    bool found_column = true;
    for (int64_t i = 0;
         OB_SUCC(ret) && OB_LIKELY(found_column) && i < column_descs.count();
         ++i) {
      const ObColDesc &col_desc = column_descs.at(i);
      const ObColumnSchemaV2 *col_schema =
          table_schema->get_column_schema(col_desc.col_id_);
      if (OB_ISNULL(col_schema)) {
        ret = OB_ERR_UNEXPECTED;
        KON_LOG(WARN, "unexpected null column schema", KR(ret), K(col_desc));
      } else if (OB_UNLIKELY(col_schema->is_hidden())) {
        ret = OB_ERR_UNEXPECTED;
        KON_LOG(WARN, "unexpected hidden column", KR(ret), K(i),
                KPC(col_schema));
      } else if (OB_FAIL(column_schemas_.push_back(col_schema))) {
        KON_LOG(WARN, "fail to push back column schema", KR(ret));
      } else {
        found_column = false;
      }
      // find column in source data columns
      for (int64_t j = 0; OB_SUCC(ret) && OB_LIKELY(!found_column) &&
                          j < field_or_var_list.count();
           ++j) {
        const ObLoadDataStmt::FieldOrVarStruct &field_or_var_struct =
            field_or_var_list.at(j);
        if (col_desc.col_id_ == field_or_var_struct.column_id_) {
          found_column = true;
          if (OB_FAIL(column_idxs_.push_back(j))) {
            KON_LOG(WARN, "fail to push back column idx", KR(ret),
                    K(column_idxs_), K(i), K(col_desc), K(j),
                    K(field_or_var_struct));
          }
        }
      }
    }
    if (OB_SUCC(ret) && OB_UNLIKELY(!found_column)) {
      ret = OB_NOT_SUPPORTED;
      KON_LOG(WARN, "not supported incomplete column data", KR(ret),
              K(column_idxs_), K(column_descs), K(field_or_var_list));
    }
  }
  return ret;
}

int KonLoadRowCaster::get_casted_row(const ObNewRow &new_row,
                                     const KonLoadDatumRow *&datum_row) {
  int ret = OB_SUCCESS;
  cast_allocator_.reuse();
  for (int64_t i = 0; OB_SUCC(ret) && i < column_idxs_.count(); ++i) {
    int64_t column_idx = column_idxs_.at(i);
    const ObColumnSchemaV2 *column_schema = column_schemas_[i];
    const ObObj &src_obj = new_row.cells_[column_idx];
    ObObj &dest_obj = datum_row_.objs_[i];
    if (OB_FAIL(cast_obj(column_schema, src_obj, dest_obj))) {
      KON_LOG(WARN, "fail to cast obj", KR(ret), K(src_obj), K(new_row));
    }
  }
  if (OB_SUCC(ret)) {
    datum_row = &datum_row_;
  }
  return ret;
}

/**
 * KonLoadCSVSubReader
 */

KonLoadCSVSubReader::KonLoadCSVSubReader()
    : fd_(-1), buffer_(), csv_parser_(), scan_offset_(0), offset_(0), end_(0) {}

KonLoadCSVSubReader::~KonLoadCSVSubReader() { reset(); }

int KonLoadCSVSubReader::init(const ObString &filepath, int idx, int total,
                              const ObDataInFileStruct &format,
                              int64_t column_count,
                              common::ObCollationType collation_type) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_file(filepath, idx, total))) {
    KON_LOG(WARN, "fail to init csv file");
  } else if (OB_FAIL(init_csv_parser(format, column_count, collation_type))) {
    KON_LOG(WARN, "fail to init csv parser");
  }
  return ret;
}

int KonLoadCSVSubReader::init_file(const ObString &filepath, int idx,
                                   int total) {
  int ret = OB_SUCCESS;
  MEMSET(&ctx_, 0, sizeof(ctx_));
  if ((fd_ = ::open(filepath.ptr(), O_RDONLY | O_DIRECT)) < 0) {
    KON_LOG(WARN, "fail to open file", KR(ret));
  } else if (OB_FAIL(buffer_.create(BUFFER_SIZE))) {
    KON_LOG(WARN, "fail to create buffer", KR(ret));
  } else if (OB_UNLIKELY(io_setup(1, &ctx_) != 0)) {
    ret = OB_IO_ERROR;
    KON_LOG(WARN, "fail to setup libaio context");
  } else {
    struct stat st;
    stat(filepath.ptr(), &st);
    uint64_t filesize = st.st_size;

    int64_t start_pos = filesize / total * idx;
    end_ = (idx + 1 == total) ? filesize : filesize / total * (idx + 1);

    int64_t unalign_read_pos = idx == 0 ? start_pos : start_pos - 1;
    int64_t read_pos = lower_align(unalign_read_pos, DIO_ALIGN_SIZE);
    if (::pread(fd_, buffer_.begin(), BUFFER_SIZE, read_pos) == -1) {
      ret = OB_IO_ERROR;
      KON_LOG(WARN, "fail to do pread", KR(ret));
    } else {
      offset_ = read_pos + BUFFER_SIZE;
      scan_offset_ = unalign_read_pos;
      buffer_.consume(unalign_read_pos - read_pos);
      if (idx != 0) {
        for (; *buffer_.begin() != '\n'; buffer_.consume(1), scan_offset_++)
          ;
        buffer_.consume(1);
        scan_offset_++;
      }
      if (OB_FAIL(prefetch())) {
        KON_LOG(WARN, "fail to prefetch", KR(ret));
      }
    }
  }

  return ret;
}

int KonLoadCSVSubReader::init_csv_parser(
    const ObDataInFileStruct &format, int64_t column_count,
    common::ObCollationType collation_type) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(csv_parser_.init(format, column_count, collation_type))) {
    KON_LOG(WARN, "fail to init csv parser", KR(ret));
  }
  return ret;
}

int KonLoadCSVSubReader::prefetch() {
  int ret = OB_SUCCESS;
  struct iocb iocb;
  struct iocb *iocbs[1];
  io_prep_pread(&iocb, fd_, buffer_.next_buffer(), BUFFER_SIZE, offset_);
  iocbs[0] = &iocb;
  if (OB_UNLIKELY(io_submit(ctx_, 1, iocbs) != 1)) {
    ret = OB_IO_ERROR;
    KON_LOG(WARN, "fail to io submit");
  }
  return ret;
}

int KonLoadCSVSubReader::wait() {
  int ret = OB_SUCCESS;
  struct io_event events[1];
  if (OB_UNLIKELY(io_getevents(ctx_, 1, 1, events, NULL) != 1)) {
    ret = OB_IO_ERROR;
    KON_LOG(WARN, "fail to io_getevents");
  } else if (OB_UNLIKELY(events[0].res < 0)) {
    ret = OB_IO_ERROR;
    KON_LOG(WARN, "fail to aio pread");
  } else {
    int64_t read_size = events[0].res;
    offset_ += read_size;
    buffer_.switch_next(read_size);
  }
  return ret;
}

int KonLoadCSVSubReader::get_next_row(const common::ObNewRow *&row) {
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(scan_offset_ >= end_)) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(csv_parser_.get_next_row(buffer_, row, scan_offset_))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      KON_LOG(WARN, "fail to get next csv row", KR(ret));
    } else if (OB_FAIL(read_next_buffer())) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        KON_LOG(WARN, "fail to read next buffer", KR(ret));
      }
    } else if (OB_FAIL(csv_parser_.get_next_row(buffer_, row, scan_offset_))) {
      KON_LOG(WARN, "fail to get next csv row", KR(ret));
    }
  }
  return ret;
}

int KonLoadCSVSubReader::read_next_buffer() {
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(offset_ >= end_)) {
    if (buffer_.empty()) {
      ret = OB_ITER_END;
    } else {
      if (::pread(fd_, buffer_.next_buffer(), BUFFER_SIZE, offset_) == -1) {
        ret = OB_IO_ERROR;
        KON_LOG(WARN, "fail to pread", K_(offset), K(errno));
      } else {
        buffer_.switch_next(BUFFER_SIZE);
      }
    }
  } else if (OB_FAIL(wait())) {
    KON_LOG(WARN, "fail to wait", KR(ret));
  } else if (OB_FAIL(prefetch())) {
    KON_LOG(WARN, "fail to prefetch", KR(ret));
  }
  return ret;
}

void KonLoadCSVSubReader::reset() {
  if (fd_ != -1) {
    ::close(fd_);
    fd_ = -1;
  }
  io_destroy(ctx_);
  buffer_.reset();
  csv_parser_.reset();
  offset_ = 0;
  end_ = 0;
}

/**
 * KonLoadMacroBlockWriter
 */

KonLoadMacroBlockWriter::KonLoadMacroBlockWriter()
    : column_count_(0), rowkey_column_num_(0), extra_rowkey_column_num_(0) {}
KonLoadMacroBlockWriter::~KonLoadMacroBlockWriter() {}

int KonLoadMacroBlockWriter::init(int idx, KonLoadSSTableBuilder *builder) {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(builder)) {
    ret = OB_INVALID_ARGUMENT;
    KON_LOG(WARN, "invalid args", KR(ret), KP(builder));
  } else if (OB_FAIL(
                 builder->init_macro_block_writer(idx, &macro_block_writer_))) {
    KON_LOG(WARN, "fail to init macro block writer");
  } else if (OB_FAIL(builder->init_datum_row(&datum_row_))) {
    KON_LOG(WARN, "fail to init datum row");
  } else {
    const share::schema::ObTableSchema *table_schema =
        builder->get_table_schema();
    column_count_ = table_schema->get_column_count();
    rowkey_column_num_ = table_schema->get_rowkey_column_num();
    extra_rowkey_column_num_ =
        ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  }
  return ret;
}

int KonLoadMacroBlockWriter::append_row(const KonLoadDatumRow &datum_row) {
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < column_count_; ++i) {
    int64_t idx = i < rowkey_column_num_ ? i : i + extra_rowkey_column_num_;
    ObStorageDatum &datum = datum_row_.storage_datums_[idx];
    const common::ObObj &obj = datum_row.objs_[i];
    switch (obj.get_type()) {
    case common::ObIntType:
    case common::ObInt32Type: {
      datum.ptr_ = reinterpret_cast<const char *>(&obj.v_.int64_);
      break;
    }
    case common::ObDateType: {
      datum.ptr_ = reinterpret_cast<const char *>(&obj.v_.int64_);
      break;
    }
    case common::ObVarcharType:
    case common::ObCharType: {
      datum.ptr_ = obj.v_.string_;
      datum.pack_ = obj.val_len_;
      break;
    }
    case common::ObNumberType: {
      datum.ptr_ = reinterpret_cast<const char *>(obj.v_.nmb_digits_) -
                   sizeof(obj.nmb_desc_);
      datum.pack_ = sizeof(obj.nmb_desc_) +
                    sizeof(*obj.v_.nmb_digits_) * obj.nmb_desc_.len_;
      break;
    }
    default: {
      if (OB_FAIL(datum.from_obj(datum_row.objs_[i]))) {
        KON_LOG(WARN, "fail to from obj", KR(ret));
      }
    }
    }
  }
  if (OB_FAIL(macro_block_writer_.append_row(datum_row_))) {
    KON_LOG(WARN, "fail to append row", KR(ret));
  }
  return ret;
}

int KonLoadMacroBlockWriter::close() {
  int ret = OB_SUCCESS;
  if (OB_FAIL(macro_block_writer_.close())) {
    KON_LOG(WARN, "fail to close macro block writer", KR(ret));
  }
  return ret;
}

/**
 * KonLoadSSTableBuilder
 */

KonLoadSSTableBuilder::KonLoadSSTableBuilder()
    : rowkey_column_num_(0), extra_rowkey_column_num_(0), column_count_(0),
      is_closed_(false), is_inited_(false) {}
KonLoadSSTableBuilder::~KonLoadSSTableBuilder() {}

int KonLoadSSTableBuilder::init(
    const share::schema::ObTableSchema *table_schema) {
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    KON_LOG(WARN, "ObLoadSSTableWriter init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    KON_LOG(WARN, "invalid args", KR(ret), KP(table_schema));
  } else {
    table_schema_ = table_schema;
    tablet_id_ = table_schema->get_tablet_id();
    rowkey_column_num_ = table_schema->get_rowkey_column_num();
    extra_rowkey_column_num_ =
        ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
    column_count_ = table_schema->get_column_count();
    share::ObLocationService *location_service = nullptr;
    bool is_cache_hit = false;
    ObLSService *ls_service = nullptr;
    ObLS *ls = nullptr;
    if (OB_ISNULL(location_service = GCTX.location_service_)) {
      ret = OB_ERR_SYS;
      KON_LOG(WARN, "location service is null", KR(ret), KP(location_service));
    } else if (OB_FAIL(location_service->get(MTL_ID(), tablet_id_, INT64_MAX,
                                             is_cache_hit, ls_id_))) {
      KON_LOG(WARN, "fail to get ls id", KR(ret), K(tablet_id_));
    } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
      ret = OB_ERR_SYS;
      KON_LOG(WARN, "ls service is null", KR(ret));
    } else if (OB_FAIL(ls_service->get_ls(ls_id_, ls_handle_,
                                          ObLSGetMod::STORAGE_MOD))) {
      KON_LOG(WARN, "fail to get ls", KR(ret), K(ls_id_));
    } else if (OB_ISNULL(ls = ls_handle_.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      KON_LOG(WARN, "ls should not be null", KR(ret));
    } else if (OB_FAIL(ls->get_tablet(tablet_id_, tablet_handle_))) {
      KON_LOG(WARN, "fail to get tablet handle", KR(ret), K(tablet_id_));
    } else if (OB_FAIL(init_sstable_index_builder(table_schema))) {
      KON_LOG(WARN, "fail to init sstable index builder", KR(ret));
    } else if (OB_FAIL(data_store_desc_.init(*table_schema, ls_id_, tablet_id_,
                                             MAJOR_MERGE, 1))) {
      KON_LOG(WARN, "fail to init data desc", KR(ret));
    } else {
      data_store_desc_.sstable_index_builder_ = &sstable_index_builder_;
      table_key_.table_type_ = ObITable::MAJOR_SSTABLE;
      table_key_.tablet_id_ = tablet_id_;
      table_key_.log_ts_range_.start_log_ts_ = 0;
      table_key_.log_ts_range_.end_log_ts_ = ObTimeUtil::current_time_ns();
      is_inited_ = true;
    }
  }
  return ret;
}

int KonLoadSSTableBuilder::init_sstable_index_builder(
    const ObTableSchema *table_schema) {
  int ret = OB_SUCCESS;
  ObDataStoreDesc data_desc;
  if (OB_FAIL(
          data_desc.init(*table_schema, ls_id_, tablet_id_, MAJOR_MERGE, 1L))) {
    KON_LOG(WARN, "fail to init data desc", KR(ret));
  } else {
    data_desc.row_column_count_ = data_desc.rowkey_column_count_ + 1;
    data_desc.need_prebuild_bloomfilter_ = false;
    data_desc.col_desc_array_.reset();
    if (OB_FAIL(data_desc.col_desc_array_.init(data_desc.row_column_count_))) {
      KON_LOG(WARN, "fail to reserve column desc array", KR(ret));
    } else if (OB_FAIL(table_schema->get_rowkey_column_ids(
                   data_desc.col_desc_array_))) {
      KON_LOG(WARN, "fail to get rowkey column ids", KR(ret));
    } else if (OB_FAIL(ObMultiVersionRowkeyHelpper::add_extra_rowkey_cols(
                   data_desc.col_desc_array_))) {
      KON_LOG(WARN, "fail to add extra rowkey cols", KR(ret));
    } else {
      ObObjMeta meta;
      meta.set_varchar();
      meta.set_collation_type(CS_TYPE_BINARY);
      ObColDesc col;
      col.col_id_ = static_cast<uint64_t>(data_desc.row_column_count_ +
                                          OB_APP_MIN_COLUMN_ID);
      col.col_type_ = meta;
      col.col_order_ = DESC;
      if (OB_FAIL(data_desc.col_desc_array_.push_back(col))) {
        KON_LOG(WARN, "fail to push back last col for index", KR(ret), K(col));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(sstable_index_builder_.init(data_desc))) {
      KON_LOG(WARN, "fail to init index builder", KR(ret), K(data_desc));
    }
  }
  return ret;
}

int KonLoadSSTableBuilder::init_macro_block_writer(
    int idx, blocksstable::ObMacroBlockWriter *writer) {
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret)) {
    ObMacroDataSeq data_seq;
    data_seq.set_parallel_degree(idx);
    if (OB_FAIL(writer->open(data_store_desc_, data_seq))) {
      KON_LOG(WARN, "fail to init macro block writer", KR(ret),
              K(data_store_desc_), K(data_seq));
    }
  }
  return ret;
}

int KonLoadSSTableBuilder::init_datum_row(blocksstable::ObDatumRow *row) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(row->init(column_count_ + extra_rowkey_column_num_))) {
    KON_LOG(WARN, "fail to init datum row", KR(ret));
  } else {
    row->row_flag_.set_flag(ObDmlFlag::DF_INSERT);
    row->mvcc_row_flag_.set_last_multi_version_row(true);
    row->storage_datums_[rowkey_column_num_].set_int(-1); // fill trans_version
    row->storage_datums_[rowkey_column_num_ + 1].set_int(0); // fill sql_no
  }
  ObSEArray<ObColDesc, 32> column_descs;
  if (OB_FAIL(table_schema_->get_column_ids(column_descs))) {
    KON_LOG(WARN, "fail to get column descs", KR(ret), KPC(table_schema_));
  } else {
    for (int64_t i = 0; i < column_descs.count(); ++i) {
      int64_t idx = i < rowkey_column_num_ ? i : i + extra_rowkey_column_num_;
      const ObColDesc &col_desc = column_descs.at(i);
      const ObColumnSchemaV2 *col_schema =
          table_schema_->get_column_schema(col_desc.col_id_);
      switch (col_schema->get_meta_type().get_type()) {
      case common::ObIntType:
      case common::ObInt32Type: {
        row->storage_datums_[idx].pack_ = sizeof(uint64_t);
        break;
      }
      case common::ObDateType: {
        row->storage_datums_[idx].pack_ = sizeof(uint32_t);
        break;
      }
      default: {
      }
      }
    }
  }
  return ret;
}

int KonLoadSSTableBuilder::close() {
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    KON_LOG(WARN, "KonLoadSSTableBuilder not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    KON_LOG(WARN, "unexpected closed sstable writer", KR(ret));
  } else {
    ObSSTable *sstable = nullptr;
    if (OB_FAIL(create_sstable())) {
      KON_LOG(WARN, "fail to create sstable", KR(ret));
    } else {
      is_closed_ = true;
    }
  }
  return ret;
}

int KonLoadSSTableBuilder::create_sstable() {
  int ret = OB_SUCCESS;
  ObTableHandleV2 table_handle;
  SMART_VAR(ObSSTableMergeRes, merge_res) {
    const ObStorageSchema &storage_schema =
        tablet_handle_.get_obj()->get_storage_schema();
    int64_t column_count = 0;
    if (OB_FAIL(
            storage_schema.get_stored_column_count_in_sstable(column_count))) {
      KON_LOG(WARN, "fail to get stored column count in sstable", KR(ret));
    } else if (OB_FAIL(sstable_index_builder_.close(column_count, merge_res))) {
      KON_LOG(WARN, "fail to close sstable index builder", KR(ret));
    } else {
      ObTabletCreateSSTableParam create_param;
      create_param.table_key_ = table_key_;
      create_param.table_mode_ = storage_schema.get_table_mode_struct();
      create_param.index_type_ = storage_schema.get_index_type();
      create_param.rowkey_column_cnt_ =
          storage_schema.get_rowkey_column_num() +
          ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
      create_param.schema_version_ = storage_schema.get_schema_version();
      create_param.create_snapshot_version_ = 0;
      ObSSTableMergeRes::fill_addr_and_data(merge_res.root_desc_,
                                            create_param.root_block_addr_,
                                            create_param.root_block_data_);
      ObSSTableMergeRes::fill_addr_and_data(
          merge_res.data_root_desc_, create_param.data_block_macro_meta_addr_,
          create_param.data_block_macro_meta_);
      create_param.root_row_store_type_ = merge_res.root_desc_.row_type_;
      create_param.data_index_tree_height_ = merge_res.root_desc_.height_;
      create_param.index_blocks_cnt_ = merge_res.index_blocks_cnt_;
      create_param.data_blocks_cnt_ = merge_res.data_blocks_cnt_;
      create_param.micro_block_cnt_ = merge_res.micro_block_cnt_;
      create_param.use_old_macro_block_count_ =
          merge_res.use_old_macro_block_count_;
      create_param.row_count_ = merge_res.row_count_;
      create_param.column_cnt_ = merge_res.data_column_cnt_;
      create_param.data_checksum_ = merge_res.data_checksum_;
      create_param.occupy_size_ = merge_res.occupy_size_;
      create_param.original_size_ = merge_res.original_size_;
      create_param.max_merged_trans_version_ =
          merge_res.max_merged_trans_version_;
      create_param.contain_uncommitted_row_ =
          merge_res.contain_uncommitted_row_;
      create_param.compressor_type_ = merge_res.compressor_type_;
      create_param.encrypt_id_ = merge_res.encrypt_id_;
      create_param.master_key_id_ = merge_res.master_key_id_;
      create_param.data_block_ids_ = merge_res.data_block_ids_;
      create_param.other_block_ids_ = merge_res.other_block_ids_;
      MEMCPY(create_param.encrypt_key_, merge_res.encrypt_key_,
             share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH);
      if (OB_FAIL(merge_res.fill_column_checksum(
              &storage_schema, create_param.column_checksums_))) {
        KON_LOG(WARN, "fail to fill column checksum for empty major", KR(ret),
                K(create_param));
      } else if (OB_FAIL(ObTabletCreateDeleteHelper::create_sstable(
                     create_param, table_handle))) {
        KON_LOG(WARN, "fail to create sstable", KR(ret), K(create_param));
      } else {
        const int64_t rebuild_seq = ls_handle_.get_ls()->get_rebuild_seq();
        ObTabletHandle new_tablet_handle;
        ObUpdateTableStoreParam table_store_param(
            table_handle, tablet_handle_.get_obj()->get_snapshot_version(),
            false, &storage_schema, rebuild_seq, true, true);
        if (OB_FAIL(ls_handle_.get_ls()->update_tablet_table_store(
                tablet_id_, table_store_param, new_tablet_handle))) {
          KON_LOG(WARN, "fail to update tablet table store", KR(ret),
                  K(tablet_id_), K(table_store_param));
        }
      }
    }
  }
  return ret;
}

/**
 * KonLoadParallelSSTableBuilder
 */

KonLoadParallelSSTableBuilder::KonLoadParallelSSTableBuilder()
    : allocator_(ObModIds::OB_SQL_LOAD_DATA) {}
KonLoadParallelSSTableBuilder::~KonLoadParallelSSTableBuilder() {}

int KonLoadParallelSSTableBuilder::init(
    int thread_num, const share::schema::ObTableSchema *table_schema) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(builder_.init(table_schema))) {
    KON_LOG(WARN, "fail to init sstable builder");
  } else if (OB_FAIL(thread_pool_.set_thread_num(thread_num))) {
    KON_LOG(WARN, "fail to set thread num");
  } else {
    allocator_.set_tenant_id(MTL_ID());
    for (int i = 0; OB_SUCC(ret) && i < thread_num; ++i) {
      void *buf = NULL;
      KonLoadMacroBlockWriter *writer = NULL;
      if (OB_ISNULL(buf = allocator_.alloc(sizeof(KonLoadMacroBlockWriter)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        KON_LOG(WARN, "fail to allocate buf");
      } else if (OB_ISNULL(writer = new (buf) KonLoadMacroBlockWriter())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        KON_LOG(WARN, "fail to new KonLoadMacroBlockWriter");
      } else if (OB_FAIL(writer->init(i, &builder_))) {
        KON_LOG(WARN, "fail to init macro block writer");
      } else {
        writers_.push_back(writer);
      }
    }
  }

  return ret;
}

int KonLoadParallelSSTableBuilder::run(GetNextItemFunc get_next_item_func) {
  int ret = OB_SUCCESS;
  KonThreadPool::KonFuncType func = [&](uint64_t idx) -> int {
    int ret = OB_SUCCESS;
    KON_LOG(INFO, "Parallel Write SSTable Thread Start", K(idx));
    const RowType *item = NULL;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(get_next_item_func(idx, item))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        }
        KON_LOG(WARN, "fail to get next item");
      } else if (OB_FAIL(writers_[idx]->append_row(*item))) {
        KON_LOG(WARN, "fail to append row to macro buffer writer");
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(writers_[idx]->close())) {
        KON_LOG(WARN, "fail to close sstable writer");
      }
    }
    KON_LOG(INFO, "Parallel Write SSTable Thread Finish", K(idx));
    return ret;
  };
  thread_pool_.set_func(func);
  if (OB_FAIL(thread_pool_.execute())) {
    KON_LOG(WARN, "fail to parallel write sstable");
  } else if (OB_FAIL(builder_.close())) {
    KON_LOG(WARN, "fail to close sstable builder");
  }
  return ret;
}

/**
 * KonLoadDataDirect
 */

KonLoadDataDirect::KonLoadDataDirect()
    : allocator_(ObModIds::OB_SQL_LOAD_DATA) {}

KonLoadDataDirect::~KonLoadDataDirect() {}

int KonLoadDataDirect::execute(ObExecContext &ctx, ObLoadDataStmt &load_stmt) {
  int ret = OB_SUCCESS;
  KON_LOG(INFO, "Begin Load Direct");
  if (OB_FAIL(inner_init(load_stmt))) {
    KON_LOG(WARN, "fail to inner init", KR(ret));
  } else if (OB_FAIL(do_load())) {
    KON_LOG(WARN, "fail to do load", KR(ret));
  }
  return ret;
}

int KonLoadDataDirect::inner_init(ObLoadDataStmt &load_stmt) {
  int ret = OB_SUCCESS;
  const ObLoadArgument &load_args = load_stmt.get_load_arguments();
  const ObIArray<ObLoadDataStmt::FieldOrVarStruct> &field_or_var_list =
      load_stmt.get_field_or_var_list();
  const uint64_t tenant_id = load_args.tenant_id_;
  const uint64_t table_id = load_args.table_id_;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = nullptr;

  allocator_.set_tenant_id(MTL_ID());
  if (OB_FAIL(
          ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(
              tenant_id, schema_guard))) {
    KON_LOG(WARN, "fail to get tenant schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id,
                                                   table_schema))) {
    KON_LOG(WARN, "fail to get table schema", KR(ret), K(tenant_id),
            K(table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    KON_LOG(WARN, "table not exist", KR(ret), K(tenant_id), K(table_id));
  } else if (OB_UNLIKELY(table_schema->is_heap_table())) {
    ret = OB_NOT_SUPPORTED;
    KON_LOG(WARN, "not support heap table", KR(ret));
  }

  const int64_t rowkey_column_num = table_schema->get_rowkey_column_num();
  ObArray<ObColDesc> multi_version_column_descs;
  if (OB_FAIL(table_schema->get_multi_version_column_descs(
          multi_version_column_descs))) {
    KON_LOG(WARN, "fail to get multi version column descs", KR(ret));
  } else if (OB_FAIL(datum_utils_.init(multi_version_column_descs,
                                       rowkey_column_num, is_oracle_mode(),
                                       allocator_))) {
    KON_LOG(WARN, "fail to init datum utils", KR(ret));
  } else if (OB_FAIL(compare_.init(rowkey_column_num, table_schema))) {
    KON_LOG(WARN, "fail to init compare", KR(ret));
  } else if (OB_FAIL(sstable_builder_.init(SSTABLE_THREAD_NUM, table_schema))) {
    KON_LOG(WARN, "fail to init sstable builder");
  } else {
    for (int i = 0; OB_SUCC(ret) && i < MEMORY_SORT_THREAD_NUM; ++i) {
      void *buf = NULL;
      KonLoadCSVSubReader *reader = NULL;
      KonLoadRowCaster *caster = NULL;
      if (OB_ISNULL(buf = allocator_.alloc(sizeof(KonLoadCSVSubReader)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        KON_LOG(WARN, "fail to allocate buf");
      } else if (OB_ISNULL(reader = new (buf) KonLoadCSVSubReader())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        KON_LOG(WARN, "fail to new KonLoadCSVSubReader");
      } else if (OB_FAIL(reader->init(
                     load_args.full_file_path_, i, MEMORY_SORT_THREAD_NUM,
                     load_stmt.get_data_struct_in_file(),
                     field_or_var_list.count(), load_args.file_cs_type_))) {
        KON_LOG(WARN, "fail to init KonLoadCSVSubReader", KR(ret));
      } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(KonLoadRowCaster)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        KON_LOG(WARN, "fail to allocate buf");
      } else if (OB_ISNULL(caster = new (buf) KonLoadRowCaster())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        KON_LOG(WARN, "fail to new KonLoadRowCaster");
      } else if (OB_FAIL(caster->init(table_schema, field_or_var_list))) {
        KON_LOG(WARN, "fail to init KonLoadRowCaster");
      } else {
        readers_.push_back(reader);
        casters_.push_back(caster);
      }
    }
    for (int i = 0; OB_SUCC(ret) && i < SSTABLE_THREAD_NUM; ++i) {
      void *buf = NULL;
      MemorySortRound::FragmentIteratorList *iter_list = NULL;
      FragmentMerge *merger = NULL;
      if (OB_ISNULL(buf = allocator_.alloc(
                        sizeof(MemorySortRound::FragmentIteratorList)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        KON_LOG(WARN, "fail to allocate buf");
      } else if (OB_ISNULL(iter_list = new (buf)
                               MemorySortRound::FragmentIteratorList())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        KON_LOG(WARN, "fail to new FragmentIteratorList");
      } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(FragmentMerge)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        KON_LOG(WARN, "fail to allocate buf");
      } else if (OB_ISNULL(merger = new (buf) FragmentMerge())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        KON_LOG(WARN, "fail to new FragmentMerge");
      } else {
        iters_.push_back(iter_list);
        mergers_.push_back(merger);
      }
    }
  }

  return ret;
}

int KonLoadDataDirect::do_load() {
  int ret = OB_SUCCESS;

  MemorySortRound::GetNextItemFunc memory_round_next_item_func =
      [&](int idx, const RowType *&item) -> int {
    int ret = OB_SUCCESS;
    const common::ObNewRow *new_row = NULL;
    if (OB_FAIL(readers_[idx]->get_next_row(new_row))) {
      if (OB_ITER_END != ret) {
        KON_LOG(WARN, "fail to get next row from KonLoadCSVSubReader");
      }
    } else if (OB_FAIL(casters_[idx]->get_casted_row(*new_row, item))) {
      KON_LOG(WARN, "fail to cast row");
    }
    return ret;
  };

  if (OB_FAIL(memory_sort_round_.init(
          MEMORY_SORT_THREAD_NUM, MEM_BUFFER_SIZE, FILE_BUFFER_SIZE, &compare_,
          memory_round_next_item_func, casters_[0]->get_column_schemas()))) {
    KON_LOG(WARN, "fail to init memory sort round");
  } else if (OB_FAIL(memory_sort_round_.run())) {
    KON_LOG(WARN, "fail to run memory sort round");
  } else {
    memory_sort_round_.generate_partition(SSTABLE_THREAD_NUM);
    KonThreadPool partition_pool;
    partition_pool.set_thread_num(SSTABLE_THREAD_NUM);
    KonThreadPool::KonFuncType open_merger = [&](uint64_t idx) -> int {
      int ret = OB_SUCCESS;
      memory_sort_round_.get_partition_iters(idx, *iters_[idx]);
      if (OB_FAIL(mergers_[idx]->init(*iters_[idx], &compare_))) {
        KON_LOG(WARN, "fail to init merger");
      } else if (mergers_[idx]->open()) {
        KON_LOG(WARN, "fail to open merger");
      }
      return ret;
    };
    partition_pool.set_func(open_merger);
    if (OB_FAIL(partition_pool.execute())) {
      KON_LOG(WARN, "fail to open merger");
    }
  }

  for (KonLoadCSVSubReader *reader : readers_) {
    reader->reset();
  }

  if (OB_SUCC(ret)) {
    KonLoadParallelSSTableBuilder::GetNextItemFunc sstable_next_item_func =
        [&](int idx, const RowType *&item) -> int {
      return mergers_[idx]->get_next_item(item);
    };
    if (OB_FAIL(sstable_builder_.run(sstable_next_item_func))) {
      KON_LOG(WARN, "fail to run sstable builder");
    }
  }

  return ret;
}

} // namespace sql
} // namespace oceanbase
