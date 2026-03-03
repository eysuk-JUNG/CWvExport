#include "DuckDbProvider.h"

#include <algorithm>
#include <climits>
#include <cmath>
#include <cstdio>
#include <string>

namespace {
std::string quote_string_literal(const std::string &s) {
  std::string out;
  out.reserve(s.size() + 2);
  out.push_back('\'');
  for (char ch : s) {
    if (ch == '\'') {
      out.push_back('\'');
      out.push_back('\'');
    } else {
      out.push_back(ch);
    }
  }
  out.push_back('\'');
  return out;
}

std::string quote_identifier(const std::string &name) {
  std::string out;
  out.reserve(name.size() + 2);
  out.push_back('"');
  for (char ch : name) {
    if (ch == '"') {
      out.push_back('"');
      out.push_back('"');
    } else {
      out.push_back(ch);
    }
  }
  out.push_back('"');
  return out;
}

std::string quote_qualified_identifier(const std::string &name) {
  if (name.find('.') == std::string::npos) {
    return quote_identifier(name);
  }
  std::string out;
  size_t start = 0;
  while (start <= name.size()) {
    const size_t dot = name.find('.', start);
    const size_t len = (dot == std::string::npos) ? (name.size() - start) : (dot - start);
    if (len == 0) {
      return quote_identifier(name);
    }
    if (!out.empty()) {
      out.push_back('.');
    }
    out += quote_identifier(name.substr(start, len));
    if (dot == std::string::npos) {
      break;
    }
    start = dot + 1;
  }
  return out;
}

bool is_zero_u128(uint64_t upper, uint64_t lower) {
  return upper == 0 && lower == 0;
}

std::string u128_to_decimal_string(uint64_t upper, uint64_t lower) {
  if (is_zero_u128(upper, lower)) {
    return "0";
  }
  uint32_t limbs[4] = {
      static_cast<uint32_t>((upper >> 32) & 0xFFFFFFFFu),
      static_cast<uint32_t>(upper & 0xFFFFFFFFu),
      static_cast<uint32_t>((lower >> 32) & 0xFFFFFFFFu),
      static_cast<uint32_t>(lower & 0xFFFFFFFFu),
  };
  std::string digits;
  digits.reserve(40);

  while (limbs[0] != 0 || limbs[1] != 0 || limbs[2] != 0 || limbs[3] != 0) {
    uint64_t carry = 0;
    for (int i = 0; i < 4; ++i) {
      const uint64_t cur = (carry << 32) | static_cast<uint64_t>(limbs[i]);
      limbs[i] = static_cast<uint32_t>(cur / 10ULL);
      carry = cur % 10ULL;
    }
    digits.push_back(static_cast<char>('0' + carry));
  }
  std::reverse(digits.begin(), digits.end());
  return digits;
}

std::string hugeint_to_decimal_string(duckdb_hugeint value) {
  const bool negative = value.upper < 0;
  uint64_t upper_bits = static_cast<uint64_t>(value.upper);
  uint64_t lower_bits = value.lower;
  if (negative) {
    lower_bits = ~lower_bits + 1ULL;
    upper_bits = ~upper_bits + (lower_bits == 0 ? 1ULL : 0ULL);
  }
  std::string magnitude = u128_to_decimal_string(upper_bits, lower_bits);
  if (negative && magnitude != "0") {
    return "-" + magnitude;
  }
  return magnitude;
}

std::string uhugeint_to_decimal_string(duckdb_uhugeint value) {
  return u128_to_decimal_string(value.upper, value.lower);
}

std::string decimal_to_string(duckdb_decimal value) {
  std::string text = hugeint_to_decimal_string(value.value);
  if (value.scale == 0) {
    return text;
  }
  bool negative = false;
  if (!text.empty() && text[0] == '-') {
    negative = true;
    text.erase(text.begin());
  }

  const size_t scale = static_cast<size_t>(value.scale);
  if (text.size() <= scale) {
    std::string out = negative ? "-0." : "0.";
    out.append(scale - text.size(), '0');
    out += text;
    return out;
  }

  const size_t point_pos = text.size() - scale;
  std::string out = negative ? "-" : "";
  out += text.substr(0, point_pos);
  out.push_back('.');
  out += text.substr(point_pos);
  return out;
}

std::string format_date(duckdb_date value) {
  if (!duckdb_is_finite_date(value)) {
    return std::string();
  }
  const duckdb_date_struct d = duckdb_from_date(value);
  char buf[32];
  std::snprintf(buf, sizeof(buf), "%04d-%02d-%02d", d.year, static_cast<int>(d.month),
                static_cast<int>(d.day));
  return std::string(buf);
}

std::string format_time(duckdb_time value) {
  const duckdb_time_struct t = duckdb_from_time(value);
  char buf[48];
  std::snprintf(buf, sizeof(buf), "%02d:%02d:%02d.%06d", static_cast<int>(t.hour),
                static_cast<int>(t.min), static_cast<int>(t.sec), t.micros);
  return std::string(buf);
}

std::string format_timestamp(duckdb_timestamp value) {
  if (!duckdb_is_finite_timestamp(value)) {
    return std::string();
  }
  const duckdb_timestamp_struct ts = duckdb_from_timestamp(value);
  char buf[64];
  std::snprintf(buf, sizeof(buf), "%04d-%02d-%02d %02d:%02d:%02d.%06d",
                ts.date.year, static_cast<int>(ts.date.month), static_cast<int>(ts.date.day),
                static_cast<int>(ts.time.hour), static_cast<int>(ts.time.min),
                static_cast<int>(ts.time.sec), ts.time.micros);
  return std::string(buf);
}
} // namespace

DuckDbProvider::~DuckDbProvider() {
  ResetStatement();
  ResetQueryResult();
  if (conn_ != nullptr) {
    duckdb_disconnect(&conn_);
  }
  if (db_ != nullptr) {
    duckdb_close(&db_);
  }
}

void DuckDbProvider::RequestCancel() {
  if (conn_ != nullptr) {
    duckdb_interrupt(conn_);
  }
}

void DuckDbProvider::ResetStatement() {
  if (stmt_ != nullptr) {
    duckdb_destroy_prepare(&stmt_);
    stmt_ = nullptr;
  }
}

void DuckDbProvider::ResetQueryResult() {
  if (current_chunk_ != nullptr) {
    duckdb_destroy_data_chunk(&current_chunk_);
    current_chunk_ = nullptr;
  }
  if (has_result_) {
    duckdb_destroy_result(&result_);
    has_result_ = false;
  }
  current_chunk_row_ = 0;
  current_chunk_size_ = 0;
  row_active_ = false;
  col_types_.clear();
  row_cache_.clear();
}

std::string DuckDbProvider::QuoteIdentifier(const std::string &name) {
  return quote_identifier(name);
}

bool DuckDbProvider::OpenReadOnly(const std::string &path, std::string *err) {
  ResetStatement();
  ResetQueryResult();
  if (conn_ != nullptr) {
    duckdb_disconnect(&conn_);
    conn_ = nullptr;
  }
  if (db_ != nullptr) {
    duckdb_close(&db_);
    db_ = nullptr;
  }

  duckdb_config cfg = nullptr;
  if (duckdb_create_config(&cfg) != DuckDBSuccess) {
    *err = "duckdb_create_config failed.";
    return false;
  }
  if (duckdb_set_config(cfg, "access_mode", "read_only") != DuckDBSuccess) {
    duckdb_destroy_config(&cfg);
    *err = "duckdb_set_config(access_mode=read_only) failed.";
    return false;
  }

  char *open_err = nullptr;
  const duckdb_state open_rc = duckdb_open_ext(path.c_str(), &db_, cfg, &open_err);
  duckdb_destroy_config(&cfg);
  if (open_rc != DuckDBSuccess) {
    *err = "duckdb_open_ext failed.";
    if (open_err != nullptr) {
      *err += " ";
      *err += open_err;
      duckdb_free(open_err);
    }
    return false;
  }
  if (open_err != nullptr) {
    duckdb_free(open_err);
  }

  if (duckdb_connect(db_, &conn_) != DuckDBSuccess) {
    *err = "duckdb_connect failed.";
    duckdb_close(&db_);
    db_ = nullptr;
    return false;
  }
  return true;
}

bool DuckDbProvider::LoadTableSchema(const std::string &table_name,
                                     std::vector<DbColumnInfo> *cols,
                                     std::string *err) {
  duckdb_result schema_result{};
  const std::string sql =
      "PRAGMA table_info(" + quote_string_literal(table_name) + ");";
  if (duckdb_query(conn_, sql.c_str(), &schema_result) != DuckDBSuccess) {
    const char *msg = duckdb_result_error(&schema_result);
    *err = "duckdb PRAGMA table_info failed: ";
    *err += (msg != nullptr) ? msg : "unknown";
    duckdb_destroy_result(&schema_result);
    return false;
  }

  cols->clear();
  const idx_t rows = duckdb_row_count(&schema_result);
  for (idx_t r = 0; r < rows; ++r) {
    DbColumnInfo info;
    info.cid = static_cast<int>(duckdb_value_int32(&schema_result, 0, r));
    char *name = duckdb_value_varchar(&schema_result, 1, r);
    info.name = (name != nullptr) ? name : "";
    if (name != nullptr) {
      duckdb_free(name);
    }
    cols->push_back(info);
  }
  duckdb_destroy_result(&schema_result);

  if (cols->empty()) {
    *err = "Table not found or has no columns: " + table_name;
    return false;
  }
  return true;
}

bool DuckDbProvider::PrepareSelect(const std::string &table_name,
                                   const std::vector<ResolvedColumn> &cols,
                                   std::string *err) {
  ResetStatement();
  ResetQueryResult();

  std::string query = "SELECT ";
  for (size_t i = 0; i < cols.size(); ++i) {
    if (i > 0) {
      query += ", ";
    }
    query += QuoteIdentifier(cols[i].map.source_name);
  }
  query += " FROM ";
  query += quote_qualified_identifier(table_name);
  query += ";";

  if (duckdb_prepare(conn_, query.c_str(), &stmt_) != DuckDBSuccess) {
    const char *msg = duckdb_prepare_error(stmt_);
    *err = "duckdb_prepare failed: ";
    *err += (msg != nullptr) ? msg : "unknown";
    ResetStatement();
    return false;
  }

  if (duckdb_execute_prepared_streaming(stmt_, &result_) != DuckDBSuccess) {
    const char *msg = duckdb_result_error(&result_);
    *err = "duckdb_execute_prepared_streaming failed: ";
    *err += (msg != nullptr) ? msg : "unknown";
    duckdb_destroy_result(&result_);
    ResetStatement();
    return false;
  }
  // Prepared statement is no longer needed after result creation.
  ResetStatement();

  has_result_ = true;
  current_chunk_row_ = 0;
  current_chunk_size_ = 0;
  row_active_ = false;
  current_chunk_ = nullptr;

  const idx_t column_count = duckdb_column_count(&result_);
  col_types_.resize(static_cast<size_t>(column_count));
  row_cache_.assign(static_cast<size_t>(column_count), CachedCell{});
  for (idx_t c = 0; c < column_count; ++c) {
    col_types_[static_cast<size_t>(c)] = duckdb_column_type(&result_, c);
  }
  return true;
}

bool DuckDbProvider::Step(bool *has_row, std::string *err) {
  if (!has_result_) {
    *err = "duckdb Step called without active result.";
    return false;
  }

  while (true) {
    if (current_chunk_ != nullptr && current_chunk_row_ < current_chunk_size_) {
      if (!LoadCurrentRowCache(err)) {
        return false;
      }
      ++current_chunk_row_;
      row_active_ = true;
      *has_row = true;
      return true;
    }

    row_active_ = false;
    if (current_chunk_ != nullptr) {
      duckdb_destroy_data_chunk(&current_chunk_);
      current_chunk_ = nullptr;
    }

    duckdb_data_chunk next_chunk = duckdb_fetch_chunk(result_);
    if (next_chunk == nullptr) {
      const char *msg = duckdb_result_error(&result_);
      if (msg != nullptr && msg[0] != '\0') {
        *err = msg;
        return false;
      }
      *has_row = false;
      return true;
    }
    const idx_t next_size = duckdb_data_chunk_get_size(next_chunk);
    if (next_size == 0) {
      duckdb_destroy_data_chunk(&next_chunk);
      continue;
    }

    current_chunk_ = next_chunk;
    current_chunk_size_ = next_size;
    current_chunk_row_ = 0;
  }
}

ExportValueType DuckDbProvider::ValueType(int col) const {
  if (!row_active_ || col < 0 || static_cast<size_t>(col) >= row_cache_.size()) {
    return ExportValueType::Null;
  }
  return row_cache_[static_cast<size_t>(col)].type;
}

int64_t DuckDbProvider::ValueInt64(int col) const {
  if (!row_active_ || col < 0 || static_cast<size_t>(col) >= row_cache_.size()) {
    return 0;
  }
  return row_cache_[static_cast<size_t>(col)].v_int;
}

double DuckDbProvider::ValueDouble(int col) const {
  if (!row_active_ || col < 0 || static_cast<size_t>(col) >= row_cache_.size()) {
    return 0.0;
  }
  return row_cache_[static_cast<size_t>(col)].v_double;
}

const char *DuckDbProvider::ValueText(int col) const {
  if (!row_active_ || col < 0 || static_cast<size_t>(col) >= row_cache_.size()) {
    return "";
  }
  return row_cache_[static_cast<size_t>(col)].v_text.c_str();
}

int DuckDbProvider::ValueBytes(int col) const {
  if (!row_active_ || col < 0 || static_cast<size_t>(col) >= row_cache_.size()) {
    return 0;
  }
  return row_cache_[static_cast<size_t>(col)].blob_bytes;
}

bool DuckDbProvider::LoadCurrentRowCache(std::string *err) {
  if (current_chunk_ == nullptr || current_chunk_row_ >= current_chunk_size_) {
    *err = "duckdb internal row cursor is out of range.";
    return false;
  }
  const idx_t row_idx = current_chunk_row_;
  const idx_t col_count = duckdb_data_chunk_get_column_count(current_chunk_);
  if (col_count != static_cast<idx_t>(col_types_.size())) {
    *err = "duckdb column count changed unexpectedly during streaming.";
    return false;
  }

  for (idx_t c = 0; c < col_count; ++c) {
    CachedCell &cell = row_cache_[static_cast<size_t>(c)];
    cell = CachedCell{};

    duckdb_vector vec = duckdb_data_chunk_get_vector(current_chunk_, c);
    uint64_t *validity = duckdb_vector_get_validity(vec);
    if (validity != nullptr && !duckdb_validity_row_is_valid(validity, row_idx)) {
      cell.type = ExportValueType::Null;
      continue;
    }

    const duckdb_type t = col_types_[static_cast<size_t>(c)];
    void *data = duckdb_vector_get_data(vec);
    switch (t) {
    case DUCKDB_TYPE_BOOLEAN:
      cell.type = ExportValueType::Integer;
      cell.v_int = static_cast<int64_t>(static_cast<bool *>(data)[row_idx] ? 1 : 0);
      break;
    case DUCKDB_TYPE_TINYINT:
      cell.type = ExportValueType::Integer;
      cell.v_int = static_cast<int64_t>(static_cast<int8_t *>(data)[row_idx]);
      break;
    case DUCKDB_TYPE_SMALLINT:
      cell.type = ExportValueType::Integer;
      cell.v_int = static_cast<int64_t>(static_cast<int16_t *>(data)[row_idx]);
      break;
    case DUCKDB_TYPE_INTEGER:
      cell.type = ExportValueType::Integer;
      cell.v_int = static_cast<int64_t>(static_cast<int32_t *>(data)[row_idx]);
      break;
    case DUCKDB_TYPE_BIGINT:
      cell.type = ExportValueType::Integer;
      cell.v_int = static_cast<int64_t>(static_cast<int64_t *>(data)[row_idx]);
      break;
    case DUCKDB_TYPE_UTINYINT:
      cell.type = ExportValueType::Integer;
      cell.v_int = static_cast<int64_t>(static_cast<uint8_t *>(data)[row_idx]);
      break;
    case DUCKDB_TYPE_USMALLINT:
      cell.type = ExportValueType::Integer;
      cell.v_int = static_cast<int64_t>(static_cast<uint16_t *>(data)[row_idx]);
      break;
    case DUCKDB_TYPE_UINTEGER:
      cell.type = ExportValueType::Integer;
      cell.v_int = static_cast<int64_t>(static_cast<uint32_t *>(data)[row_idx]);
      break;
    case DUCKDB_TYPE_UBIGINT: {
      const uint64_t v = static_cast<uint64_t *>(data)[row_idx];
      if (v > static_cast<uint64_t>(INT64_MAX)) {
        cell.type = ExportValueType::Text;
        cell.v_text = std::to_string(v);
        cell.blob_bytes = static_cast<int>(cell.v_text.size());
      } else {
        cell.type = ExportValueType::Integer;
        cell.v_int = static_cast<int64_t>(v);
      }
      break;
    }
    case DUCKDB_TYPE_FLOAT:
      cell.type = ExportValueType::Float;
      cell.v_double = static_cast<double>(static_cast<float *>(data)[row_idx]);
      break;
    case DUCKDB_TYPE_DOUBLE:
      cell.type = ExportValueType::Float;
      cell.v_double = static_cast<double *>(data)[row_idx];
      break;
    case DUCKDB_TYPE_DECIMAL:
      cell.type = ExportValueType::Text;
      cell.v_text = decimal_to_string(static_cast<duckdb_decimal *>(data)[row_idx]);
      cell.blob_bytes = static_cast<int>(cell.v_text.size());
      break;
    case DUCKDB_TYPE_DATE: {
      cell.type = ExportValueType::Text;
      const duckdb_date v = static_cast<duckdb_date *>(data)[row_idx];
      cell.v_text = format_date(v);
      cell.blob_bytes = static_cast<int>(cell.v_text.size());
      if (cell.v_text.empty()) {
        cell.type = ExportValueType::Null;
      }
      break;
    }
    case DUCKDB_TYPE_TIME: {
      cell.type = ExportValueType::Text;
      const duckdb_time v = static_cast<duckdb_time *>(data)[row_idx];
      cell.v_text = format_time(v);
      cell.blob_bytes = static_cast<int>(cell.v_text.size());
      break;
    }
    case DUCKDB_TYPE_TIME_TZ: {
      cell.type = ExportValueType::Text;
      const duckdb_time_tz v = static_cast<duckdb_time_tz *>(data)[row_idx];
      const duckdb_time_tz_struct tz = duckdb_from_time_tz(v);
      char buf[80];
      std::snprintf(buf, sizeof(buf), "%02d:%02d:%02d.%06d[tz_offset=%d]",
                    static_cast<int>(tz.time.hour), static_cast<int>(tz.time.min),
                    static_cast<int>(tz.time.sec), tz.time.micros, tz.offset);
      cell.v_text = buf;
      cell.blob_bytes = static_cast<int>(cell.v_text.size());
      break;
    }
    case DUCKDB_TYPE_TIMESTAMP:
    case DUCKDB_TYPE_TIMESTAMP_TZ: {
      cell.type = ExportValueType::Text;
      const duckdb_timestamp v = static_cast<duckdb_timestamp *>(data)[row_idx];
      cell.v_text = format_timestamp(v);
      cell.blob_bytes = static_cast<int>(cell.v_text.size());
      if (cell.v_text.empty()) {
        cell.type = ExportValueType::Null;
      }
      break;
    }
    case DUCKDB_TYPE_TIMESTAMP_S: {
      cell.type = ExportValueType::Text;
      const duckdb_timestamp_s v = static_cast<duckdb_timestamp_s *>(data)[row_idx];
      if (duckdb_is_finite_timestamp_s(v)) {
        if (v.seconds <= (LLONG_MAX / 1000000LL) && v.seconds >= (LLONG_MIN / 1000000LL)) {
          duckdb_timestamp ts{};
          ts.micros = v.seconds * 1000000LL;
          cell.v_text = format_timestamp(ts);
        }
      }
      cell.blob_bytes = static_cast<int>(cell.v_text.size());
      if (cell.v_text.empty()) {
        cell.type = ExportValueType::Null;
      }
      break;
    }
    case DUCKDB_TYPE_TIMESTAMP_MS: {
      cell.type = ExportValueType::Text;
      const duckdb_timestamp_ms v = static_cast<duckdb_timestamp_ms *>(data)[row_idx];
      if (duckdb_is_finite_timestamp_ms(v)) {
        if (v.millis <= (LLONG_MAX / 1000LL) && v.millis >= (LLONG_MIN / 1000LL)) {
          duckdb_timestamp ts{};
          ts.micros = v.millis * 1000LL;
          cell.v_text = format_timestamp(ts);
        }
      }
      cell.blob_bytes = static_cast<int>(cell.v_text.size());
      if (cell.v_text.empty()) {
        cell.type = ExportValueType::Null;
      }
      break;
    }
    case DUCKDB_TYPE_TIMESTAMP_NS: {
      cell.type = ExportValueType::Text;
      const duckdb_timestamp_ns v = static_cast<duckdb_timestamp_ns *>(data)[row_idx];
      if (duckdb_is_finite_timestamp_ns(v)) {
        duckdb_timestamp ts{};
        ts.micros = v.nanos / 1000LL;
        cell.v_text = format_timestamp(ts);
      }
      cell.blob_bytes = static_cast<int>(cell.v_text.size());
      if (cell.v_text.empty()) {
        cell.type = ExportValueType::Null;
      }
      break;
    }
    case DUCKDB_TYPE_HUGEINT:
      cell.type = ExportValueType::Text;
      cell.v_text = hugeint_to_decimal_string(static_cast<duckdb_hugeint *>(data)[row_idx]);
      cell.blob_bytes = static_cast<int>(cell.v_text.size());
      break;
    case DUCKDB_TYPE_UHUGEINT:
      cell.type = ExportValueType::Text;
      cell.v_text = uhugeint_to_decimal_string(static_cast<duckdb_uhugeint *>(data)[row_idx]);
      cell.blob_bytes = static_cast<int>(cell.v_text.size());
      break;
    case DUCKDB_TYPE_BLOB: {
      cell.type = ExportValueType::Blob;
      duckdb_string_t *arr = static_cast<duckdb_string_t *>(data);
      duckdb_string_t *v = &arr[row_idx];
      const uint32_t len = duckdb_string_t_length(*v);
      cell.blob_bytes = static_cast<int>(len);
      break;
    }
    case DUCKDB_TYPE_VARCHAR: {
      cell.type = ExportValueType::Text;
      duckdb_string_t *arr = static_cast<duckdb_string_t *>(data);
      duckdb_string_t *v = &arr[row_idx];
      const uint32_t len = duckdb_string_t_length(*v);
      const char *ptr = duckdb_string_t_data(v);
      if (ptr != nullptr && len > 0) {
        cell.v_text.assign(ptr, ptr + len);
      }
      cell.blob_bytes = static_cast<int>(len);
      break;
    }
    default:
      cell.type = ExportValueType::Text;
      cell.v_text = "[UNSUPPORTED DUCKDB TYPE]";
      cell.blob_bytes = static_cast<int>(cell.v_text.size());
      break;
    }
  }
  return true;
}
