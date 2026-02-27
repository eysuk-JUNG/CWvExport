#include "DuckDbProvider.h"

#include <cstring>
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
} // namespace

DuckDbProvider::~DuckDbProvider() {
  ResetQueryResult();
  if (conn_ != nullptr) {
    duckdb_disconnect(&conn_);
  }
  if (db_ != nullptr) {
    duckdb_close(&db_);
  }
}

void DuckDbProvider::ResetQueryResult() {
  if (has_result_) {
    duckdb_destroy_result(&result_);
    has_result_ = false;
  }
  current_row_ = 0;
  row_active_ = false;
  text_buf_.clear();
}

std::string DuckDbProvider::QuoteIdentifier(const std::string &name) {
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

bool DuckDbProvider::OpenReadOnly(const std::string &path, std::string *err) {
  ResetQueryResult();
  if (conn_ != nullptr) {
    duckdb_disconnect(&conn_);
    conn_ = nullptr;
  }
  if (db_ != nullptr) {
    duckdb_close(&db_);
    db_ = nullptr;
  }

  if (duckdb_open(path.c_str(), &db_) != DuckDBSuccess) {
    *err = "duckdb_open failed.";
    return false;
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
  ResetQueryResult();

  std::string query = "SELECT ";
  for (size_t i = 0; i < cols.size(); ++i) {
    if (i > 0) {
      query += ", ";
    }
    query += QuoteIdentifier(cols[i].map.source_name);
  }
  query += " FROM ";
  query += QuoteIdentifier(table_name);
  query += ";";

  if (duckdb_query(conn_, query.c_str(), &result_) != DuckDBSuccess) {
    const char *msg = duckdb_result_error(&result_);
    *err = "duckdb SELECT failed: ";
    *err += (msg != nullptr) ? msg : "unknown";
    duckdb_destroy_result(&result_);
    return false;
  }
  has_result_ = true;
  current_row_ = 0;
  row_active_ = false;
  return true;
}

bool DuckDbProvider::Step(bool *has_row, std::string *err) {
  if (!has_result_) {
    *err = "duckdb Step called without active result.";
    return false;
  }

  if (row_active_) {
    ++current_row_;
    row_active_ = false;
  }

  const idx_t total = duckdb_row_count(&result_);
  if (current_row_ < total) {
    *has_row = true;
    row_active_ = true;
    return true;
  }
  *has_row = false;
  return true;
}

ExportValueType DuckDbProvider::ValueType(int col) const {
  if (!has_result_) {
    return ExportValueType::Null;
  }
  duckdb_result *res = const_cast<duckdb_result *>(&result_);
  const idx_t row = current_row_;
  if (duckdb_value_is_null(res, static_cast<idx_t>(col), row)) {
    return ExportValueType::Null;
  }

  const duckdb_type t = duckdb_column_type(res, static_cast<idx_t>(col));
  switch (t) {
  case DUCKDB_TYPE_BOOLEAN:
  case DUCKDB_TYPE_TINYINT:
  case DUCKDB_TYPE_SMALLINT:
  case DUCKDB_TYPE_INTEGER:
  case DUCKDB_TYPE_BIGINT:
  case DUCKDB_TYPE_UTINYINT:
  case DUCKDB_TYPE_USMALLINT:
  case DUCKDB_TYPE_UINTEGER:
  case DUCKDB_TYPE_UBIGINT:
    return ExportValueType::Integer;
  case DUCKDB_TYPE_FLOAT:
  case DUCKDB_TYPE_DOUBLE:
  case DUCKDB_TYPE_DECIMAL:
    return ExportValueType::Float;
  case DUCKDB_TYPE_BLOB:
    return ExportValueType::Blob;
  default:
    return ExportValueType::Text;
  }
}

int64_t DuckDbProvider::ValueInt64(int col) const {
  duckdb_result *res = const_cast<duckdb_result *>(&result_);
  return duckdb_value_int64(res, static_cast<idx_t>(col), current_row_);
}

double DuckDbProvider::ValueDouble(int col) const {
  duckdb_result *res = const_cast<duckdb_result *>(&result_);
  return duckdb_value_double(res, static_cast<idx_t>(col), current_row_);
}

const char *DuckDbProvider::ValueText(int col) const {
  duckdb_result *res = const_cast<duckdb_result *>(&result_);
  char *value = duckdb_value_varchar(res, static_cast<idx_t>(col), current_row_);
  if (value == nullptr) {
    text_buf_.clear();
    return "";
  }
  text_buf_ = value;
  duckdb_free(value);
  return text_buf_.c_str();
}

int DuckDbProvider::ValueBytes(int col) const {
  if (!has_result_) {
    return 0;
  }
  duckdb_result *res = const_cast<duckdb_result *>(&result_);
  if (ValueType(col) == ExportValueType::Blob) {
    duckdb_blob b = duckdb_value_blob(res, static_cast<idx_t>(col), current_row_);
    int n = static_cast<int>(b.size);
    if (b.data != nullptr) {
      duckdb_free(b.data);
    }
    return n;
  }

  char *value = duckdb_value_varchar(res, static_cast<idx_t>(col), current_row_);
  if (value == nullptr) {
    return 0;
  }
  const int n = static_cast<int>(std::strlen(value));
  duckdb_free(value);
  return n;
}
