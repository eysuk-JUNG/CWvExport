#include "SqliteProvider.h"

#include <string>

namespace {
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
} // namespace

SqliteProvider::~SqliteProvider() {
  if (stmt_ != nullptr) {
    sqlite3_finalize(stmt_);
  }
  if (db_ != nullptr) {
    sqlite3_close(db_);
  }
}

void SqliteProvider::RequestCancel() {
  if (db_ != nullptr) {
    sqlite3_interrupt(db_);
  }
}

bool SqliteProvider::OpenReadOnly(const std::string &path, std::string *err) {
  if (stmt_ != nullptr) {
    sqlite3_finalize(stmt_);
    stmt_ = nullptr;
  }
  if (db_ != nullptr) {
    sqlite3_close(db_);
    db_ = nullptr;
  }

  int rc = sqlite3_open_v2(path.c_str(), &db_, SQLITE_OPEN_READONLY, nullptr);
  if (rc != SQLITE_OK) {
    const char *msg = (db_ != nullptr) ? sqlite3_errmsg(db_) : "unknown error";
    *err = "sqlite3_open_v2 failed: ";
    *err += msg;
    if (db_ != nullptr) {
      sqlite3_close(db_);
      db_ = nullptr;
    }
    return false;
  }
  return true;
}

bool SqliteProvider::LoadTableSchema(const std::string &table_name,
                                     std::vector<DbColumnInfo> *cols,
                                     std::string *err) {
  sqlite3_stmt *s = nullptr;
  std::string sql = "PRAGMA table_info(" + quote_qualified_identifier(table_name) + ");";
  int rc = sqlite3_prepare_v2(db_, sql.c_str(), -1, &s, nullptr);
  if (rc != SQLITE_OK) {
    *err = "PRAGMA table_info prepare failed: ";
    *err += sqlite3_errmsg(db_);
    return false;
  }
  cols->clear();
  while ((rc = sqlite3_step(s)) == SQLITE_ROW) {
    DbColumnInfo info;
    info.cid = sqlite3_column_int(s, 0);
    const unsigned char *name = sqlite3_column_text(s, 1);
    info.name = name ? (const char *)name : "";
    cols->push_back(info);
  }
  sqlite3_finalize(s);
  if (rc != SQLITE_DONE) {
    *err = "PRAGMA table_info step failed.";
    return false;
  }
  if (cols->empty()) {
    *err = "Table not found or has no columns: " + table_name;
    return false;
  }
  return true;
}

bool SqliteProvider::PrepareSelect(const std::string &table_name,
                                   const std::vector<ResolvedColumn> &cols,
                                   std::string *err) {
  if (stmt_ != nullptr) {
    sqlite3_finalize(stmt_);
    stmt_ = nullptr;
  }

  std::string query = "SELECT ";
  for (size_t i = 0; i < cols.size(); ++i) {
    if (i > 0) {
      query += ", ";
    }
    query += quote_identifier(cols[i].map.source_name);
  }
  query += " FROM ";
  query += quote_qualified_identifier(table_name);
  query += ";";

  int rc = sqlite3_prepare_v2(db_, query.c_str(), -1, &stmt_, nullptr);
  if (rc != SQLITE_OK) {
    *err = "SELECT prepare failed: ";
    *err += sqlite3_errmsg(db_);
    return false;
  }
  return true;
}

bool SqliteProvider::Step(bool *has_row, std::string *err) {
  if (stmt_ == nullptr) {
    *err = "sqlite3_step called without prepared statement.";
    return false;
  }
  int rc = sqlite3_step(stmt_);
  if (rc == SQLITE_ROW) {
    *has_row = true;
    return true;
  }
  if (rc == SQLITE_DONE) {
    *has_row = false;
    return true;
  }
  if (rc == SQLITE_INTERRUPT) {
    *err = "export canceled by user.";
    return false;
  }
  *err = "sqlite3_step failed during export. rc=" + std::to_string(rc) + " msg=" +
         ((db_ != nullptr) ? sqlite3_errmsg(db_) : "unknown");
  return false;
}

ExportValueType SqliteProvider::ValueType(int col) const {
  if (stmt_ == nullptr) {
    return ExportValueType::Null;
  }
  int t = sqlite3_column_type(stmt_, col);
  if (t == SQLITE_NULL) {
    return ExportValueType::Null;
  }
  if (t == SQLITE_INTEGER) {
    return ExportValueType::Integer;
  }
  if (t == SQLITE_FLOAT) {
    return ExportValueType::Float;
  }
  if (t == SQLITE_BLOB) {
    return ExportValueType::Blob;
  }
  return ExportValueType::Text;
}

int64_t SqliteProvider::ValueInt64(int col) const {
  if (stmt_ == nullptr) {
    return 0;
  }
  return sqlite3_column_int64(stmt_, col);
}

double SqliteProvider::ValueDouble(int col) const {
  if (stmt_ == nullptr) {
    return 0.0;
  }
  return sqlite3_column_double(stmt_, col);
}

const char *SqliteProvider::ValueText(int col) const {
  if (stmt_ == nullptr) {
    return "";
  }
  const unsigned char *v = sqlite3_column_text(stmt_, col);
  return v ? (const char *)v : "";
}

int SqliteProvider::ValueBytes(int col) const {
  if (stmt_ == nullptr) {
    return 0;
  }
  return sqlite3_column_bytes(stmt_, col);
}
