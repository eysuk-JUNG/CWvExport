#include "CWvExport.h"

#include "cxlsx/cxlsx.h"

#include <duckdb.h>
#include <sqlite3.h>

#include <algorithm>
#include <atomic>
#include <cstdio>
#include <cstring>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <mutex>
#include <unordered_map>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>
#if defined(_WIN32)
#include <windows.h>
#endif

namespace {
constexpr const char *kDefaultDbPath = "test_export_cache.db";
constexpr const char *kDefaultDuckDbPath = "test_export_cache.duckdb";
constexpr const char *kDefaultXlsxPath = "test_export_result.xlsx";
constexpr int kSeedRowCount = 10000;
constexpr int kCancelSeedRowCount = 300000;
const char *kTestLicenseKey =
    "VEVTVC1VU0VSADIwOTktMTItMzEAEL3Siqtci7iNq5i3b6OLcDfLb0jTwOpUr148vD"
    "NQkRCqlqFh0udB635MvlpP6QWmjXM/cpJkG+zCeESDlxjyvSbEUHdgfsnTgHaiidrI"
    "XB/on9qJ6XcN0fB71pQUTQCismEH8YFYlVV/jFqXMfEXU9WRUdrWK9GH7VYaaKV4ih"
    "XkGD2D18USSCXPllbj0mt1Y7Bn3dSjRdsTOt+SEv6YTUIDLiTMHmNlhz2sTVfjwOkH"
    "vSrjvQNfvhfOtOa+tnnKgP/wrm7pkED+S8UuJ8oKXNPhCCz1MJfjO8one3oxX2ukMd"
    "Q0TZHJjPjLrFPDB7usMT6boOSoB+SEvc+UCHJNWg==";

int create_seed_sqlite_db(const char *db_path, int row_count) {
  sqlite3 *db = nullptr;
  sqlite3_stmt *stmt = nullptr;
  if (sqlite3_open(db_path, &db) != SQLITE_OK) {
    sqlite3_close(db);
    return -1;
  }
  sqlite3_exec(db,
               "CREATE TABLE IF NOT EXISTS sample_data ("
               "id INTEGER PRIMARY KEY, name TEXT, sql_text TEXT, score INTEGER, "
               "created_at TEXT);",
               nullptr, nullptr, nullptr);
  sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM sample_data;", -1, &stmt, nullptr);
  int count = 0;
  if (sqlite3_step(stmt) == SQLITE_ROW) {
    count = sqlite3_column_int(stmt, 0);
  }
  sqlite3_finalize(stmt);
  if (count >= row_count) {
    sqlite3_close(db);
    return 0;
  }

  sqlite3_exec(db, "BEGIN IMMEDIATE TRANSACTION;", nullptr, nullptr, nullptr);
  sqlite3_prepare_v2(db,
                     "INSERT INTO sample_data (id,name,sql_text,score,created_at) "
                     "VALUES (?,?,?,?,?);",
                     -1, &stmt, nullptr);
  for (int i = count + 1; i <= row_count; ++i) {
    char name[64], sql[128], ts[32];
    snprintf(name, sizeof(name), "user_%05d", i);
    snprintf(sql, sizeof(sql), "SELECT * FROM sample_data WHERE id=%d;", i);
    snprintf(ts, sizeof(ts), "2026-02-%02d 10:%02d:%02d", 1 + (i % 28), i % 60,
             (i * 7) % 60);
    sqlite3_bind_int(stmt, 1, i);
    sqlite3_bind_text(stmt, 2, name, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 3, sql, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int(stmt, 4, i % 101);
    sqlite3_bind_text(stmt, 5, ts, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(stmt) != SQLITE_DONE) {
      sqlite3_finalize(stmt);
      sqlite3_exec(db, "ROLLBACK;", nullptr, nullptr, nullptr);
      sqlite3_close(db);
      return -1;
    }
    sqlite3_reset(stmt);
    sqlite3_clear_bindings(stmt);
  }
  sqlite3_finalize(stmt);
  sqlite3_exec(db, "COMMIT;", nullptr, nullptr, nullptr);
  sqlite3_close(db);
  return 0;
}

int get_sqlite_row_count(const char *db_path, int *out_rows) {
  sqlite3 *db = nullptr;
  sqlite3_stmt *stmt = nullptr;
  if (sqlite3_open(db_path, &db) != SQLITE_OK) {
    sqlite3_close(db);
    return -1;
  }
  if (sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM sample_data;", -1, &stmt, nullptr) !=
      SQLITE_OK) {
    sqlite3_close(db);
    return -1;
  }
  int rows = -1;
  if (sqlite3_step(stmt) == SQLITE_ROW) {
    rows = sqlite3_column_int(stmt, 0);
  }
  sqlite3_finalize(stmt);
  sqlite3_close(db);
  if (rows < 0) {
    return -1;
  }
  *out_rows = rows;
  return 0;
}

bool duckdb_exec(duckdb_connection conn, const char *sql, std::string *err) {
  duckdb_result r{};
  if (duckdb_query(conn, sql, &r) != DuckDBSuccess) {
    const char *msg = duckdb_result_error(&r);
    *err = msg ? msg : "duckdb query failed";
    duckdb_destroy_result(&r);
    return false;
  }
  duckdb_destroy_result(&r);
  return true;
}

std::string sql_escape_single_quoted(const std::string &value) {
  std::string out;
  out.reserve(value.size() + 8);
  for (char ch : value) {
    if (ch == '\'') {
      out.push_back('\'');
      out.push_back('\'');
    } else {
      out.push_back(ch);
    }
  }
  return out;
}

int create_seed_duckdb_db(const char *db_path, int row_count) {
  duckdb_database db = nullptr;
  duckdb_connection conn = nullptr;
  std::string err;
  if (duckdb_open(db_path, &db) != DuckDBSuccess) {
    return -1;
  }
  if (duckdb_connect(db, &conn) != DuckDBSuccess) {
    duckdb_close(&db);
    return -1;
  }

  if (!duckdb_exec(conn,
                   "CREATE TABLE IF NOT EXISTS sample_data ("
                   "id BIGINT, name VARCHAR, sql_text VARCHAR, score BIGINT, "
                   "created_at VARCHAR);",
                   &err)) {
    duckdb_disconnect(&conn);
    duckdb_close(&db);
    return -1;
  }

  duckdb_result count_result{};
  int existing = 0;
  if (duckdb_query(conn, "SELECT COUNT(*) FROM sample_data;", &count_result) ==
      DuckDBSuccess) {
    existing = static_cast<int>(duckdb_value_int64(&count_result, 0, 0));
  }
  duckdb_destroy_result(&count_result);
  if (existing >= row_count) {
    duckdb_disconnect(&conn);
    duckdb_close(&db);
    return 0;
  }

  if (!duckdb_exec(conn, "BEGIN TRANSACTION;", &err)) {
    duckdb_disconnect(&conn);
    duckdb_close(&db);
    return -1;
  }
  for (int i = existing + 1; i <= row_count; ++i) {
    char sql[512];
    snprintf(sql, sizeof(sql),
             "INSERT INTO sample_data VALUES (%d, 'user_%05d', "
             "'SELECT * FROM sample_data WHERE id=%d;', %d, '2026-02-%02d "
             "10:%02d:%02d');",
             i, i, i, i % 101, 1 + (i % 28), i % 60, (i * 7) % 60);
    if (!duckdb_exec(conn, sql, &err)) {
      duckdb_exec(conn, "ROLLBACK;", &err);
      duckdb_disconnect(&conn);
      duckdb_close(&db);
      return -1;
    }
  }
  if (!duckdb_exec(conn, "COMMIT;", &err)) {
    duckdb_disconnect(&conn);
    duckdb_close(&db);
    return -1;
  }

  duckdb_disconnect(&conn);
  duckdb_close(&db);
  return 0;
}

int create_special_sqlite_table(const char *db_path) {
  sqlite3 *db = nullptr;
  sqlite3_stmt *stmt = nullptr;
  if (sqlite3_open(db_path, &db) != SQLITE_OK) {
    sqlite3_close(db);
    return -1;
  }
  const char *ddl =
      "CREATE TABLE IF NOT EXISTS sample_data_special ("
      "id INTEGER PRIMARY KEY, name TEXT, sql_text TEXT, score INTEGER, created_at TEXT);";
  if (sqlite3_exec(db, ddl, nullptr, nullptr, nullptr) != SQLITE_OK) {
    sqlite3_close(db);
    return -1;
  }
  if (sqlite3_exec(db, "DELETE FROM sample_data_special;", nullptr, nullptr, nullptr) !=
      SQLITE_OK) {
    sqlite3_close(db);
    return -1;
  }

  if (sqlite3_prepare_v2(db,
                         "INSERT INTO sample_data_special "
                         "(id,name,sql_text,score,created_at) VALUES (?,?,?,?,?);",
                         -1, &stmt, nullptr) != SQLITE_OK) {
    sqlite3_close(db);
    return -1;
  }

  struct Row {
    int id;
    const char *name;
    const char *sql;
    int score;
  };
  const Row rows[] = {
      {1, u8"\uD55C\uAE00 \"\uB530\uC634\uD45C\", \uC27C\uD45C", "SELECT \"A\", 'B', 1,\n2", 7},
      {2, u8"\u4E2D\u6587,\u9017\u53F7", "line1\r\nline2 \"quote\"", 8},
      {3, u8"\u65E5\u672C\u8A9E\u30C6\u30B9\u30C8", u8"\u5024=\"\u6771\u4EAC\", note='\u5927\u962A'", 9},
      {4, "O'Reilly", "SELECT 'It''s fine';", 10},
  };

  for (const auto &r : rows) {
    sqlite3_bind_int(stmt, 1, r.id);
    sqlite3_bind_text(stmt, 2, r.name, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 3, r.sql, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int(stmt, 4, r.score);
    sqlite3_bind_text(stmt, 5, "2026-03-03 12:00:00", -1, SQLITE_TRANSIENT);
    if (sqlite3_step(stmt) != SQLITE_DONE) {
      sqlite3_finalize(stmt);
      sqlite3_close(db);
      return -1;
    }
    sqlite3_reset(stmt);
    sqlite3_clear_bindings(stmt);
  }
  sqlite3_finalize(stmt);
  sqlite3_close(db);
  return 0;
}

int create_special_duckdb_table(const char *db_path) {
  duckdb_database db = nullptr;
  duckdb_connection conn = nullptr;
  std::string err;
  if (duckdb_open(db_path, &db) != DuckDBSuccess) {
    return -1;
  }
  if (duckdb_connect(db, &conn) != DuckDBSuccess) {
    duckdb_close(&db);
    return -1;
  }
  if (!duckdb_exec(conn,
                   "CREATE TABLE IF NOT EXISTS sample_data_special ("
                   "id BIGINT, name VARCHAR, sql_text VARCHAR, score BIGINT, created_at VARCHAR);",
                   &err)) {
    duckdb_disconnect(&conn);
    duckdb_close(&db);
    return -1;
  }
  if (!duckdb_exec(conn, "DELETE FROM sample_data_special;", &err)) {
    duckdb_disconnect(&conn);
    duckdb_close(&db);
    return -1;
  }

  struct Row {
    int id;
    const char *name;
    const char *sql;
    int score;
  };
  const Row rows[] = {
      {1, u8"\uD55C\uAE00 \"\uB530\uC634\uD45C\", \uC27C\uD45C", "SELECT \"A\", 'B', 1,\n2", 7},
      {2, u8"\u4E2D\u6587,\u9017\u53F7", "line1\r\nline2 \"quote\"", 8},
      {3, u8"\u65E5\u672C\u8A9E\u30C6\u30B9\u30C8", u8"\u5024=\"\u6771\u4EAC\", note='\u5927\u962A'", 9},
      {4, "O'Reilly", "SELECT 'It''s fine';", 10},
  };
  for (const auto &r : rows) {
    std::string sql = "INSERT INTO sample_data_special VALUES (";
    sql += std::to_string(r.id) + ", '";
    sql += sql_escape_single_quoted(r.name) + "', '";
    sql += sql_escape_single_quoted(r.sql) + "', ";
    sql += std::to_string(r.score) + ", '2026-03-03 12:00:00');";
    if (!duckdb_exec(conn, sql.c_str(), &err)) {
      duckdb_disconnect(&conn);
      duckdb_close(&db);
      return -1;
    }
  }

  duckdb_disconnect(&conn);
  duckdb_close(&db);
  return 0;
}

int get_duckdb_row_count(const char *db_path, int *out_rows) {
  duckdb_database db = nullptr;
  duckdb_connection conn = nullptr;
  duckdb_result result{};
  if (duckdb_open(db_path, &db) != DuckDBSuccess) {
    return -1;
  }
  if (duckdb_connect(db, &conn) != DuckDBSuccess) {
    duckdb_close(&db);
    return -1;
  }
  if (duckdb_query(conn, "SELECT COUNT(*) FROM sample_data;", &result) != DuckDBSuccess) {
    duckdb_disconnect(&conn);
    duckdb_close(&db);
    return -1;
  }
  const int rows = static_cast<int>(duckdb_value_int64(&result, 0, 0));
  duckdb_destroy_result(&result);
  duckdb_disconnect(&conn);
  duckdb_close(&db);
  if (rows < 0) {
    return -1;
  }
  *out_rows = rows;
  return 0;
}

int verify_exported_xlsx(const char *xlsx_path, int expected_rows) {
  cxlsx_reader *r = cxlsx_reader_open(xlsx_path, nullptr);
  if (r == nullptr) {
    return -1;
  }
  if (cxlsx_reader_open_sheet(r, 0) != CXLSX_OK) {
    cxlsx_reader_close(r);
    return -1;
  }
  int row_count = cxlsx_reader_row_count(r);
  if (row_count != expected_rows + 1) {
    cxlsx_reader_close(r);
    return -1;
  }
  cxlsx_read_cell c{};
  if (cxlsx_reader_get_cell(r, 0, 0, &c) != CXLSX_OK || c.str_val == nullptr ||
      strcmp(c.str_val, "UserName") != 0) {
    cxlsx_reader_close(r);
    return -1;
  }
  cxlsx_reader_close(r);
  return 0;
}

int verify_exported_xlsx_no_header(const char *xlsx_path, int expected_rows) {
  cxlsx_reader *r = cxlsx_reader_open(xlsx_path, nullptr);
  if (r == nullptr) {
    return -1;
  }
  if (cxlsx_reader_open_sheet(r, 0) != CXLSX_OK) {
    cxlsx_reader_close(r);
    return -1;
  }
  const int row_count = cxlsx_reader_row_count(r);
  if (row_count != expected_rows) {
    cxlsx_reader_close(r);
    return -1;
  }
  cxlsx_read_cell c{};
  if (cxlsx_reader_get_cell(r, 0, 0, &c) != CXLSX_OK || c.str_val == nullptr) {
    cxlsx_reader_close(r);
    return -1;
  }
  if (strcmp(c.str_val, "UserName") == 0) {
    cxlsx_reader_close(r);
    return -1;
  }
  cxlsx_reader_close(r);
  return 0;
}

int count_json_rows(const std::string &content) {
  const size_t rows_key = content.find("\"rows\"");
  if (rows_key == std::string::npos) {
    return -1;
  }
  const size_t rows_array_start = content.find('[', rows_key);
  if (rows_array_start == std::string::npos) {
    return -1;
  }

  bool in_string = false;
  bool escaped = false;
  int depth = 0;
  int rows = 0;
  for (size_t i = rows_array_start; i < content.size(); ++i) {
    const char ch = content[i];
    if (in_string) {
      if (escaped) {
        escaped = false;
      } else if (ch == '\\') {
        escaped = true;
      } else if (ch == '"') {
        in_string = false;
      }
      continue;
    }
    if (ch == '"') {
      in_string = true;
      continue;
    }
    if (ch == '[') {
      ++depth;
      if (depth == 2) {
        ++rows;
      }
      continue;
    }
    if (ch == ']') {
      if (depth <= 0) {
        return -1;
      }
      --depth;
      if (depth == 0) {
        return rows;
      }
    }
  }
  return -1;
}

int verify_exported_json(const char *json_path, int expected_rows = -1) {
  std::ifstream in(json_path, std::ios::binary);
  if (!in.is_open()) {
    return -1;
  }
  const std::string content((std::istreambuf_iterator<char>(in)),
                            std::istreambuf_iterator<char>());
  if (content.find("\"columns\"") == std::string::npos) {
    return -1;
  }
  if (content.find("\"rows\"") == std::string::npos) {
    return -1;
  }
  if (content.find("[") == std::string::npos || content.find("]") == std::string::npos) {
    return -1;
  }
  if (expected_rows >= 0) {
    const int json_rows = count_json_rows(content);
    if (json_rows != expected_rows) {
      return -1;
    }
  }
  return 0;
}

int verify_exported_csv(const char *csv_path, int expected_rows) {
  std::ifstream in(csv_path, std::ios::binary);
  if (!in.is_open()) {
    return -1;
  }
  std::string content((std::istreambuf_iterator<char>(in)),
                      std::istreambuf_iterator<char>());

  // Detect UTF-8 BOM and consume it.
  size_t pos = 0;
  if (content.size() >= 3 &&
      static_cast<unsigned char>(content[0]) == 0xEF &&
      static_cast<unsigned char>(content[1]) == 0xBB &&
      static_cast<unsigned char>(content[2]) == 0xBF) {
    pos = 3;
  }

  bool in_quotes = false;
  std::string field;
  std::vector<std::string> row;
  int data_rows = 0;
  auto finish_row = [&]() -> int {
    if (row.size() != 4) {
      return -1;
    }
    if (data_rows == 0) {
      if (row[0] != "UserName" || row[1] != "ID" || row[2] != "SQL" || row[3] != "Score") {
        return -1;
      }
    }
    ++data_rows;
    row.clear();
    return 0;
  };

  while (pos < content.size()) {
    const char ch = content[pos++];
    if (in_quotes) {
      if (ch == '"') {
        if (pos < content.size() && content[pos] == '"') {
          field.push_back('"');
          ++pos;
        } else {
          in_quotes = false;
        }
      } else {
        field.push_back(ch);
      }
      continue;
    }

    if (ch == '"') {
      in_quotes = true;
      continue;
    }
    if (ch == ',') {
      row.push_back(field);
      field.clear();
      continue;
    }
    if (ch == '\r') {
      if (pos < content.size() && content[pos] == '\n') {
        ++pos;
      }
      row.push_back(field);
      field.clear();
      if (finish_row() != 0) {
        return -1;
      }
      continue;
    }
    if (ch == '\n') {
      row.push_back(field);
      field.clear();
      if (finish_row() != 0) {
        return -1;
      }
      continue;
    }
    field.push_back(ch);
  }

  // Final row (when file has no trailing newline).
  if (in_quotes) {
    return -1;
  }
  if (!field.empty() || !row.empty()) {
    row.push_back(field);
    if (finish_row() != 0) {
      return -1;
    }
  }

  // Exclude header row.
  const int parsed_data_rows = (data_rows > 0) ? (data_rows - 1) : 0;
  return (parsed_data_rows == expected_rows) ? 0 : -1;
}

bool csv_has_crlf(const char *csv_path) {
  std::ifstream in(csv_path, std::ios::binary);
  if (!in.is_open()) {
    return false;
  }
  const std::string content((std::istreambuf_iterator<char>(in)),
                            std::istreambuf_iterator<char>());
  return content.find("\r\n") != std::string::npos;
}

bool csv_has_lf_without_crlf(const char *csv_path) {
  std::ifstream in(csv_path, std::ios::binary);
  if (!in.is_open()) {
    return false;
  }
  const std::string content((std::istreambuf_iterator<char>(in)),
                            std::istreambuf_iterator<char>());
  return content.find('\n') != std::string::npos &&
         content.find("\r\n") == std::string::npos;
}

bool parse_csv_file(const char *csv_path, std::vector<std::vector<std::string>> *rows) {
  rows->clear();
  std::ifstream in(csv_path, std::ios::binary);
  if (!in.is_open()) {
    return false;
  }
  std::string content((std::istreambuf_iterator<char>(in)),
                      std::istreambuf_iterator<char>());

  size_t pos = 0;
  if (content.size() >= 3 &&
      static_cast<unsigned char>(content[0]) == 0xEF &&
      static_cast<unsigned char>(content[1]) == 0xBB &&
      static_cast<unsigned char>(content[2]) == 0xBF) {
    pos = 3;
  }

  bool in_quotes = false;
  std::string field;
  std::vector<std::string> row;
  auto push_row = [&]() {
    rows->push_back(row);
    row.clear();
  };

  while (pos < content.size()) {
    const char ch = content[pos++];
    if (in_quotes) {
      if (ch == '"') {
        if (pos < content.size() && content[pos] == '"') {
          field.push_back('"');
          ++pos;
        } else {
          in_quotes = false;
        }
      } else {
        field.push_back(ch);
      }
      continue;
    }
    if (ch == '"') {
      in_quotes = true;
      continue;
    }
    if (ch == ',') {
      row.push_back(field);
      field.clear();
      continue;
    }
    if (ch == '\r') {
      if (pos < content.size() && content[pos] == '\n') {
        ++pos;
      }
      row.push_back(field);
      field.clear();
      push_row();
      continue;
    }
    if (ch == '\n') {
      row.push_back(field);
      field.clear();
      push_row();
      continue;
    }
    field.push_back(ch);
  }

  if (in_quotes) {
    return false;
  }
  if (!field.empty() || !row.empty()) {
    row.push_back(field);
    push_row();
  }
  return true;
}

bool verify_special_csv_rows(const char *csv_path) {
  std::vector<std::vector<std::string>> rows;
  if (!parse_csv_file(csv_path, &rows)) {
    return false;
  }
  if (rows.size() != 5) {
    return false;
  }
  if (rows[0].size() != 4 || rows[0][0] != "UserName" || rows[0][1] != "ID" ||
      rows[0][2] != "SQL" || rows[0][3] != "Score") {
    return false;
  }

  const std::string sep(1, '\x1f');
  std::unordered_map<std::string, int> expected;
  expected[std::string(u8"\uD55C\uAE00 \"\uB530\uC634\uD45C\", \uC27C\uD45C") + sep + "1" +
           sep + "SELECT \"A\", 'B', 1,\n2" + sep + "7"] = 1;
  expected[std::string(u8"\u4E2D\u6587,\u9017\u53F7") + sep + "2" + sep +
           "line1\r\nline2 \"quote\"" + sep + "8"] = 1;
  expected[std::string(u8"\u65E5\u672C\u8A9E\u30C6\u30B9\u30C8") + sep + "3" + sep +
           std::string(u8"\u5024=\"\u6771\u4EAC\", note='\u5927\u962A'") + sep + "9"] = 1;
  expected["O'Reilly" + sep + "4" + sep + "SELECT 'It''s fine';" + sep + "10"] = 1;

  for (size_t i = 1; i < rows.size(); ++i) {
    if (rows[i].size() != 4) {
      return false;
    }
    const std::string key = rows[i][0] + sep + rows[i][1] + sep + rows[i][2] + sep + rows[i][3];
    auto it = expected.find(key);
    if (it == expected.end() || it->second == 0) {
      return false;
    }
    it->second -= 1;
  }
  for (const auto &kv : expected) {
    if (kv.second != 0) {
      return false;
    }
  }
  return true;
}

#if defined(_WIN32)
bool read_clipboard_utf8(std::string *text, std::string *err) {
  constexpr int kOpenRetries = 50;
  bool opened = false;
  for (int i = 0; i < kOpenRetries; ++i) {
    if (OpenClipboard(nullptr) != 0) {
      opened = true;
      break;
    }
    Sleep(10);
  }
  if (!opened) {
    *err = "OpenClipboard failed.";
    return false;
  }
  bool ok = false;
  do {
    HANDLE h = GetClipboardData(CF_UNICODETEXT);
    if (h == nullptr) {
      *err = "GetClipboardData(CF_UNICODETEXT) failed.";
      break;
    }
    const wchar_t *w = static_cast<const wchar_t *>(GlobalLock(h));
    if (w == nullptr) {
      *err = "GlobalLock clipboard handle failed.";
      break;
    }
    const int needed =
        WideCharToMultiByte(CP_UTF8, 0, w, -1, nullptr, 0, nullptr, nullptr);
    if (needed <= 0) {
      GlobalUnlock(h);
      *err = "WideCharToMultiByte size query failed.";
      break;
    }
    std::vector<char> utf8_buf(static_cast<size_t>(needed), '\0');
    if (WideCharToMultiByte(CP_UTF8, 0, w, -1, utf8_buf.data(), needed, nullptr, nullptr) <=
        0) {
      GlobalUnlock(h);
      *err = "WideCharToMultiByte conversion failed.";
      break;
    }
    GlobalUnlock(h);
    *text = std::string(utf8_buf.data());
    ok = true;
  } while (false);
  CloseClipboard();
  return ok;
}
#endif

int run_provider_focus_tests(const char *db_path, CWvDataSourceType source_type,
                             const std::vector<CWvExportColumn> &mapping) {
  CWvExportOptions opt;
  opt.source_type = source_type;
  opt.export_format = CWvExportFormat::Csv;
  opt.table_name = "sample_data";
  opt.output_path = "out_test/provider_focus.csv";
  opt.include_header = true;
  opt.enforce_source_index = true;

  const char *missing_path = (source_type == CWvDataSourceType::Sqlite)
                                 ? "out_test/missing_provider_focus.sqlite"
                                 : "out_test/missing_provider_focus.duckdb";
  {
    CWvExport exporter;
    CWvExportResult res;
    if (exporter.Export(missing_path, mapping, opt, &res)) {
      fprintf(stderr, "[testExport] provider-focus missing-path expected failure.\n");
      return 81;
    }
  }

  {
    CWvExportOptions bad_table = opt;
    bad_table.table_name = "not_exists_table";
    CWvExport exporter;
    CWvExportResult res;
    if (exporter.Export(db_path, mapping, bad_table, &res)) {
      fprintf(stderr, "[testExport] provider-focus bad-table expected failure.\n");
      return 82;
    }
    const std::string table_err = exporter.GetLastError();
    if (table_err.find("Table not found") == std::string::npos &&
        table_err.find("does not exist") == std::string::npos) {
      fprintf(stderr, "[testExport] provider-focus bad-table wrong error: %s\n",
              exporter.GetLastError().c_str());
      return 83;
    }
  }

  {
    std::vector<CWvExportColumn> bad_mapping = mapping;
    if (!bad_mapping.empty()) {
      bad_mapping[0].source_name = "__missing_column__";
    }
    CWvExport exporter;
    CWvExportResult res;
    if (exporter.Export(db_path, bad_mapping, opt, &res)) {
      fprintf(stderr, "[testExport] provider-focus bad-mapping expected failure.\n");
      return 84;
    }
    if (exporter.GetLastError().find("Mapped column not found") == std::string::npos) {
      fprintf(stderr, "[testExport] provider-focus bad-mapping wrong error: %s\n",
              exporter.GetLastError().c_str());
      return 85;
    }
  }

  printf("[testExport] provider-focus all checks passed.\n");
  return 0;
}

int run_csv_unicode_tests(const char *db_path, CWvDataSourceType source_type,
                          const std::vector<CWvExportColumn> &mapping) {
  int seed_rc = 0;
  if (source_type == CWvDataSourceType::Sqlite) {
    seed_rc = create_special_sqlite_table(db_path);
  } else if (source_type == CWvDataSourceType::DuckDb) {
    seed_rc = create_special_duckdb_table(db_path);
  } else {
    return 86;
  }
  if (seed_rc != 0) {
    fprintf(stderr, "[testExport] csv-unicode failed to seed special table.\n");
    return 86;
  }

  CWvExportOptions opt;
  opt.source_type = source_type;
  opt.export_format = CWvExportFormat::Csv;
  opt.table_name = "sample_data_special";
  opt.include_header = true;
  opt.enforce_source_index = true;
  opt.csv_use_utf8 = true;
  opt.csv_use_crlf = true;
  opt.output_path = "out_test/csv_unicode_utf8.csv";

  {
    CWvExport exporter;
    CWvExportResult res;
    if (!exporter.Export(db_path, mapping, opt, &res)) {
      fprintf(stderr, "[testExport] csv-unicode utf8 export failed: %s\n",
              exporter.GetLastError().c_str());
      return 87;
    }
    if (verify_exported_csv(res.output_path.c_str(), 4) != 0 ||
        !verify_special_csv_rows(res.output_path.c_str())) {
      fprintf(stderr, "[testExport] csv-unicode utf8 verify failed.\n");
      return 88;
    }
  }

  printf("[testExport] csv-unicode all checks passed.\n");
  return 0;
}

std::vector<std::string> find_part_files(const std::string &base_out) {
  std::vector<std::string> parts;
  const std::filesystem::path p(base_out);
  const std::string prefix = p.stem().string() + ".part";
  const std::string ext = p.extension().string();
  const std::filesystem::path dir =
      p.parent_path().empty() ? std::filesystem::path(".") : p.parent_path();
  std::error_code ec;
  if (!std::filesystem::exists(dir, ec)) {
    return parts;
  }
  for (const auto &e : std::filesystem::directory_iterator(dir, ec)) {
    if (ec || !e.is_regular_file()) {
      continue;
    }
    const std::filesystem::path f = e.path();
    if (f.extension().string() != ext) {
      continue;
    }
    const std::string name = f.stem().string();
    if (name.rfind(prefix, 0) == 0) {
      parts.push_back(f.string());
    }
  }
  std::sort(parts.begin(), parts.end());
  return parts;
}

void remove_part_files(const std::string &base_out) {
  std::error_code ec;
  auto parts = find_part_files(base_out);
  for (const auto &p : parts) {
    std::filesystem::remove(p, ec);
  }
}

int run_cancel_export_test(const char *db_path, CWvDataSourceType source_type,
                           const std::vector<CWvExportColumn> &mapping,
                           CWvExportFormat format, CWvJsonBackend json_backend,
                           const std::string &out, CWvCancelPolicy policy, const char *label) {
  std::error_code ec;
  std::filesystem::remove(out, ec);
  remove_part_files(out);

  CWvExportOptions opt;
  opt.source_type = source_type;
  opt.export_format = format;
  opt.json_backend = json_backend;
  opt.table_name = "sample_data";
  opt.output_path = out;
  opt.include_header = true;
  opt.enforce_source_index = true;
  opt.max_rows_per_file = 1000;
  opt.cancel_policy = policy;

  CWvExport exporter;
  CWvExportResult res;
  bool ok = false;

  std::thread worker([&]() { ok = exporter.Export(db_path, mapping, opt, &res); });
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (find_part_files(out).empty() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  exporter.RequestCancel();
  worker.join();

  if (ok) {
    fprintf(stderr, "[testExport] cancel test failed: export succeeded unexpectedly.\n");
    return 7;
  }
  if (exporter.GetLastError().find("canceled") == std::string::npos) {
    fprintf(stderr, "[testExport] cancel test failed: unexpected error: %s\n",
            exporter.GetLastError().c_str());
    return 8;
  }
  const auto parts = find_part_files(out);
  if (policy == CWvCancelPolicy::KeepPartial) {
    if (parts.empty()) {
      fprintf(stderr, "[testExport] cancel(%s) failed: expected partial part files.\n", label);
      return 9;
    }
  } else {
    if (!parts.empty()) {
      fprintf(stderr, "[testExport] cancel(%s) failed: expected no part files after cleanup.\n",
              label);
      return 10;
    }
  }
  if (std::filesystem::exists(out)) {
    fprintf(stderr, "[testExport] cancel(%s) failed: base output unexpectedly exists: %s\n",
            label, out.c_str());
    return 9;
  }
  printf("[testExport] cancel test passed (%s).\n", label);
  return 0;
}

int run_csv_focus_tests(const char *db_path, CWvDataSourceType source_type,
                        const std::vector<CWvExportColumn> &mapping, int expected_rows) {
  CWvExportOptions opt;
  opt.source_type = source_type;
  opt.export_format = CWvExportFormat::Csv;
  opt.table_name = "sample_data";
  opt.include_header = true;
  opt.enforce_source_index = true;

  // 1) Default CSV: UTF-8 + CRLF
  {
    opt.output_path = "out_test/csv_focus_default.csv";
    opt.csv_use_utf8 = true;
    opt.csv_use_crlf = true;
    opt.max_rows_per_file = 0;
    CWvExport exporter;
    CWvExportResult res;
    if (!exporter.Export(db_path, mapping, opt, &res)) {
      fprintf(stderr, "[testExport] csv-focus default export failed: %s\n",
              exporter.GetLastError().c_str());
      return 21;
    }
    if (verify_exported_csv(res.output_path.c_str(), expected_rows) != 0 ||
        !csv_has_crlf(res.output_path.c_str())) {
      fprintf(stderr, "[testExport] csv-focus default verify failed.\n");
      return 22;
    }
  }

  // 2) LF mode
  {
    opt.output_path = "out_test/csv_focus_lf.csv";
    opt.csv_use_utf8 = true;
    opt.csv_use_crlf = false;
    opt.max_rows_per_file = 0;
    CWvExport exporter;
    CWvExportResult res;
    if (!exporter.Export(db_path, mapping, opt, &res)) {
      fprintf(stderr, "[testExport] csv-focus lf export failed: %s\n",
              exporter.GetLastError().c_str());
      return 23;
    }
    if (verify_exported_csv(res.output_path.c_str(), expected_rows) != 0 ||
        !csv_has_lf_without_crlf(res.output_path.c_str())) {
      fprintf(stderr, "[testExport] csv-focus lf verify failed.\n");
      return 24;
    }
  }

  // 3) ANSI mode must be rejected
  {
    CWvExportOptions ansi = opt;
    ansi.output_path = "out_test/csv_focus_ansi_should_fail.csv";
    ansi.csv_use_utf8 = false;
    ansi.csv_use_crlf = true;
    ansi.max_rows_per_file = 0;
    CWvExport exporter;
    CWvExportResult res;
    if (exporter.Export(db_path, mapping, ansi, &res)) {
      fprintf(stderr, "[testExport] csv-focus ansi should fail but succeeded.\n");
      return 25;
    }
    if (exporter.GetLastError().find("ANSI CSV is disabled") == std::string::npos) {
      fprintf(stderr, "[testExport] csv-focus ansi wrong error: %s\n",
              exporter.GetLastError().c_str());
      return 26;
    }
  }

  // 4) Split mode
  {
    opt.output_path = "out_test/csv_focus_split.csv";
    opt.csv_use_utf8 = true;
    opt.csv_use_crlf = true;
    opt.max_rows_per_file = 1000;
    CWvExport exporter;
    CWvExportResult res;
    if (!exporter.Export(db_path, mapping, opt, &res)) {
      fprintf(stderr, "[testExport] csv-focus split export failed: %s\n",
              exporter.GetLastError().c_str());
      return 27;
    }
    constexpr int kSplitRows = 1000;
    const int expected_parts = (expected_rows + kSplitRows - 1) / kSplitRows;
    if (res.output_paths.size() != static_cast<size_t>(expected_parts)) {
      fprintf(stderr, "[testExport] csv-focus split part count mismatch: %zu\n",
              res.output_paths.size());
      return 28;
    }
    for (size_t i = 0; i < res.output_paths.size(); ++i) {
      const int expected_part_rows =
          (i + 1 == res.output_paths.size())
              ? (expected_rows - static_cast<int>(i) * kSplitRows)
              : kSplitRows;
      if (verify_exported_csv(res.output_paths[i].c_str(), expected_part_rows) != 0) {
        fprintf(stderr, "[testExport] csv-focus split verify failed: %s\n",
                res.output_paths[i].c_str());
        return 29;
      }
    }
  }

  // 5) Cancel mode (keep/delete partial)
  {
    int rc = run_cancel_export_test(db_path, source_type, mapping, CWvExportFormat::Csv,
                                    CWvJsonBackend::RapidJson, "out_test/cancel_test.csv",
                                    CWvCancelPolicy::KeepPartial, "csv-keep");
    if (rc != 0) {
      return rc;
    }
    rc = run_cancel_export_test(db_path, source_type, mapping, CWvExportFormat::Csv,
                                CWvJsonBackend::RapidJson, "out_test/cancel_test.csv",
                                CWvCancelPolicy::DeletePartial, "csv-delete");
    if (rc != 0) {
      return rc;
    }
  }

  printf("[testExport] csv-focus all checks passed.\n");
  return 0;
}

int run_split_focus_tests(const char *db_path, CWvDataSourceType source_type,
                          const std::vector<CWvExportColumn> &mapping, int expected_rows) {
  constexpr int kSplitRows = 1000;
  const int expected_parts = (expected_rows + kSplitRows - 1) / kSplitRows;

  auto run_split_case = [&](CWvExportFormat format, CWvJsonBackend json_backend,
                            const char *out, const char *label, bool csv_utf8,
                            bool csv_crlf) -> int {
    std::error_code ec;
    std::filesystem::remove(out, ec);
    remove_part_files(out);

    CWvExportOptions opt;
    opt.source_type = source_type;
    opt.export_format = format;
    opt.json_backend = json_backend;
    opt.table_name = "sample_data";
    opt.output_path = out;
    opt.include_header = true;
    opt.enforce_source_index = true;
    opt.max_rows_per_file = kSplitRows;
    opt.csv_use_utf8 = csv_utf8;
    opt.csv_use_crlf = csv_crlf;

    CWvExport exporter;
    CWvExportResult res;
    if (!exporter.Export(db_path, mapping, opt, &res)) {
      fprintf(stderr, "[testExport] split-focus %s export failed: %s\n", label,
              exporter.GetLastError().c_str());
      return 31;
    }
    if (res.output_paths.size() != static_cast<size_t>(expected_parts)) {
      fprintf(stderr, "[testExport] split-focus %s part count mismatch: %zu\n", label,
              res.output_paths.size());
      return 32;
    }

    for (size_t i = 0; i < res.output_paths.size(); ++i) {
      const int expected_part_rows =
          (i + 1 == res.output_paths.size())
              ? (expected_rows - static_cast<int>(i) * kSplitRows)
              : kSplitRows;
      int rc = -1;
      if (format == CWvExportFormat::Xlsx) {
        rc = verify_exported_xlsx(res.output_paths[i].c_str(), expected_part_rows);
      } else if (format == CWvExportFormat::Csv) {
        rc = verify_exported_csv(res.output_paths[i].c_str(), expected_part_rows);
      } else if (format == CWvExportFormat::Json) {
        rc = verify_exported_json(res.output_paths[i].c_str(), expected_part_rows);
      }
      if (rc != 0) {
        fprintf(stderr, "[testExport] split-focus %s verify failed: %s\n", label,
                res.output_paths[i].c_str());
        return 33;
      }
    }
    printf("[testExport] split test passed (%s).\n", label);
    return 0;
  };

  int rc = run_split_case(CWvExportFormat::Xlsx, CWvJsonBackend::RapidJson,
                          "out_test/split_focus.xlsx", "xlsx", true, true);
  if (rc != 0) {
    return rc;
  }
  rc = run_split_case(CWvExportFormat::Csv, CWvJsonBackend::RapidJson,
                      "out_test/split_focus.csv", "csv", true, true);
  if (rc != 0) {
    return rc;
  }
  rc = run_split_case(CWvExportFormat::Json, CWvJsonBackend::RapidJson,
                      "out_test/split_focus_rapid.json", "json-rapid", true, true);
  if (rc != 0) {
    return rc;
  }
  rc = run_split_case(CWvExportFormat::Json, CWvJsonBackend::YyJson,
                      "out_test/split_focus_yy.json", "json-yy", true, true);
  if (rc != 0) {
    return rc;
  }

  printf("[testExport] split-focus all checks passed.\n");
  return 0;
}

int run_clipboard_focus_tests(const char *db_path, CWvDataSourceType source_type,
                              const std::vector<CWvExportColumn> &mapping) {
  CWvExportOptions opt;
  opt.source_type = source_type;
  opt.export_format = CWvExportFormat::Clipboard;
  opt.table_name = "sample_data";
  opt.include_header = true;
  opt.enforce_source_index = true;
  opt.max_clipboard_bytes = 128u * 1024u * 1024u;
  opt.max_clipboard_rows = 1000000;

  // 1) Interactive flow simulation: y, y, n -> 20,000 rows copied (2 chunks)
  {
    opt.clipboard_chunk_rows = 10000;
    opt.clipboard_chunk_confirm = [](int copied_rows, int) {
      return (copied_rows < 20000) ? CWvClipboardChunkAction::Continue
                                    : CWvClipboardChunkAction::Stop;
    };

    CWvExport exporter;
    CWvExportResult res;
    if (!exporter.Export(db_path, mapping, opt, &res)) {
      fprintf(stderr, "[testExport] clipboard-focus yyn export failed: %s\n",
              exporter.GetLastError().c_str());
      return 41;
    }
    if (!res.stopped_by_user || res.clipboard_chunks_copied != 2 || res.rows_exported != 20000) {
      fprintf(stderr, "[testExport] clipboard-focus yyn verify failed (rows=%d chunks=%d stop=%d).\n",
              res.rows_exported, res.clipboard_chunks_copied, res.stopped_by_user ? 1 : 0);
      return 42;
    }
#if defined(_WIN32)
    std::string clip;
    std::string clip_err;
    if (!read_clipboard_utf8(&clip, &clip_err)) {
      fprintf(stderr, "[testExport] clipboard-focus yyn read failed: %s\n", clip_err.c_str());
      return 42;
    }
    int line_count = 0;
    for (char ch : clip) {
      if (ch == '\n') {
        ++line_count;
      }
    }
    if (line_count != 10000 ||
        clip.rfind("UserName\tID\tSQL\tScore\r\n", 0) == 0) {
      fprintf(stderr, "[testExport] clipboard-focus yyn content verify failed.\n");
      return 42;
    }
#endif
  }

  // 2) Interactive flow simulation: n -> 10,000 rows copied (1 chunk)
  {
    opt.clipboard_chunk_rows = 10000;
    opt.clipboard_chunk_confirm = [](int, int) { return CWvClipboardChunkAction::Stop; };

    CWvExport exporter;
    CWvExportResult res;
    if (!exporter.Export(db_path, mapping, opt, &res)) {
      fprintf(stderr, "[testExport] clipboard-focus n export failed: %s\n",
              exporter.GetLastError().c_str());
      return 43;
    }
    if (!res.stopped_by_user || res.clipboard_chunks_copied != 1 || res.rows_exported != 10000) {
      fprintf(stderr, "[testExport] clipboard-focus n verify failed (rows=%d chunks=%d stop=%d).\n",
              res.rows_exported, res.clipboard_chunks_copied, res.stopped_by_user ? 1 : 0);
      return 44;
    }
#if defined(_WIN32)
    std::string clip;
    std::string clip_err;
    if (!read_clipboard_utf8(&clip, &clip_err)) {
      fprintf(stderr, "[testExport] clipboard-focus n read failed: %s\n", clip_err.c_str());
      return 44;
    }
    int line_count = 0;
    for (char ch : clip) {
      if (ch == '\n') {
        ++line_count;
      }
    }
    if (line_count != 10001 ||
        clip.rfind("UserName\tID\tSQL\tScore\r\n", 0) != 0) {
      fprintf(stderr, "[testExport] clipboard-focus n content verify failed.\n");
      return 44;
    }
#endif
  }

  // 3) Validation: chunk size without callback must fail
  {
    opt.clipboard_chunk_rows = 10000;
    opt.clipboard_chunk_confirm = {};

    CWvExport exporter;
    CWvExportResult res;
    if (exporter.Export(db_path, mapping, opt, &res)) {
      fprintf(stderr, "[testExport] clipboard-focus missing callback expected failure.\n");
      return 45;
    }
    if (exporter.GetLastError().find("clipboard_chunk_confirm") == std::string::npos) {
      fprintf(stderr, "[testExport] clipboard-focus missing callback wrong error: %s\n",
              exporter.GetLastError().c_str());
      return 46;
    }
  }

  // 4) Callback exception should fail safely
  {
    opt.clipboard_chunk_rows = 10000;
    opt.clipboard_chunk_confirm = [](int, int) -> CWvClipboardChunkAction {
      throw std::runtime_error("boom");
    };

    CWvExport exporter;
    CWvExportResult res;
    if (exporter.Export(db_path, mapping, opt, &res)) {
      fprintf(stderr, "[testExport] clipboard-focus callback throw expected failure.\n");
      return 47;
    }
    if (exporter.GetLastError().find("callback threw") == std::string::npos) {
      fprintf(stderr, "[testExport] clipboard-focus callback throw wrong error: %s\n",
              exporter.GetLastError().c_str());
      return 48;
    }
  }

  // 5) Boundary: max_clipboard_rows = 1 must fail by row-limit
  {
    opt.clipboard_chunk_rows = 0;
    opt.clipboard_chunk_confirm = {};
    opt.max_clipboard_rows = 1;

    CWvExport exporter;
    CWvExportResult res;
    if (exporter.Export(db_path, mapping, opt, &res)) {
      fprintf(stderr, "[testExport] clipboard-focus row limit expected failure.\n");
      return 49;
    }
    if (exporter.GetLastError().find("max_clipboard_rows") == std::string::npos) {
      fprintf(stderr, "[testExport] clipboard-focus row limit wrong error: %s\n",
              exporter.GetLastError().c_str());
      return 50;
    }
  }

  printf("[testExport] clipboard-focus all checks passed.\n");
  return 0;
}

int run_writer_focus_tests(const char *db_path, CWvDataSourceType source_type,
                           const std::vector<CWvExportColumn> &mapping, int expected_rows) {
  // 1) XLSX no-header verification
  {
    CWvExportOptions opt;
    opt.source_type = source_type;
    opt.export_format = CWvExportFormat::Xlsx;
    opt.table_name = "sample_data";
    opt.sheet_name = "ExportData";
    opt.output_path = "out_test/xlsx_focus_no_header.xlsx";
    opt.include_header = false;
    opt.enforce_source_index = true;

    CWvExport exporter;
    CWvExportResult res;
    if (!exporter.Export(db_path, mapping, opt, &res)) {
      fprintf(stderr, "[testExport] writer-focus xlsx no-header export failed: %s\n",
              exporter.GetLastError().c_str());
      return 61;
    }
    if (verify_exported_xlsx_no_header(res.output_path.c_str(), expected_rows) != 0) {
      fprintf(stderr, "[testExport] writer-focus xlsx no-header verify failed.\n");
      return 62;
    }
  }

  // 2) JSON rapid row-count verification
  {
    CWvExportOptions opt;
    opt.source_type = source_type;
    opt.export_format = CWvExportFormat::Json;
    opt.json_backend = CWvJsonBackend::RapidJson;
    opt.table_name = "sample_data";
    opt.output_path = "out_test/json_focus_rapid.json";
    opt.include_header = true;
    opt.enforce_source_index = true;

    CWvExport exporter;
    CWvExportResult res;
    if (!exporter.Export(db_path, mapping, opt, &res)) {
      fprintf(stderr, "[testExport] writer-focus json-rapid export failed: %s\n",
              exporter.GetLastError().c_str());
      return 63;
    }
    if (verify_exported_json(res.output_path.c_str(), expected_rows) != 0) {
      fprintf(stderr, "[testExport] writer-focus json-rapid verify failed.\n");
      return 64;
    }
  }

  // 3) JSON yy row-count verification
  {
    CWvExportOptions opt;
    opt.source_type = source_type;
    opt.export_format = CWvExportFormat::Json;
    opt.json_backend = CWvJsonBackend::YyJson;
    opt.table_name = "sample_data";
    opt.output_path = "out_test/json_focus_yy.json";
    opt.include_header = true;
    opt.enforce_source_index = true;

    CWvExport exporter;
    CWvExportResult res;
    if (!exporter.Export(db_path, mapping, opt, &res)) {
      fprintf(stderr, "[testExport] writer-focus json-yy export failed: %s\n",
              exporter.GetLastError().c_str());
      return 65;
    }
    if (verify_exported_json(res.output_path.c_str(), expected_rows) != 0) {
      fprintf(stderr, "[testExport] writer-focus json-yy verify failed.\n");
      return 66;
    }
  }

  printf("[testExport] writer-focus all checks passed.\n");
  return 0;
}

int run_internal_stress_tests(const char *db_path, CWvDataSourceType source_type,
                              const std::vector<CWvExportColumn> &mapping) {
  struct StressCase {
    CWvExportFormat format;
    CWvJsonBackend json_backend;
    const char *tag;
    const char *ext;
  };
  const StressCase cases[] = {
      {CWvExportFormat::Xlsx, CWvJsonBackend::RapidJson, "xlsx", ".xlsx"},
      {CWvExportFormat::Csv, CWvJsonBackend::RapidJson, "csv", ".csv"},
      {CWvExportFormat::Json, CWvJsonBackend::RapidJson, "jsonr", ".json"},
      {CWvExportFormat::Json, CWvJsonBackend::YyJson, "jsony", ".json"},
  };

  constexpr int kThreads = 4;
  constexpr int kIterationsPerThread = 8;
  std::atomic<int> pass_count{0};
  std::atomic<int> fail_count{0};
  std::mutex fail_mu;
  std::string first_error;

  std::vector<std::thread> workers;
  workers.reserve(kThreads);

  for (int t = 0; t < kThreads; ++t) {
    workers.emplace_back([&, t]() {
      for (int i = 0; i < kIterationsPerThread; ++i) {
        for (const auto &c : cases) {
          char out_path[256];
          std::snprintf(out_path, sizeof(out_path), "out_test/stress_%s_t%02d_i%03d%s", c.tag,
                        t, i, c.ext);

          CWvExportOptions opt;
          opt.source_type = source_type;
          opt.export_format = c.format;
          opt.json_backend = c.json_backend;
          opt.table_name = "sample_data";
          opt.sheet_name = "ExportData";
          opt.output_path = out_path;
          opt.include_header = true;
          opt.enforce_source_index = true;

          CWvExport exporter;
          CWvExportResult res;
          if (!exporter.Export(db_path, mapping, opt, &res)) {
            fail_count.fetch_add(1, std::memory_order_relaxed);
            std::lock_guard<std::mutex> lock(fail_mu);
            if (first_error.empty()) {
              first_error = std::string("t=") + std::to_string(t) + " i=" +
                            std::to_string(i) + " case=" + c.tag + " err=" +
                            exporter.GetLastError();
            }
          } else {
            pass_count.fetch_add(1, std::memory_order_relaxed);
          }
        }
      }
    });
  }

  for (auto &w : workers) {
    w.join();
  }

  printf("[testExport] stress done: pass=%d fail=%d (threads=%d iter/thread=%d)\n",
         pass_count.load(), fail_count.load(), kThreads, kIterationsPerThread);
  if (fail_count.load() != 0) {
    fprintf(stderr, "[testExport] stress failed: %s\n",
            first_error.empty() ? "unknown failure" : first_error.c_str());
    return 71;
  }
  return 0;
}
} // namespace

int main(int argc, char **argv) {
  const char *db_path = kDefaultDbPath;
  const char *xlsx_path = kDefaultXlsxPath;
  const char *mode = "xlsx";
  const char *source = "sqlite";
  bool skip_seed = false;
  if (const char *env_skip = std::getenv("TESTEXPORT_SKIP_SEED")) {
    if (std::strcmp(env_skip, "1") == 0 || std::strcmp(env_skip, "true") == 0 ||
        std::strcmp(env_skip, "TRUE") == 0) {
      skip_seed = true;
    }
  }
  if (argc >= 2 && argv[1] != nullptr && argv[1][0] != '\0') {
    db_path = argv[1];
  }
  if (argc >= 3 && argv[2] != nullptr && argv[2][0] != '\0') {
    xlsx_path = argv[2];
  }
  if (argc >= 4 && argv[3] != nullptr && argv[3][0] != '\0') {
    mode = argv[3];
  }
  if (argc >= 5 && argv[4] != nullptr && argv[4][0] != '\0') {
    source = argv[4];
  }
  if (argc >= 6 && argv[5] != nullptr && argv[5][0] != '\0') {
    if (strcmp(argv[5], "skip-seed") == 0) {
      skip_seed = true;
    }
  }

  std::string all_sqlite_db_path = kDefaultDbPath;
  std::string all_duckdb_db_path = kDefaultDuckDbPath;
  if (strcmp(mode, "all") == 0) {
    all_sqlite_db_path = std::string(db_path) + ".all.sqlite.db";
    all_duckdb_db_path = std::string(db_path) + ".all.duckdb";
  }

  if (!skip_seed) {
    if (strcmp(mode, "all") != 0) {
      if (argc < 2 && strcmp(source, "duckdb") == 0) {
        db_path = kDefaultDuckDbPath;
      }

      if (strcmp(source, "sqlite") == 0) {
        if (create_seed_sqlite_db(db_path, kSeedRowCount) != 0) {
          fprintf(stderr, "[testExport] failed to create sqlite seed db.\n");
          return 1;
        }
      } else if (strcmp(source, "duckdb") == 0) {
        if (create_seed_duckdb_db(db_path, kSeedRowCount) != 0) {
          fprintf(stderr, "[testExport] failed to create duckdb seed db.\n");
          return 1;
        }
      } else {
        fprintf(stderr, "[testExport] unknown source: %s (use sqlite|duckdb)\n", source);
        return 6;
      }
    } else {
      std::error_code ec;
      std::filesystem::remove(all_sqlite_db_path, ec);
      std::filesystem::remove(all_sqlite_db_path + "-shm", ec);
      std::filesystem::remove(all_sqlite_db_path + "-wal", ec);
      std::filesystem::remove(all_duckdb_db_path, ec);

      if (create_seed_sqlite_db(all_sqlite_db_path.c_str(), kSeedRowCount) != 0) {
        fprintf(stderr, "[testExport] failed to create sqlite seed db.\n");
        return 1;
      }
      if (create_seed_duckdb_db(all_duckdb_db_path.c_str(), kSeedRowCount) != 0) {
        fprintf(stderr, "[testExport] failed to create duckdb seed db.\n");
        return 1;
      }
    }
  }
  if (cxlsx_init(kTestLicenseKey) != 0 || cxlsx_license_active() != 1) {
    fprintf(stderr, "[testExport] license activation failed.\n");
    return 2;
  }

  std::vector<CWvExportColumn> mapping = {
      {1, "name", 1, "UserName", 0, true},
      {2, "id", 0, "ID", 1, true},
      {3, "sql_text", 2, "SQL", 2, true},
      {4, "score", 3, "Score", 3, true},
  };

  CWvExportOptions opt;
  opt.source_type = (strcmp(source, "duckdb") == 0) ? CWvDataSourceType::DuckDb
                                                     : CWvDataSourceType::Sqlite;
  opt.export_format = CWvExportFormat::Xlsx;
  opt.table_name = "sample_data";
  opt.sheet_name = "ExportData";
  opt.output_path = xlsx_path;
  opt.include_header = true;
  opt.enforce_source_index = true;
  if (strcmp(mode, "all") == 0) {
    struct ExportCase {
      CWvDataSourceType source_type;
      CWvExportFormat format;
      CWvJsonBackend json_backend;
      const char *db;
      const char *out;
      const char *label;
    };
    const ExportCase cases[] = {
        {CWvDataSourceType::Sqlite, CWvExportFormat::Xlsx, CWvJsonBackend::RapidJson,
         all_sqlite_db_path.c_str(),
         "out_test/all_sqlite.xlsx", "sqlite+xlsx"},
        {CWvDataSourceType::Sqlite, CWvExportFormat::Json, CWvJsonBackend::RapidJson,
         all_sqlite_db_path.c_str(),
         "out_test/all_sqlite.json", "sqlite+json-rapid"},
        {CWvDataSourceType::Sqlite, CWvExportFormat::Json, CWvJsonBackend::YyJson,
         all_sqlite_db_path.c_str(),
         "out_test/all_sqlite_yy.json", "sqlite+json-yy"},
        {CWvDataSourceType::Sqlite, CWvExportFormat::Csv, CWvJsonBackend::RapidJson,
         all_sqlite_db_path.c_str(),
         "out_test/all_sqlite.csv", "sqlite+csv"},
        {CWvDataSourceType::DuckDb, CWvExportFormat::Xlsx, CWvJsonBackend::RapidJson,
         all_duckdb_db_path.c_str(),
         "out_test/all_duckdb.xlsx", "duckdb+xlsx"},
        {CWvDataSourceType::DuckDb, CWvExportFormat::Json, CWvJsonBackend::RapidJson,
         all_duckdb_db_path.c_str(),
         "out_test/all_duckdb.json", "duckdb+json-rapid"},
        {CWvDataSourceType::DuckDb, CWvExportFormat::Json, CWvJsonBackend::YyJson,
         all_duckdb_db_path.c_str(),
         "out_test/all_duckdb_yy.json", "duckdb+json-yy"},
        {CWvDataSourceType::DuckDb, CWvExportFormat::Csv, CWvJsonBackend::RapidJson,
         all_duckdb_db_path.c_str(),
         "out_test/all_duckdb.csv", "duckdb+csv"},
    };

    for (const auto &c : cases) {
      CWvExportOptions case_opt = opt;
      case_opt.source_type = c.source_type;
      case_opt.export_format = c.format;
      case_opt.output_path = c.out;
      case_opt.json_backend = c.json_backend;

      CWvExport exporter;
      CWvExportResult res;
      if (!exporter.Export(c.db, mapping, case_opt, &res)) {
        fprintf(stderr, "[testExport] %s export failed: %s\n", c.label,
                exporter.GetLastError().c_str());
        return 3;
      }

      int ok = 0;
      if (c.format == CWvExportFormat::Xlsx) {
        ok = verify_exported_xlsx(res.output_path.c_str(), kSeedRowCount);
      } else if (c.format == CWvExportFormat::Csv) {
        ok = verify_exported_csv(res.output_path.c_str(), kSeedRowCount);
      } else {
        ok = verify_exported_json(res.output_path.c_str());
      }
      if (ok != 0) {
        fprintf(stderr, "[testExport] %s verify failed.\n", c.label);
        return 4;
      }
      printf("[testExport] %s passed -> %s\n", c.label, res.output_path.c_str());
    }
    printf("[testExport] all 8 cases passed.\n");
    return 0;
  } else if (strcmp(mode, "cancel") == 0) {
    auto run_all_cancel_cases = [&](CWvDataSourceType st) -> int {
      struct CancelCase {
        CWvExportFormat format;
        CWvJsonBackend backend;
        const char *out;
        const char *keep_label;
        const char *delete_label;
      };
      const CancelCase cases[] = {
          {CWvExportFormat::Json, CWvJsonBackend::RapidJson, "out_test/cancel_test.json",
           "json-rapid-keep", "json-rapid-delete"},
          {CWvExportFormat::Json, CWvJsonBackend::YyJson, "out_test/cancel_test_yy.json",
           "json-yy-keep", "json-yy-delete"},
          {CWvExportFormat::Xlsx, CWvJsonBackend::RapidJson, "out_test/cancel_test.xlsx",
           "xlsx-keep", "xlsx-delete"},
      };
      for (const auto &c : cases) {
        int rc = run_cancel_export_test(db_path, st, mapping, c.format, c.backend, c.out,
                                        CWvCancelPolicy::KeepPartial, c.keep_label);
        if (rc != 0) {
          return rc;
        }
        rc = run_cancel_export_test(db_path, st, mapping, c.format, c.backend, c.out,
                                    CWvCancelPolicy::DeletePartial, c.delete_label);
        if (rc != 0) {
          return rc;
        }
      }
      return 0;
    };

    if (strcmp(source, "sqlite") == 0) {
      if (create_seed_sqlite_db(db_path, kCancelSeedRowCount) != 0) {
        fprintf(stderr, "[testExport] failed to create sqlite seed db for cancel test.\n");
        return 1;
      }
      return run_all_cancel_cases(CWvDataSourceType::Sqlite);
    }
    if (strcmp(source, "duckdb") == 0) {
      if (create_seed_duckdb_db(db_path, kCancelSeedRowCount) != 0) {
        fprintf(stderr, "[testExport] failed to create duckdb seed db for cancel test.\n");
        return 1;
      }
      return run_all_cancel_cases(CWvDataSourceType::DuckDb);
    }
    fprintf(stderr, "[testExport] unknown source: %s (use sqlite|duckdb)\n", source);
    return 6;
  } else if (strcmp(mode, "csv-check") == 0) {
    if (strcmp(source, "sqlite") == 0) {
      if (create_seed_sqlite_db(db_path, kCancelSeedRowCount) != 0) {
        fprintf(stderr, "[testExport] failed to create sqlite seed db for csv-check.\n");
        return 1;
      }
      return run_csv_focus_tests(db_path, CWvDataSourceType::Sqlite, mapping,
                                 kCancelSeedRowCount);
    }
    if (strcmp(source, "duckdb") == 0) {
      if (create_seed_duckdb_db(db_path, kCancelSeedRowCount) != 0) {
        fprintf(stderr, "[testExport] failed to create duckdb seed db for csv-check.\n");
        return 1;
      }
      return run_csv_focus_tests(db_path, CWvDataSourceType::DuckDb, mapping,
                                 kCancelSeedRowCount);
    }
    fprintf(stderr, "[testExport] unknown source: %s (use sqlite|duckdb)\n", source);
    return 6;
  } else if (strcmp(mode, "csv-unicode") == 0) {
    if (strcmp(source, "sqlite") == 0) {
      if (create_seed_sqlite_db(db_path, kSeedRowCount) != 0) {
        fprintf(stderr, "[testExport] failed to create sqlite seed db for csv-unicode.\n");
        return 1;
      }
      return run_csv_unicode_tests(db_path, CWvDataSourceType::Sqlite, mapping);
    }
    if (strcmp(source, "duckdb") == 0) {
      if (create_seed_duckdb_db(db_path, kSeedRowCount) != 0) {
        fprintf(stderr, "[testExport] failed to create duckdb seed db for csv-unicode.\n");
        return 1;
      }
      return run_csv_unicode_tests(db_path, CWvDataSourceType::DuckDb, mapping);
    }
    fprintf(stderr, "[testExport] unknown source: %s (use sqlite|duckdb)\n", source);
    return 6;
  } else if (strcmp(mode, "split-check") == 0) {
    int expected_rows = 0;
    if (strcmp(source, "sqlite") == 0) {
      if (create_seed_sqlite_db(db_path, kSeedRowCount) != 0) {
        fprintf(stderr, "[testExport] failed to create sqlite seed db for split-check.\n");
        return 1;
      }
      if (get_sqlite_row_count(db_path, &expected_rows) != 0) {
        fprintf(stderr, "[testExport] failed to read sqlite row count for split-check.\n");
        return 1;
      }
      return run_split_focus_tests(db_path, CWvDataSourceType::Sqlite, mapping, expected_rows);
    }
    if (strcmp(source, "duckdb") == 0) {
      if (create_seed_duckdb_db(db_path, kSeedRowCount) != 0) {
        fprintf(stderr, "[testExport] failed to create duckdb seed db for split-check.\n");
        return 1;
      }
      if (get_duckdb_row_count(db_path, &expected_rows) != 0) {
        fprintf(stderr, "[testExport] failed to read duckdb row count for split-check.\n");
        return 1;
      }
      return run_split_focus_tests(db_path, CWvDataSourceType::DuckDb, mapping, expected_rows);
    }
    fprintf(stderr, "[testExport] unknown source: %s (use sqlite|duckdb)\n", source);
    return 6;
  } else if (strcmp(mode, "clipboard-check") == 0) {
    if (strcmp(source, "sqlite") == 0) {
      if (create_seed_sqlite_db(db_path, 25000) != 0) {
        fprintf(stderr, "[testExport] failed to create sqlite seed db for clipboard-check.\n");
        return 1;
      }
      return run_clipboard_focus_tests(db_path, CWvDataSourceType::Sqlite, mapping);
    }
    if (strcmp(source, "duckdb") == 0) {
      if (create_seed_duckdb_db(db_path, 25000) != 0) {
        fprintf(stderr, "[testExport] failed to create duckdb seed db for clipboard-check.\n");
        return 1;
      }
      return run_clipboard_focus_tests(db_path, CWvDataSourceType::DuckDb, mapping);
    }
    fprintf(stderr, "[testExport] unknown source: %s (use sqlite|duckdb)\n", source);
    return 6;
  } else if (strcmp(mode, "provider-check") == 0) {
    if (strcmp(source, "sqlite") == 0) {
      if (create_seed_sqlite_db(db_path, kSeedRowCount) != 0) {
        fprintf(stderr, "[testExport] failed to create sqlite seed db for provider-check.\n");
        return 1;
      }
      return run_provider_focus_tests(db_path, CWvDataSourceType::Sqlite, mapping);
    }
    if (strcmp(source, "duckdb") == 0) {
      if (create_seed_duckdb_db(db_path, kSeedRowCount) != 0) {
        fprintf(stderr, "[testExport] failed to create duckdb seed db for provider-check.\n");
        return 1;
      }
      return run_provider_focus_tests(db_path, CWvDataSourceType::DuckDb, mapping);
    }
    fprintf(stderr, "[testExport] unknown source: %s (use sqlite|duckdb)\n", source);
    return 6;
  } else if (strcmp(mode, "writer-check") == 0) {
    if (strcmp(source, "sqlite") == 0) {
      if (create_seed_sqlite_db(db_path, kSeedRowCount) != 0) {
        fprintf(stderr, "[testExport] failed to create sqlite seed db for writer-check.\n");
        return 1;
      }
      return run_writer_focus_tests(db_path, CWvDataSourceType::Sqlite, mapping, kSeedRowCount);
    }
    if (strcmp(source, "duckdb") == 0) {
      if (create_seed_duckdb_db(db_path, kSeedRowCount) != 0) {
        fprintf(stderr, "[testExport] failed to create duckdb seed db for writer-check.\n");
        return 1;
      }
      return run_writer_focus_tests(db_path, CWvDataSourceType::DuckDb, mapping, kSeedRowCount);
    }
    fprintf(stderr, "[testExport] unknown source: %s (use sqlite|duckdb)\n", source);
    return 6;
  } else if (strcmp(mode, "stress") == 0) {
    if (strcmp(source, "sqlite") == 0) {
      if (create_seed_sqlite_db(db_path, kSeedRowCount) != 0) {
        fprintf(stderr, "[testExport] failed to create sqlite seed db for stress.\n");
        return 1;
      }
      return run_internal_stress_tests(db_path, CWvDataSourceType::Sqlite, mapping);
    }
    if (strcmp(source, "duckdb") == 0) {
      if (create_seed_duckdb_db(db_path, kSeedRowCount) != 0) {
        fprintf(stderr, "[testExport] failed to create duckdb seed db for stress.\n");
        return 1;
      }
      return run_internal_stress_tests(db_path, CWvDataSourceType::DuckDb, mapping);
    }
    fprintf(stderr, "[testExport] unknown source: %s (use sqlite|duckdb)\n", source);
    return 6;
  } else if (strcmp(mode, "json-rapid") == 0) {
    opt.export_format = CWvExportFormat::Json;
    opt.json_backend = CWvJsonBackend::RapidJson;
  } else if (strcmp(mode, "json-yy") == 0) {
    opt.export_format = CWvExportFormat::Json;
    opt.json_backend = CWvJsonBackend::YyJson;
  } else if (strcmp(mode, "csv") == 0) {
    opt.export_format = CWvExportFormat::Csv;
  } else if (strcmp(mode, "clipboard") == 0) {
    opt.export_format = CWvExportFormat::Clipboard;
    opt.max_clipboard_bytes = 16u * 1024u * 1024u;
  } else if (strcmp(mode, "clipboard-chunk") == 0) {
    opt.export_format = CWvExportFormat::Clipboard;
    opt.max_clipboard_bytes = 16u * 1024u * 1024u;
    opt.max_clipboard_rows = 1000000;
    opt.clipboard_chunk_rows = 10000;
    opt.clipboard_chunk_confirm = [](int copied_rows, int chunk_rows) {
      std::printf("[testExport] %d rows copied to clipboard. Copy next %d rows? [y/N]: ",
                  copied_rows, chunk_rows);
      std::fflush(stdout);
      char answer[8] = {0};
      if (std::fgets(answer, sizeof(answer), stdin) == nullptr) {
        return CWvClipboardChunkAction::Stop;
      }
      const char c = static_cast<char>(std::tolower(static_cast<unsigned char>(answer[0])));
      return (c == 'y') ? CWvClipboardChunkAction::Continue
                        : CWvClipboardChunkAction::Stop;
    };
  } else if (strcmp(mode, "clipboard-limit") == 0) {
    opt.export_format = CWvExportFormat::Clipboard;
    opt.max_clipboard_bytes = 256;
  } else if (strcmp(mode, "xlsx") != 0) {
    fprintf(stderr, "[testExport] unknown mode: %s (use xlsx|csv|csv-check|csv-unicode|split-check|clipboard-check|provider-check|writer-check|stress|json-rapid|json-yy|clipboard|clipboard-chunk|clipboard-limit|all|cancel)\n",
            mode);
    return 5;
  }

  CWvExport exporter;
  CWvExportResult res;
  if (!exporter.Export(db_path, mapping, opt, &res)) {
    if (strcmp(mode, "clipboard-limit") == 0 &&
        exporter.GetLastError().find("max_clipboard_bytes") != std::string::npos) {
      printf("[testExport] clipboard-limit negative test passed.\n");
      return 0;
    }
    fprintf(stderr, "[testExport] export failed: %s\n",
            exporter.GetLastError().c_str());
    return 3;
  }
  if (strcmp(mode, "clipboard-limit") == 0) {
    fprintf(stderr, "[testExport] clipboard-limit expected failure but export succeeded.\n");
    return 11;
  }
  printf("[testExport] exported rows=%d cols=%d file=%s\n", res.rows_exported,
         res.columns_exported, res.output_path.c_str());
  if (opt.export_format == CWvExportFormat::Clipboard) {
    if (res.stopped_by_user) {
      printf("[testExport] clipboard chunk copy stopped by user. rows=%d chunks=%d\n",
             res.rows_exported, res.clipboard_chunks_copied);
    }
    printf("[testExport] clipboard export passed.\n");
    return 0;
  }
  if (opt.export_format == CWvExportFormat::Xlsx) {
    if (verify_exported_xlsx(res.output_path.c_str(), kSeedRowCount) != 0) {
      fprintf(stderr, "[testExport] verify failed.\n");
      return 4;
    }
  } else if (opt.export_format == CWvExportFormat::Csv) {
    if (verify_exported_csv(res.output_path.c_str(), kSeedRowCount) != 0) {
      fprintf(stderr, "[testExport] verify failed.\n");
      return 4;
    }
  } else {
    if (verify_exported_json(res.output_path.c_str()) != 0) {
      fprintf(stderr, "[testExport] verify failed.\n");
      return 4;
    }
  }
  printf("[testExport] verify passed.\n");
  return 0;
}

