#include "CWvExport.h"

#include "cxlsx/cxlsx.h"

#include <duckdb.h>
#include <sqlite3.h>

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

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

  // 3) ANSI mode
  {
    opt.output_path = "out_test/csv_focus_ansi.csv";
    opt.csv_use_utf8 = false;
    opt.csv_use_crlf = true;
    opt.max_rows_per_file = 0;
    CWvExport exporter;
    CWvExportResult res;
    if (!exporter.Export(db_path, mapping, opt, &res)) {
      fprintf(stderr, "[testExport] csv-focus ansi export failed: %s\n",
              exporter.GetLastError().c_str());
      return 25;
    }
    if (verify_exported_csv(res.output_path.c_str(), expected_rows) != 0) {
      fprintf(stderr, "[testExport] csv-focus ansi verify failed.\n");
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
} // namespace

int main(int argc, char **argv) {
  const char *db_path = kDefaultDbPath;
  const char *xlsx_path = kDefaultXlsxPath;
  const char *mode = "xlsx";
  const char *source = "sqlite";
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
    std::filesystem::remove(kDefaultDbPath, ec);
    std::filesystem::remove(std::string(kDefaultDbPath) + "-shm", ec);
    std::filesystem::remove(std::string(kDefaultDbPath) + "-wal", ec);
    std::filesystem::remove(kDefaultDuckDbPath, ec);

    if (create_seed_sqlite_db(kDefaultDbPath, kSeedRowCount) != 0) {
      fprintf(stderr, "[testExport] failed to create sqlite seed db.\n");
      return 1;
    }
    if (create_seed_duckdb_db(kDefaultDuckDbPath, kSeedRowCount) != 0) {
      fprintf(stderr, "[testExport] failed to create duckdb seed db.\n");
      return 1;
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
         kDefaultDbPath,
         "out_test/all_sqlite.xlsx", "sqlite+xlsx"},
        {CWvDataSourceType::Sqlite, CWvExportFormat::Json, CWvJsonBackend::RapidJson,
         kDefaultDbPath,
         "out_test/all_sqlite.json", "sqlite+json-rapid"},
        {CWvDataSourceType::Sqlite, CWvExportFormat::Json, CWvJsonBackend::YyJson,
         kDefaultDbPath,
         "out_test/all_sqlite_yy.json", "sqlite+json-yy"},
        {CWvDataSourceType::Sqlite, CWvExportFormat::Csv, CWvJsonBackend::RapidJson,
         kDefaultDbPath,
         "out_test/all_sqlite.csv", "sqlite+csv"},
        {CWvDataSourceType::DuckDb, CWvExportFormat::Xlsx, CWvJsonBackend::RapidJson,
         kDefaultDuckDbPath,
         "out_test/all_duckdb.xlsx", "duckdb+xlsx"},
        {CWvDataSourceType::DuckDb, CWvExportFormat::Json, CWvJsonBackend::RapidJson,
         kDefaultDuckDbPath,
         "out_test/all_duckdb.json", "duckdb+json-rapid"},
        {CWvDataSourceType::DuckDb, CWvExportFormat::Json, CWvJsonBackend::YyJson,
         kDefaultDuckDbPath,
         "out_test/all_duckdb_yy.json", "duckdb+json-yy"},
        {CWvDataSourceType::DuckDb, CWvExportFormat::Csv, CWvJsonBackend::RapidJson,
         kDefaultDuckDbPath,
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
    int rc = 0;
    if (strcmp(source, "sqlite") == 0) {
      if (create_seed_sqlite_db(db_path, kCancelSeedRowCount) != 0) {
        fprintf(stderr, "[testExport] failed to create sqlite seed db for cancel test.\n");
        return 1;
      }
      rc = run_cancel_export_test(db_path, CWvDataSourceType::Sqlite, mapping,
                                  CWvExportFormat::Json, CWvJsonBackend::RapidJson,
                                  "out_test/cancel_test.json", CWvCancelPolicy::KeepPartial,
                                  "keep");
      if (rc != 0) {
        return rc;
      }
      rc = run_cancel_export_test(db_path, CWvDataSourceType::Sqlite, mapping,
                                  CWvExportFormat::Json, CWvJsonBackend::RapidJson,
                                  "out_test/cancel_test.json", CWvCancelPolicy::DeletePartial,
                                  "delete");
      return rc;
    }
    if (strcmp(source, "duckdb") == 0) {
      if (create_seed_duckdb_db(db_path, kCancelSeedRowCount) != 0) {
        fprintf(stderr, "[testExport] failed to create duckdb seed db for cancel test.\n");
        return 1;
      }
      rc = run_cancel_export_test(db_path, CWvDataSourceType::DuckDb, mapping,
                                  CWvExportFormat::Json, CWvJsonBackend::RapidJson,
                                  "out_test/cancel_test.json", CWvCancelPolicy::KeepPartial,
                                  "keep");
      if (rc != 0) {
        return rc;
      }
      rc = run_cancel_export_test(db_path, CWvDataSourceType::DuckDb, mapping,
                                  CWvExportFormat::Json, CWvJsonBackend::RapidJson,
                                  "out_test/cancel_test.json", CWvCancelPolicy::DeletePartial,
                                  "delete");
      return rc;
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
  } else if (strcmp(mode, "json-rapid") == 0) {
    opt.export_format = CWvExportFormat::Json;
    opt.json_backend = CWvJsonBackend::RapidJson;
  } else if (strcmp(mode, "json-yy") == 0) {
    opt.export_format = CWvExportFormat::Json;
    opt.json_backend = CWvJsonBackend::YyJson;
  } else if (strcmp(mode, "csv") == 0) {
    opt.export_format = CWvExportFormat::Csv;
  } else if (strcmp(mode, "csv-ansi") == 0) {
    opt.export_format = CWvExportFormat::Csv;
    opt.csv_use_utf8 = false;
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
    fprintf(stderr, "[testExport] unknown mode: %s (use xlsx|csv|csv-ansi|csv-check|split-check|clipboard-check|json-rapid|json-yy|clipboard|clipboard-chunk|clipboard-limit|all|cancel)\n",
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
