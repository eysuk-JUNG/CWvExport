#include "CWvExport.h"

#include "cxlsx/cxlsx.h"

#include <sqlite3.h>

#include <cstdio>
#include <cstring>
#include <vector>

namespace {
constexpr const char *kDefaultDbPath = "test_export_cache.db";
constexpr const char *kDefaultXlsxPath = "test_export_result.xlsx";
constexpr int kSeedRowCount = 10000;
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
} // namespace

int main(int argc, char **argv) {
  const char *db_path = kDefaultDbPath;
  const char *xlsx_path = kDefaultXlsxPath;
  if (argc >= 2 && argv[1] != nullptr && argv[1][0] != '\0') {
    db_path = argv[1];
  }
  if (argc >= 3 && argv[2] != nullptr && argv[2][0] != '\0') {
    xlsx_path = argv[2];
  }

  if (create_seed_sqlite_db(db_path, kSeedRowCount) != 0) {
    fprintf(stderr, "[testExport] failed to create sqlite seed db.\n");
    return 1;
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
  opt.source_type = CWvDataSourceType::Sqlite;
  opt.export_format = CWvExportFormat::Xlsx;
  opt.table_name = "sample_data";
  opt.sheet_name = "ExportData";
  opt.output_path = xlsx_path;
  opt.include_header = true;
  opt.enforce_source_index = true;

  CWvExport exporter;
  CWvExportResult res;
  if (!exporter.Export(db_path, mapping, opt, &res)) {
    fprintf(stderr, "[testExport] export failed: %s\n",
            exporter.GetLastError().c_str());
    return 3;
  }
  printf("[testExport] exported rows=%d cols=%d file=%s\n", res.rows_exported,
         res.columns_exported, res.output_path.c_str());
  if (verify_exported_xlsx(res.output_path.c_str(), kSeedRowCount) != 0) {
    fprintf(stderr, "[testExport] verify failed.\n");
    return 4;
  }
  printf("[testExport] verify passed.\n");
  return 0;
}
