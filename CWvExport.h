#ifndef CWV_EXPORT_H
#define CWV_EXPORT_H

/* This component requires C++17 or later. */

#include <string>
#include <vector>

enum class CWvDataSourceType {
  Sqlite = 0,
  DuckDb = 1,
  FileDbGeneric = 2
};

enum class CWvExportFormat {
  Xlsx = 0,
  Csv = 1,
  Json = 2,
  Other = 3
};

enum class CWvJsonBackend {
  RapidJson = 0,
  YyJson = 1
};

struct CWvExportColumn {
  int id = 0;
  std::string source_name;
  int source_index = -1;
  std::string header_name;
  int export_order = 0;
  bool enabled = true;
};

struct CWvExportOptions {
  CWvDataSourceType source_type = CWvDataSourceType::Sqlite;
  CWvExportFormat export_format = CWvExportFormat::Xlsx;
  std::string table_name = "sample_data";
  std::string sheet_name = "sample_data";
  std::string output_path;
  std::string password;
  bool include_header = true;
  bool enforce_source_index = false;
  bool large_integer_as_text = true;
  CWvJsonBackend json_backend = CWvJsonBackend::RapidJson;
};

struct CWvExportResult {
  int rows_exported = 0;
  int columns_exported = 0;
  std::string output_path;
};

class CWvExport {
public:
  bool Export(const std::string &source_path,
              const std::vector<CWvExportColumn> &mapping,
              const CWvExportOptions &options,
              CWvExportResult *result = nullptr);
  bool ExportSqliteToXlsx(const std::string &sqlite_path,
                          const std::vector<CWvExportColumn> &mapping,
                          const CWvExportOptions &options,
                          CWvExportResult *result = nullptr);
  bool ExportSqliteToJson(const std::string &sqlite_path,
                          const std::vector<CWvExportColumn> &mapping,
                          const CWvExportOptions &options,
                          CWvExportResult *result = nullptr);
  bool ExportDuckDbToXlsx(const std::string &duckdb_path,
                          const std::vector<CWvExportColumn> &mapping,
                          const CWvExportOptions &options,
                          CWvExportResult *result = nullptr);
  bool ExportDuckDbToJson(const std::string &duckdb_path,
                          const std::vector<CWvExportColumn> &mapping,
                          const CWvExportOptions &options,
                          CWvExportResult *result = nullptr);

  const std::string &GetLastError() const;
  const std::string &GetLastWarning() const;

private:
  enum class JobState {
    Created = 0,
    Running = 1,
    Finished = 2
  };

  JobState state_ = JobState::Created;
  std::string last_error_;
  std::string last_warning_;
};

#endif
