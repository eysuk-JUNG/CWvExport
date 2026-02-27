#include "CWvExport.h"

#include "core/ExportCoreTypes.h"
#include "providers/DuckDbProvider.h"
#include "providers/IDataSourceProvider.h"
#include "providers/SqliteProvider.h"
#include "writers/IExportWriter.h"
#include "writers/JsonWriterRapid.h"
#include "writers/JsonWriterYy.h"
#include "writers/XlsxWriter.h"

#include <algorithm>
#include <cctype>
#include <cstdio>
#include <filesystem>
#include <memory>
#include <string>
#include <system_error>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <windows.h>

namespace {
std::string trim_ascii(const std::string &s) {
  size_t begin = 0;
  while (begin < s.size() && std::isspace(static_cast<unsigned char>(s[begin])) != 0) {
    ++begin;
  }
  size_t end = s.size();
  while (end > begin &&
         std::isspace(static_cast<unsigned char>(s[end - 1])) != 0) {
    --end;
  }
  return s.substr(begin, end - begin);
}

bool prepare_output_path(const std::string &raw, CWvExportFormat format,
                         std::string *normalized, std::string *err) {
  std::string trimmed = trim_ascii(raw);
  if (trimmed.empty()) {
    *err = "output_path is empty.";
    return false;
  }

  std::filesystem::path out_path(trimmed);
  if (out_path.has_filename() == false) {
    *err = "output_path must include file name.";
    return false;
  }

  if (format == CWvExportFormat::Xlsx && out_path.extension().empty()) {
    out_path.replace_extension(".xlsx");
  } else if (format == CWvExportFormat::Csv && out_path.extension().empty()) {
    out_path.replace_extension(".csv");
  } else if (format == CWvExportFormat::Json && out_path.extension().empty()) {
    out_path.replace_extension(".json");
  }

  std::error_code ec;
  if (out_path.is_relative()) {
    out_path = std::filesystem::absolute(out_path, ec);
    if (ec) {
      *err = "failed to resolve absolute output_path: " + ec.message();
      return false;
    }
  }

  const std::filesystem::path parent = out_path.parent_path();
  if (!parent.empty() && !std::filesystem::exists(parent, ec)) {
    ec.clear();
    if (!std::filesystem::create_directories(parent, ec) && ec) {
      *err = "failed to create output directory: " + parent.string() +
             " (" + ec.message() + ")";
      return false;
    }
  }

  *normalized = out_path.string();
  return true;
}

std::string make_unique_temp_path(const std::string &final_path) {
  static volatile LONG s_temp_seq = 0;
  const unsigned long pid = GetCurrentProcessId();
  const unsigned long tid = GetCurrentThreadId();
  const unsigned long seq =
      static_cast<unsigned long>(InterlockedIncrement(&s_temp_seq));
  const unsigned long long tick = GetTickCount64();
  return final_path + ".tmp." + std::to_string(pid) + "." + std::to_string(tid) +
         "." + std::to_string(tick) + "." + std::to_string(seq);
}

bool replace_file_safely(const std::string &from, const std::string &to,
                         std::string *err) {
  if (MoveFileExA(from.c_str(), to.c_str(),
                  MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH) == 0) {
    const DWORD win_err = GetLastError();
    *err = "failed to replace output file. GetLastError=" +
           std::to_string(static_cast<unsigned long>(win_err));
    return false;
  }
  return true;
}

bool resolve_columns(const std::vector<DbColumnInfo> &table_cols,
                     const std::vector<CWvExportColumn> &mapping,
                     const CWvExportOptions &opt,
                     std::vector<ResolvedColumn> *resolved, std::string *warn,
                     std::string *err) {
  std::unordered_map<std::string, DbColumnInfo> by_name;
  for (const auto &c : table_cols) {
    by_name[c.name] = c;
  }

  resolved->clear();
  warn->clear();

  std::unordered_set<int> used_ids;
  std::unordered_set<int> used_orders;
  std::unordered_set<std::string> used_headers;

  auto add_unique = [&](const CWvExportColumn &m, std::string *out_err) -> bool {
    if (!used_ids.insert(m.id).second) {
      *out_err = "duplicate mapping id: " + std::to_string(m.id);
      return false;
    }
    if (!used_orders.insert(m.export_order).second) {
      *out_err = "duplicate export_order: " + std::to_string(m.export_order);
      return false;
    }
    if (!used_headers.insert(m.header_name).second) {
      *out_err = "duplicate header_name: " + m.header_name;
      return false;
    }
    return true;
  };

  if (mapping.empty()) {
    for (const auto &c : table_cols) {
      ResolvedColumn rc;
      rc.map.id = c.cid;
      rc.map.source_name = c.name;
      rc.map.source_index = c.cid;
      rc.map.header_name = c.name;
      rc.map.export_order = c.cid;
      rc.map.enabled = true;
      rc.actual_cid = c.cid;
      std::string dup_err;
      if (!add_unique(rc.map, &dup_err)) {
        *err = dup_err;
        return false;
      }
      resolved->push_back(rc);
    }
    return true;
  }

  for (const auto &m : mapping) {
    if (!m.enabled) {
      continue;
    }
    if (m.source_name.empty()) {
      *err = "Mapping contains empty source_name.";
      return false;
    }
    auto it = by_name.find(m.source_name);
    if (it == by_name.end()) {
      *err = "Mapped column not found in table: " + m.source_name;
      return false;
    }

    ResolvedColumn rc;
    rc.map = m;
    rc.actual_cid = it->second.cid;
    if (rc.map.header_name.empty()) {
      rc.map.header_name = rc.map.source_name;
    }
    if (opt.enforce_source_index && rc.map.source_index >= 0 &&
        rc.map.source_index != rc.actual_cid) {
      *err = "source_index mismatch for column: " + rc.map.source_name;
      return false;
    }
    if (!opt.enforce_source_index && rc.map.source_index >= 0 &&
        rc.map.source_index != rc.actual_cid) {
      if (!warn->empty()) {
        *warn += "; ";
      }
      *warn += "source_index mismatch ignored: " + rc.map.source_name;
    }

    std::string dup_err;
    if (!add_unique(rc.map, &dup_err)) {
      *err = dup_err;
      return false;
    }
    resolved->push_back(rc);
  }

  if (resolved->empty()) {
    *err = "No enabled columns in mapping.";
    return false;
  }

  std::stable_sort(resolved->begin(), resolved->end(),
                   [](const ResolvedColumn &a, const ResolvedColumn &b) {
                     if (a.map.export_order != b.map.export_order) {
                       return a.map.export_order < b.map.export_order;
                     }
                     return a.map.id < b.map.id;
                   });
  return true;
}

std::unique_ptr<IExportWriter> make_writer(const CWvExportOptions &options,
                                           std::string *err) {
  if (options.export_format == CWvExportFormat::Xlsx) {
    return std::make_unique<XlsxWriter>();
  }
  if (options.export_format == CWvExportFormat::Json) {
    if (options.json_backend == CWvJsonBackend::RapidJson) {
      return std::make_unique<JsonWriterRapid>();
    }
    if (options.json_backend == CWvJsonBackend::YyJson) {
      return std::make_unique<JsonWriterYy>();
    }
    *err = "json backend is not supported.";
    return nullptr;
  }
  if (options.export_format == CWvExportFormat::Csv) {
    *err = "export_format Csv is not implemented yet.";
    return nullptr;
  }
  *err = "export_format is not supported.";
  return nullptr;
}

std::unique_ptr<IDataSourceProvider> make_provider(CWvDataSourceType source_type,
                                                   std::string *err) {
  if (source_type == CWvDataSourceType::Sqlite) {
    return std::make_unique<SqliteProvider>();
  }
  if (source_type == CWvDataSourceType::DuckDb) {
    return std::make_unique<DuckDbProvider>();
  }
  *err = "source_type is not implemented yet. Current implementation: Sqlite, DuckDb.";
  return nullptr;
}
} // namespace

bool CWvExport::Export(const std::string &source_path,
                       const std::vector<CWvExportColumn> &mapping,
                       const CWvExportOptions &options,
                       CWvExportResult *result) {
  if (state_ != JobState::Created) {
    last_error_ = "CWvExport is single-shot. Create new instance per job.";
    return false;
  }
  state_ = JobState::Running;
  struct Finalize {
    CWvExport *self;
    ~Finalize() { self->state_ = JobState::Finished; }
  } finalize{this};

  last_error_.clear();
  last_warning_.clear();
  if (result) {
    *result = CWvExportResult{};
  }

  if (source_path.empty()) {
    last_error_ = "source_path is empty.";
    return false;
  }
  if (options.output_path.empty()) {
    last_error_ = "output_path is empty.";
    return false;
  }
  if (options.table_name.empty()) {
    last_error_ = "table_name is empty.";
    return false;
  }

  if (options.export_format != CWvExportFormat::Xlsx &&
      options.export_format != CWvExportFormat::Json) {
    last_error_ =
        "export_format is not implemented yet. Current implementation: Xlsx, Json.";
    return false;
  }

  CWvExportOptions runtime_opt = options;
  std::string final_path;
  if (!prepare_output_path(options.output_path, runtime_opt.export_format,
                           &final_path, &last_error_)) {
    return false;
  }
  const std::string temp_path = make_unique_temp_path(final_path);
  runtime_opt.output_path = temp_path;

  std::string err;
  std::unique_ptr<IDataSourceProvider> provider = make_provider(runtime_opt.source_type, &err);
  if (!provider) {
    last_error_ = err;
    return false;
  }
  std::unique_ptr<IExportWriter> writer = make_writer(runtime_opt, &err);
  if (!writer) {
    last_error_ = err;
    return false;
  }
  std::vector<DbColumnInfo> table_cols;
  std::vector<ResolvedColumn> resolved;
  std::string resolve_warn;
  std::string resolve_err;

  if (!provider->OpenReadOnly(source_path, &err) ||
      !provider->LoadTableSchema(runtime_opt.table_name, &table_cols, &err)) {
    last_error_ = err;
    return false;
  }
  if (!resolve_columns(table_cols, mapping, runtime_opt, &resolved, &resolve_warn,
                       &resolve_err)) {
    last_error_ = resolve_err;
    return false;
  }
  last_warning_ = resolve_warn;
  if (!provider->PrepareSelect(runtime_opt.table_name, resolved, &err)) {
    last_error_ = err;
    return false;
  }
  if (!writer->Begin(runtime_opt, resolved, &err)) {
    last_error_ = err;
    return false;
  }

  int row = 0;
  if (runtime_opt.include_header) {
    if (!writer->WriteHeader(resolved, &err)) {
      last_error_ = err;
      std::remove(temp_path.c_str());
      return false;
    }
    row = 1;
  }

  bool has_row = false;
  while (provider->Step(&has_row, &err) && has_row) {
    for (int i = 0; i < (int)resolved.size(); ++i) {
      if (!writer->WriteCell(row, i, provider->ValueType(i), provider->ValueInt64(i),
                             provider->ValueDouble(i), provider->ValueText(i),
                             provider->ValueBytes(i), &err)) {
        last_error_ = err;
        std::remove(temp_path.c_str());
        return false;
      }
    }
    ++row;
  }
  if (!err.empty()) {
    last_error_ = err;
    std::remove(temp_path.c_str());
    return false;
  }
  if (!writer->End(&err)) {
    last_error_ = err;
    std::remove(temp_path.c_str());
    return false;
  }

  if (!replace_file_safely(temp_path, final_path, &last_error_)) {
    std::remove(temp_path.c_str());
    return false;
  }

  if (result) {
    result->rows_exported = row - (runtime_opt.include_header ? 1 : 0);
    result->columns_exported = (int)resolved.size();
    result->output_path = final_path;
  }
  return true;
}

bool CWvExport::ExportSqliteToXlsx(const std::string &sqlite_path,
                                   const std::vector<CWvExportColumn> &mapping,
                                   const CWvExportOptions &options,
                                   CWvExportResult *result) {
  CWvExportOptions forced = options;
  forced.source_type = CWvDataSourceType::Sqlite;
  forced.export_format = CWvExportFormat::Xlsx;
  return Export(sqlite_path, mapping, forced, result);
}

bool CWvExport::ExportSqliteToJson(const std::string &sqlite_path,
                                   const std::vector<CWvExportColumn> &mapping,
                                   const CWvExportOptions &options,
                                   CWvExportResult *result) {
  CWvExportOptions forced = options;
  forced.source_type = CWvDataSourceType::Sqlite;
  forced.export_format = CWvExportFormat::Json;
  return Export(sqlite_path, mapping, forced, result);
}

bool CWvExport::ExportDuckDbToXlsx(const std::string &duckdb_path,
                                   const std::vector<CWvExportColumn> &mapping,
                                   const CWvExportOptions &options,
                                   CWvExportResult *result) {
  CWvExportOptions forced = options;
  forced.source_type = CWvDataSourceType::DuckDb;
  forced.export_format = CWvExportFormat::Xlsx;
  return Export(duckdb_path, mapping, forced, result);
}

bool CWvExport::ExportDuckDbToJson(const std::string &duckdb_path,
                                   const std::vector<CWvExportColumn> &mapping,
                                   const CWvExportOptions &options,
                                   CWvExportResult *result) {
  CWvExportOptions forced = options;
  forced.source_type = CWvDataSourceType::DuckDb;
  forced.export_format = CWvExportFormat::Json;
  return Export(duckdb_path, mapping, forced, result);
}

const std::string &CWvExport::GetLastError() const { return last_error_; }
const std::string &CWvExport::GetLastWarning() const { return last_warning_; }
