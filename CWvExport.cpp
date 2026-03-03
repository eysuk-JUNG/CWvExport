#include "CWvExport.h"

#include "core/ExportCoreTypes.h"
#include "providers/DuckDbProvider.h"
#include "providers/IDataSourceProvider.h"
#include "providers/SqliteProvider.h"
#include "writers/IExportWriter.h"
#include "writers/ClipboardWriter.h"
#include "writers/CsvWriter.h"
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
#include <rpc.h>

#pragma comment(lib, "Rpcrt4.lib")

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

  std::filesystem::path out_path;
  try {
    out_path = std::filesystem::u8path(trimmed);
  } catch (...) {
    *err = "invalid output_path encoding.";
    return false;
  }
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
#if defined(_WIN32)
  if (!parent.empty()) {
    const std::wstring parent_w = parent.wstring();
    const DWORD parent_attr = GetFileAttributesW(parent_w.c_str());
    if (parent_attr != INVALID_FILE_ATTRIBUTES &&
        (parent_attr & FILE_ATTRIBUTE_REPARSE_POINT) != 0) {
      *err = "output_path parent must not be a reparse point.";
      return false;
    }
  }
#endif
  if (std::filesystem::exists(out_path, ec) && std::filesystem::is_directory(out_path, ec)) {
    *err = "output_path must be a file path, not a directory.";
    return false;
  }
#if defined(_WIN32)
  if (std::filesystem::exists(out_path, ec)) {
    const std::wstring out_w = out_path.wstring();
    const DWORD out_attr = GetFileAttributesW(out_w.c_str());
    if (out_attr != INVALID_FILE_ATTRIBUTES &&
        (out_attr & FILE_ATTRIBUTE_REPARSE_POINT) != 0) {
      *err = "output_path must not target a reparse point.";
      return false;
    }
  }
#endif

  *normalized = out_path.string();
  return true;
}

std::string make_unique_temp_path(const std::string &final_path) {
  const std::filesystem::path final_p(final_path);
  const std::string stem = final_p.filename().string();
  const std::filesystem::path dir = final_p.parent_path();
  for (int attempt = 0; attempt < 32; ++attempt) {
    UUID uuid{};
    const RPC_STATUS uuid_rc = UuidCreate(&uuid);
    if (uuid_rc != RPC_S_OK && uuid_rc != RPC_S_UUID_LOCAL_ONLY) {
      continue;
    }

    RPC_CSTR uuid_text = nullptr;
    if (UuidToStringA(&uuid, &uuid_text) != RPC_S_OK || uuid_text == nullptr) {
      continue;
    }

    const std::string candidate_name =
        stem + ".tmp." + reinterpret_cast<const char *>(uuid_text);
    RpcStringFreeA(&uuid_text);
    return (dir / candidate_name).string();
  }

  static volatile LONG s_temp_seq = 0;
  const unsigned long seq =
      static_cast<unsigned long>(InterlockedIncrement(&s_temp_seq));
  return final_path + ".tmp.fallback." + std::to_string(seq);
}

std::string make_part_output_path(const std::string &base_path, int part_index,
                                  bool split_enabled) {
  if (!split_enabled) {
    return base_path;
  }
  std::filesystem::path p(base_path);
  const std::string stem = p.stem().string();
  const std::string ext = p.extension().string();
  char part_buf[32];
  std::snprintf(part_buf, sizeof(part_buf), ".part%04d", part_index);
  return (p.parent_path() / (stem + part_buf + ext)).string();
}

bool replace_file_safely(const std::string &from, const std::string &to,
                         std::string *err) {
  const std::wstring from_w = std::filesystem::path(from).wstring();
  const std::wstring to_w = std::filesystem::path(to).wstring();
  if (MoveFileExW(from_w.c_str(), to_w.c_str(),
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
  if (options.export_format == CWvExportFormat::Csv) {
    return std::make_unique<CsvWriter>();
  }
  if (options.export_format == CWvExportFormat::Clipboard) {
    return std::make_unique<ClipboardWriter>();
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
  auto is_canceled = [this]() {
    return cancel_requested_.load(std::memory_order_relaxed);
  };

  if (is_canceled()) {
    last_error_ = "export canceled by user.";
    return false;
  }

  if (source_path.empty()) {
    last_error_ = "source_path is empty.";
    return false;
  }
  const bool clipboard_mode = (options.export_format == CWvExportFormat::Clipboard);
  if (!clipboard_mode && options.output_path.empty()) {
    last_error_ = "output_path is empty.";
    return false;
  }
  if (options.table_name.empty()) {
    last_error_ = "table_name is empty.";
    return false;
  }

  if (options.export_format != CWvExportFormat::Xlsx &&
      options.export_format != CWvExportFormat::Csv &&
      options.export_format != CWvExportFormat::Json &&
      options.export_format != CWvExportFormat::Clipboard) {
    last_error_ =
        "export_format is not implemented yet. Current implementation: Xlsx, Csv, Json, Clipboard.";
    return false;
  }
  if (options.export_format == CWvExportFormat::Csv && !options.csv_use_utf8) {
    last_error_ = "ANSI CSV is disabled. Use UTF-8 CSV only.";
    return false;
  }

  CWvExportOptions runtime_opt = options;
  std::string final_path;
  if (!clipboard_mode) {
    if (!prepare_output_path(options.output_path, runtime_opt.export_format,
                             &final_path, &last_error_)) {
      return false;
    }
  } else {
    if (runtime_opt.max_rows_per_file > 0) {
      last_error_ = "clipboard export does not support max_rows_per_file split.";
      return false;
    }
    if (runtime_opt.clipboard_chunk_rows > 0 && !runtime_opt.clipboard_chunk_confirm) {
      last_error_ =
          "clipboard_chunk_confirm callback is required when clipboard_chunk_rows is enabled.";
      return false;
    }
  }
  std::string err;
  std::unique_ptr<IDataSourceProvider> provider = make_provider(runtime_opt.source_type, &err);
  if (!provider) {
    last_error_ = err;
    return false;
  }
  struct ActiveProviderGuard {
    std::mutex *mu;
    IDataSourceProvider **slot;
    ~ActiveProviderGuard() {
      std::lock_guard<std::mutex> lock(*mu);
      *slot = nullptr;
    }
  } active_provider_guard{&active_provider_mu_, &active_provider_};
  {
    std::lock_guard<std::mutex> lock(active_provider_mu_);
    active_provider_ = provider.get();
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
  const bool split_enabled = (!clipboard_mode && runtime_opt.max_rows_per_file > 0);
  const bool clipboard_chunk_mode =
      (clipboard_mode && runtime_opt.clipboard_chunk_rows > 0 &&
       static_cast<bool>(runtime_opt.clipboard_chunk_confirm));
  int part_index = 1;
  int row_index_in_file = 0;
  int rows_in_current_file = 0;
  int total_rows_exported = 0;
  int clipboard_chunks_copied = 0;
  bool stopped_by_user = false;
  std::string current_final_path;
  std::string current_temp_path;
  std::vector<std::string> output_paths;
  std::unique_ptr<IExportWriter> writer;
  auto finalize_result = [&]() {
    if (!result) {
      return;
    }
    result->rows_exported = total_rows_exported;
    result->columns_exported = (int)resolved.size();
    result->output_paths = output_paths;
    result->clipboard_chunks_copied = clipboard_chunks_copied;
    result->stopped_by_user = stopped_by_user;
    if (clipboard_mode) {
      result->output_path = "clipboard://plain-text";
    } else {
      result->output_path = output_paths.empty() ? "" : output_paths.front();
    }
  };

  auto cleanup_current_temp = [&writer, &current_temp_path]() {
    writer.reset();
    if (!current_temp_path.empty()) {
      std::error_code ec;
      std::filesystem::remove(current_temp_path, ec);
      current_temp_path.clear();
    }
  };
  auto open_part = [&](int idx) -> bool {
    if (clipboard_mode) {
      current_final_path.clear();
      current_temp_path.clear();
      runtime_opt.output_path.clear();
    } else {
      current_final_path = make_part_output_path(final_path, idx, split_enabled);
      current_temp_path = make_unique_temp_path(current_final_path);
      runtime_opt.output_path = current_temp_path;
    }

    writer = make_writer(runtime_opt, &err);
    if (!writer) {
      return false;
    }
    if (!writer->Begin(runtime_opt, resolved, &err)) {
      return false;
    }
    row_index_in_file = 0;
    rows_in_current_file = 0;

    const bool should_write_header =
        runtime_opt.include_header && (!clipboard_chunk_mode || idx == 1);
    if (should_write_header) {
      if (!writer->WriteHeader(resolved, &err)) {
        return false;
      }
      row_index_in_file = 1;
    }
    return true;
  };
  auto close_part = [&](bool commit) -> bool {
    if (!writer) {
      return true;
    }
    if (!writer->End(&err)) {
      return false;
    }
    writer.reset();

    if (clipboard_mode) {
      ++clipboard_chunks_copied;
      return true;
    }

    if (!commit) {
      std::error_code ec;
      std::filesystem::remove(current_temp_path, ec);
      current_temp_path.clear();
      return true;
    }
    if (!replace_file_safely(current_temp_path, current_final_path, &last_error_)) {
      std::error_code ec;
      std::filesystem::remove(current_temp_path, ec);
      current_temp_path.clear();
      return false;
    }
    output_paths.push_back(current_final_path);
    current_temp_path.clear();
    return true;
  };
  auto cleanup_committed_outputs = [&output_paths]() {
    for (const auto &p : output_paths) {
      std::error_code ec;
      std::filesystem::remove(p, ec);
    }
  };
  auto cancel_and_finish = [&]() -> bool {
    last_error_ = "export canceled by user.";
    cleanup_current_temp();
    if (runtime_opt.cancel_policy == CWvCancelPolicy::DeletePartial) {
      cleanup_committed_outputs();
      output_paths.clear();
    }
    finalize_result();
    return false;
  };

  if (!open_part(part_index)) {
    last_error_ = err;
    cleanup_current_temp();
    return false;
  }
  if (is_canceled()) {
    return cancel_and_finish();
  }

  bool has_row = false;
  while (provider->Step(&has_row, &err) && has_row) {
    if (is_canceled()) {
      return cancel_and_finish();
    }
    const bool rotate_by_split = split_enabled && rows_in_current_file >= runtime_opt.max_rows_per_file;
    const bool rotate_by_clipboard_chunk =
        clipboard_chunk_mode && rows_in_current_file >= runtime_opt.clipboard_chunk_rows;
    if (rotate_by_split || rotate_by_clipboard_chunk) {
      if (!close_part(true)) {
        if (last_error_.empty()) {
          last_error_ = err.empty() ? "failed to close current export part." : err;
        }
        cleanup_current_temp();
        return false;
      }
      if (rotate_by_clipboard_chunk) {
        CWvClipboardChunkAction action = CWvClipboardChunkAction::Continue;
        try {
          action = runtime_opt.clipboard_chunk_confirm(total_rows_exported, rows_in_current_file);
        } catch (...) {
          last_error_ = "clipboard_chunk_confirm callback threw an exception.";
          return false;
        }
        if (action == CWvClipboardChunkAction::Stop) {
          stopped_by_user = true;
          if (!last_warning_.empty()) {
            last_warning_ += "; ";
          }
          last_warning_ += "clipboard copy stopped by user.";
          finalize_result();
          return true;
        }
      }
      ++part_index;
      if (!open_part(part_index)) {
        last_error_ = err;
        cleanup_current_temp();
        return false;
      }
    }

    for (int i = 0; i < (int)resolved.size(); ++i) {
      if (is_canceled()) {
        return cancel_and_finish();
      }
      if (!writer->WriteCell(row_index_in_file, i, provider->ValueType(i),
                             provider->ValueInt64(i), provider->ValueDouble(i),
                             provider->ValueText(i), provider->ValueBytes(i), &err)) {
        last_error_ = err;
        cleanup_current_temp();
        return false;
      }
    }
    ++row_index_in_file;
    ++rows_in_current_file;
    ++total_rows_exported;
  }
  if (!err.empty()) {
    if (is_canceled() || err.find("canceled") != std::string::npos ||
        err.find("interrupted") != std::string::npos ||
        err.find("Interrupted") != std::string::npos) {
      return cancel_and_finish();
    }
    last_error_ = err;
    cleanup_current_temp();
    return false;
  }
  if (is_canceled()) {
    return cancel_and_finish();
  }
  if (!close_part(true)) {
    if (last_error_.empty()) {
      last_error_ = err.empty() ? "failed to close current export part." : err;
    }
    cleanup_current_temp();
    return false;
  }

  finalize_result();
  return true;
}

void CWvExport::RequestCancel() {
  cancel_requested_.store(true, std::memory_order_relaxed);
  std::lock_guard<std::mutex> lock(active_provider_mu_);
  if (active_provider_ != nullptr) {
    active_provider_->RequestCancel();
  }
}

bool CWvExport::IsCancelRequested() const {
  return cancel_requested_.load(std::memory_order_relaxed);
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
