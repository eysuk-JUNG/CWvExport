#include "CsvWriter.h"

#include <cmath>
#include <cstdio>
#include <string>
#include <windows.h>

namespace {
constexpr int64_t kMaxSafeJsonInt = 9007199254740991LL;
}

CsvWriter::~CsvWriter() {
  if (out_.is_open()) {
    out_.close();
  }
}

bool CsvWriter::Begin(const CWvExportOptions &options,
                      const std::vector<ResolvedColumn> &resolved,
                      std::string *err) {
  headers_.clear();
  headers_.reserve(resolved.size());
  for (const auto &col : resolved) {
    headers_.push_back(col.map.header_name);
  }

  include_header_ = options.include_header;
  large_integer_as_text_ = options.large_integer_as_text;
  csv_use_utf8_ = options.csv_use_utf8;
  csv_use_crlf_ = options.csv_use_crlf;

  out_.open(options.output_path, std::ios::binary | std::ios::trunc);
  if (!out_.is_open()) {
    *err = "failed to open CSV output file.";
    return false;
  }

  ResetCurrentRow();
  return true;
}

bool CsvWriter::WriteHeader(const std::vector<ResolvedColumn> &resolved,
                            std::string *err) {
  (void)resolved;
  if (!include_header_) {
    return true;
  }
  return WriteCsvRow(headers_, err);
}

bool CsvWriter::WriteCell(int row, int col, ExportValueType value_type, int64_t v_int,
                          double v_double, const char *v_text, int blob_bytes,
                          std::string *err) {
  if (col < 0 || col >= static_cast<int>(headers_.size())) {
    *err = "column index out of range while writing CSV.";
    return false;
  }
  if (current_row_index_ != row) {
    if (!FlushCurrentRow(err)) {
      return false;
    }
    current_row_index_ = row;
  }

  RowCell &cell = current_row_[static_cast<size_t>(col)];
  cell.type = value_type;
  cell.v_int = v_int;
  cell.v_double = v_double;
  cell.v_text = v_text ? v_text : "";
  cell.blob_bytes = blob_bytes;
  return true;
}

bool CsvWriter::End(std::string *err) {
  if (!FlushCurrentRow(err)) {
    return false;
  }
  if (!out_.is_open()) {
    *err = "CSV output file is not open.";
    return false;
  }
  out_.flush();
  if (!out_) {
    *err = "failed to flush CSV output file.";
    return false;
  }
  out_.close();
  return true;
}

void CsvWriter::AppendEscapedCsvCell(const std::string &in, std::string *out) {
  bool need_quote = false;
  for (char ch : in) {
    if (ch == ',' || ch == '"' || ch == '\r' || ch == '\n') {
      need_quote = true;
      break;
    }
  }
  if (!need_quote) {
    out->append(in);
    return;
  }

  out->push_back('"');
  for (char ch : in) {
    if (ch == '"') {
      out->push_back('"');
      out->push_back('"');
    } else {
      out->push_back(ch);
    }
  }
  out->push_back('"');
}

std::string CsvWriter::FormatCellValue(const RowCell &cell, bool large_integer_as_text) {
  if (cell.type == ExportValueType::Null) {
    return std::string();
  }
  if (cell.type == ExportValueType::Integer) {
    if (large_integer_as_text &&
        (cell.v_int > kMaxSafeJsonInt || cell.v_int < -kMaxSafeJsonInt)) {
      return std::to_string(cell.v_int);
    }
    return std::to_string(cell.v_int);
  }
  if (cell.type == ExportValueType::Float) {
    if (std::isfinite(cell.v_double) == 0) {
      return std::string();
    }
    char buf[64];
    std::snprintf(buf, sizeof(buf), "%.17g", cell.v_double);
    return std::string(buf);
  }
  if (cell.type == ExportValueType::Blob) {
    char buf[64];
    std::snprintf(buf, sizeof(buf), "[BLOB %d bytes]", cell.blob_bytes);
    return std::string(buf);
  }
  return cell.v_text;
}

bool CsvWriter::WriteCsvRow(const std::vector<std::string> &cells, std::string *err) {
  if (!out_.is_open()) {
    *err = "CSV output file is not open.";
    return false;
  }

  std::string line;
  for (size_t i = 0; i < cells.size(); ++i) {
    if (i > 0) {
      line.push_back(',');
    }
    AppendEscapedCsvCell(cells[i], &line);
  }
  if (csv_use_crlf_) {
    line.append("\r\n");
  } else {
    line.push_back('\n');
  }
  return WriteEncodedLine(line, err);
}

bool CsvWriter::FlushCurrentRow(std::string *err) {
  if (current_row_index_ < 0) {
    return true;
  }

  std::vector<std::string> cells;
  cells.reserve(current_row_.size());
  for (const auto &cell : current_row_) {
    cells.push_back(FormatCellValue(cell, large_integer_as_text_));
  }
  if (!WriteCsvRow(cells, err)) {
    return false;
  }

  current_row_.assign(headers_.size(), RowCell{});
  current_row_index_ = -1;
  return true;
}

void CsvWriter::ResetCurrentRow() {
  current_row_.assign(headers_.size(), RowCell{});
  current_row_index_ = -1;
}

bool CsvWriter::WriteEncodedLine(const std::string &line, std::string *err) {
  if (csv_use_utf8_) {
    out_.write(line.data(), static_cast<std::streamsize>(line.size()));
    if (!out_) {
      *err = "failed to write CSV row.";
      return false;
    }
    return true;
  }

  std::string ansi;
  if (!Utf8ToAnsi(line, &ansi)) {
    *err = "failed to convert CSV row from UTF-8 to ANSI.";
    return false;
  }
  out_.write(ansi.data(), static_cast<std::streamsize>(ansi.size()));
  if (!out_) {
    *err = "failed to write CSV row.";
    return false;
  }
  return true;
}

bool CsvWriter::Utf8ToAnsi(const std::string &utf8, std::string *ansi_out) {
  ansi_out->clear();
  if (utf8.empty()) {
    return true;
  }

  const int wide_len = MultiByteToWideChar(CP_UTF8, 0, utf8.data(),
                                           static_cast<int>(utf8.size()), nullptr, 0);
  if (wide_len <= 0) {
    return false;
  }
  std::wstring wide(static_cast<size_t>(wide_len), L'\0');
  if (MultiByteToWideChar(CP_UTF8, 0, utf8.data(), static_cast<int>(utf8.size()),
                          &wide[0], wide_len) <= 0) {
    return false;
  }

  const int ansi_len = WideCharToMultiByte(CP_ACP, 0, wide.data(), wide_len, nullptr, 0,
                                           nullptr, nullptr);
  if (ansi_len <= 0) {
    return false;
  }
  ansi_out->assign(static_cast<size_t>(ansi_len), '\0');
  if (ansi_len == 0) {
    return true;
  }
  if (WideCharToMultiByte(CP_ACP, 0, wide.data(), wide_len, &(*ansi_out)[0], ansi_len,
                          nullptr, nullptr) <= 0) {
    return false;
  }
  return true;
}
