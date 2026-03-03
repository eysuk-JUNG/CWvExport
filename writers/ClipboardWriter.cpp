#include "ClipboardWriter.h"

#include <cmath>
#include <cstdio>
#include <limits>
#include <string>

bool ClipboardWriter::Begin(const CWvExportOptions &options,
                            const std::vector<ResolvedColumn> &resolved,
                            std::string *err) {
  headers_.clear();
  headers_.reserve(resolved.size());
  for (const auto &col : resolved) {
    headers_.push_back(col.map.header_name);
  }
  current_row_.assign(headers_.size(), RowCell{});
  current_row_index_ = -1;
  include_header_ = options.include_header;
  large_integer_as_text_ = options.large_integer_as_text;
  max_clipboard_bytes_ = options.max_clipboard_bytes;
  max_clipboard_rows_ = options.max_clipboard_rows;
  data_rows_written_ = 0;
  buffer_.clear();
  if (max_clipboard_bytes_ == 0) {
    *err = "max_clipboard_bytes must be greater than zero for clipboard export.";
    return false;
  }
  if (max_clipboard_rows_ <= 0) {
    *err = "max_clipboard_rows must be greater than zero for clipboard export.";
    return false;
  }

  if (options.clipboard_format != CWvClipboardFormat::PlainText) {
    *err = "only PlainText clipboard format is currently supported.";
    return false;
  }

  backend_ = CreateClipboardBackend(err);
  if (!backend_) {
    if (err->empty()) {
      *err = "clipboard backend initialization failed.";
    }
    return false;
  }
  return true;
}

bool ClipboardWriter::WriteHeader(const std::vector<ResolvedColumn> &resolved,
                                  std::string *err) {
  if (!include_header_) {
    return true;
  }
  std::vector<std::string> header_cells;
  header_cells.reserve(resolved.size());
  for (const auto &col : resolved) {
    header_cells.push_back(col.map.header_name);
  }
  return AppendLine(header_cells, err);
}

bool ClipboardWriter::WriteCell(int row, int col, ExportValueType value_type, int64_t v_int,
                                double v_double, const char *v_text, int blob_bytes,
                                std::string *err) {
  if (col < 0 || col >= static_cast<int>(headers_.size())) {
    *err = "column index out of range while writing clipboard.";
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
  cell.v_text = (v_text != nullptr) ? v_text : "";
  cell.blob_bytes = blob_bytes;
  return true;
}

bool ClipboardWriter::End(std::string *err) {
  if (!FlushCurrentRow(err)) {
    return false;
  }
  if (!CheckSizeLimit(err)) {
    return false;
  }
  if (!backend_) {
    *err = "clipboard backend is not initialized.";
    return false;
  }
  return backend_->SetPlainTextUtf8(buffer_, err);
}

bool ClipboardWriter::FlushCurrentRow(std::string *err) {
  if (current_row_index_ < 0) {
    return true;
  }
  if (data_rows_written_ >= max_clipboard_rows_) {
    *err = "clipboard row count exceeded max_clipboard_rows (" +
           std::to_string(max_clipboard_rows_) + ").";
    return false;
  }

  std::vector<std::string> row_values;
  row_values.reserve(current_row_.size());
  for (const RowCell &cell : current_row_) {
    if (cell.type == ExportValueType::Null) {
      row_values.emplace_back();
      continue;
    }
    if (cell.type == ExportValueType::Integer) {
      row_values.push_back(std::to_string(cell.v_int));
      continue;
    }
    if (cell.type == ExportValueType::Float) {
      if (std::isfinite(cell.v_double) != 0) {
        char num_buf[64];
        std::snprintf(num_buf, sizeof(num_buf), "%.17g", cell.v_double);
        row_values.push_back(num_buf);
      } else {
        row_values.emplace_back();
      }
      continue;
    }
    if (cell.type == ExportValueType::Blob) {
      char blob_buf[64];
      std::snprintf(blob_buf, sizeof(blob_buf), "[BLOB %d bytes]", cell.blob_bytes);
      row_values.push_back(blob_buf);
      continue;
    }
    row_values.push_back(cell.v_text);
  }

  current_row_.assign(headers_.size(), RowCell{});
  current_row_index_ = -1;
  if (!AppendLine(row_values, err)) {
    return false;
  }
  ++data_rows_written_;
  return true;
}

bool ClipboardWriter::AppendLine(const std::vector<std::string> &cells, std::string *err) {
  std::string line;
  for (size_t i = 0; i < cells.size(); ++i) {
    if (i > 0) {
      line.push_back('\t');
    }
    AppendEscapedTsvCell(cells[i], &line);
  }
  line.append("\r\n");
  buffer_ += line;
  return CheckSizeLimit(err);
}

void ClipboardWriter::AppendEscapedTsvCell(const std::string &in, std::string *out) {
  out->reserve(out->size() + in.size());
  for (char ch : in) {
    if (ch == '\t' || ch == '\r' || ch == '\n') {
      out->push_back(' ');
    } else {
      out->push_back(ch);
    }
  }
}

bool ClipboardWriter::CheckSizeLimit(std::string *err) const {
  if (buffer_.size() > max_clipboard_bytes_) {
    *err = "clipboard payload exceeded max_clipboard_bytes (" +
           std::to_string(max_clipboard_bytes_) + ").";
    return false;
  }
  return true;
}
