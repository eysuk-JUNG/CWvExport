#include "JsonWriterRapid.h"

#include <cmath>
#include <cstdio>
#include <limits>
#include <string>

#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

namespace {
bool write_number_or_string_int64(bool large_integer_as_text, int64_t value,
                                  rapidjson::Writer<rapidjson::StringBuffer> *writer) {
  const int64_t max_safe_int = 9007199254740991LL;
  if (large_integer_as_text && (value > max_safe_int || value < -max_safe_int)) {
    const std::string text = std::to_string(value);
    return writer->String(text.c_str(),
                          static_cast<rapidjson::SizeType>(text.size()));
  }
  return writer->Int64(value);
}
} // namespace

JsonWriterRapid::~JsonWriterRapid() {
  if (out_.is_open()) {
    out_.close();
  }
}

bool JsonWriterRapid::Begin(const CWvExportOptions &options,
                            const std::vector<ResolvedColumn> &resolved,
                            std::string *err) {
  headers_.clear();
  headers_.reserve(resolved.size());
  for (const auto &col : resolved) {
    headers_.push_back(col.map.header_name);
  }

  large_integer_as_text_ = options.large_integer_as_text;
  include_header_ = options.include_header;
  out_.open(options.output_path, std::ios::binary | std::ios::trunc);
  if (!out_.is_open()) {
    *err = "failed to open JSON output file.";
    return false;
  }
  out_ << "{\"columns\":[";
  if (include_header_) {
    for (size_t i = 0; i < headers_.size(); ++i) {
      rapidjson::StringBuffer sb;
      rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
      if (!writer.String(headers_[i].c_str(),
                         static_cast<rapidjson::SizeType>(headers_[i].size()))) {
        *err = "rapidjson failed to encode column header.";
        return false;
      }
      if (i > 0) {
        out_ << ",";
      }
      out_ << sb.GetString();
    }
  }
  out_ << "],\"rows\":[";
  first_row_written_ = false;
  ResetCurrentRow();
  return true;
}

bool JsonWriterRapid::WriteHeader(const std::vector<ResolvedColumn> &resolved,
                                  std::string *err) {
  (void)resolved;
  (void)err;
  return true;
}

bool JsonWriterRapid::WriteCell(int row, int col, ExportValueType value_type, int64_t v_int,
                                double v_double, const char *v_text, int blob_bytes,
                                std::string *err) {
  if (col < 0 || col >= static_cast<int>(headers_.size())) {
    *err = "column index out of range while writing JSON.";
    return false;
  }
  if (current_row_index_ != row) {
    if (!FlushCurrentRow(err)) {
      return false;
    }
    current_row_index_ = row;
  }

  RowCell &cell = current_row_[col];
  cell.type = value_type;
  cell.v_int = v_int;
  cell.v_double = v_double;
  cell.blob_bytes = blob_bytes;
  cell.v_text = v_text ? v_text : "";
  return true;
}

bool JsonWriterRapid::End(std::string *err) {
  if (!FlushCurrentRow(err)) {
    return false;
  }
  if (!out_.is_open()) {
    *err = "JSON output file is not open.";
    return false;
  }
  out_ << "]}";
  out_.flush();
  if (!out_) {
    *err = "failed to flush JSON output file.";
    return false;
  }
  out_.close();
  return true;
}

void JsonWriterRapid::ResetCurrentRow() {
  current_row_index_ = -1;
  current_row_.assign(headers_.size(), RowCell{});
}

bool JsonWriterRapid::FlushCurrentRow(std::string *err) {
  if (current_row_index_ < 0) {
    return true;
  }
  if (!out_.is_open()) {
    *err = "JSON output file is not open.";
    return false;
  }

  rapidjson::StringBuffer sb;
  rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
  if (!writer.StartArray()) {
    *err = "rapidjson failed to start row array.";
    return false;
  }
  for (size_t i = 0; i < headers_.size(); ++i) {
    const RowCell &cell = current_row_[i];

    if (cell.type == ExportValueType::Null) {
      if (!writer.Null()) {
        *err = "rapidjson failed to write null value.";
        return false;
      }
      continue;
    }
    if (cell.type == ExportValueType::Integer) {
      if (!write_number_or_string_int64(large_integer_as_text_, cell.v_int, &writer)) {
        *err = "rapidjson failed to write integer value.";
        return false;
      }
      continue;
    }
    if (cell.type == ExportValueType::Float) {
      if (std::isfinite(cell.v_double) != 0) {
        if (!writer.Double(cell.v_double)) {
          *err = "rapidjson failed to write float value.";
          return false;
        }
      } else {
        if (!writer.Null()) {
          *err = "rapidjson failed to write non-finite fallback null.";
          return false;
        }
      }
      continue;
    }
    if (cell.type == ExportValueType::Blob) {
      char buf[64];
      snprintf(buf, sizeof(buf), "[BLOB %d bytes]", cell.blob_bytes);
      if (!writer.String(buf)) {
        *err = "rapidjson failed to write blob marker.";
        return false;
      }
      continue;
    }
    if (!writer.String(cell.v_text.c_str(),
                       static_cast<rapidjson::SizeType>(cell.v_text.size()))) {
      *err = "rapidjson failed to write text value.";
      return false;
    }
  }
  if (!writer.EndArray()) {
    *err = "rapidjson failed to end row array.";
    return false;
  }

  if (first_row_written_) {
    out_ << ",";
  }
  out_ << sb.GetString();
  if (!out_) {
    *err = "failed to write JSON row.";
    return false;
  }

  first_row_written_ = true;
  current_row_.assign(headers_.size(), RowCell{});
  current_row_index_ = -1;
  return true;
}
