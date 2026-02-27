#include "JsonWriterYy.h"

#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <limits>
#include <string>

#include <yyjson.h>

JsonWriterYy::~JsonWriterYy() {
  if (out_.is_open()) {
    out_.close();
  }
}

bool JsonWriterYy::Begin(const CWvExportOptions &options,
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
      yyjson_mut_doc *doc = yyjson_mut_doc_new(nullptr);
      if (doc == nullptr) {
        *err = "yyjson_mut_doc_new failed.";
        return false;
      }
      yyjson_mut_val *root = yyjson_mut_strcpy(doc, headers_[i].c_str());
      if (root == nullptr) {
        yyjson_mut_doc_free(doc);
        *err = "yyjson_mut_strcpy failed.";
        return false;
      }
      yyjson_mut_doc_set_root(doc, root);
      size_t len = 0;
      char *json = yyjson_mut_write(doc, YYJSON_WRITE_NOFLAG, &len);
      yyjson_mut_doc_free(doc);
      if (json == nullptr) {
        *err = "yyjson_mut_write failed.";
        return false;
      }
      if (i > 0) {
        out_ << ",";
      }
      out_.write(json, static_cast<std::streamsize>(len));
      free(json);
    }
  }
  out_ << "],\"rows\":[";
  first_row_written_ = false;
  ResetCurrentRow();
  return true;
}

bool JsonWriterYy::WriteHeader(const std::vector<ResolvedColumn> &resolved,
                               std::string *err) {
  (void)resolved;
  (void)err;
  return true;
}

bool JsonWriterYy::WriteCell(int row, int col, ExportValueType value_type, int64_t v_int,
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

bool JsonWriterYy::End(std::string *err) {
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

void JsonWriterYy::ResetCurrentRow() {
  current_row_index_ = -1;
  current_row_.assign(headers_.size(), RowCell{});
}

bool JsonWriterYy::FlushCurrentRow(std::string *err) {
  if (current_row_index_ < 0) {
    return true;
  }
  if (!out_.is_open()) {
    *err = "JSON output file is not open.";
    return false;
  }

  yyjson_mut_doc *doc = yyjson_mut_doc_new(nullptr);
  if (doc == nullptr) {
    *err = "yyjson_mut_doc_new failed.";
    return false;
  }
  yyjson_mut_val *arr = yyjson_mut_arr(doc);
  if (arr == nullptr) {
    yyjson_mut_doc_free(doc);
    *err = "yyjson_mut_arr failed.";
    return false;
  }
  yyjson_mut_doc_set_root(doc, arr);

  for (size_t i = 0; i < headers_.size(); ++i) {
    const RowCell &cell = current_row_[i];
    bool ok = false;

    if (cell.type == ExportValueType::Null) {
      ok = yyjson_mut_arr_add_null(doc, arr);
    } else if (cell.type == ExportValueType::Integer) {
      const int64_t max_safe_int = 9007199254740991LL;
      if (large_integer_as_text_ &&
          (cell.v_int > max_safe_int || cell.v_int < -max_safe_int)) {
        const std::string v = std::to_string(cell.v_int);
        yyjson_mut_val *val = yyjson_mut_strncpy(doc, v.c_str(), static_cast<size_t>(v.size()));
        ok = (val != nullptr) && yyjson_mut_arr_append(arr, val);
      } else {
        ok = yyjson_mut_arr_add_sint(doc, arr, cell.v_int);
      }
    } else if (cell.type == ExportValueType::Float) {
      if (std::isfinite(cell.v_double) != 0) {
        ok = yyjson_mut_arr_add_real(doc, arr, cell.v_double);
      } else {
        ok = yyjson_mut_arr_add_null(doc, arr);
      }
    } else if (cell.type == ExportValueType::Blob) {
      char buf[64];
      snprintf(buf, sizeof(buf), "[BLOB %d bytes]", cell.blob_bytes);
      yyjson_mut_val *val = yyjson_mut_strcpy(doc, buf);
      ok = (val != nullptr) && yyjson_mut_arr_append(arr, val);
    } else {
      yyjson_mut_val *val = yyjson_mut_strcpy(doc, cell.v_text.c_str());
      ok = (val != nullptr) && yyjson_mut_arr_append(arr, val);
    }

    if (!ok) {
      yyjson_mut_doc_free(doc);
      *err = "yyjson failed to append JSON value.";
      return false;
    }
  }

  size_t len = 0;
  char *json = yyjson_mut_write(doc, YYJSON_WRITE_NOFLAG, &len);
  if (json == nullptr) {
    yyjson_mut_doc_free(doc);
    *err = "yyjson_mut_write failed.";
    return false;
  }

  if (first_row_written_) {
    out_ << ",";
  }
  out_.write(json, static_cast<std::streamsize>(len));
  free(json);
  yyjson_mut_doc_free(doc);

  if (!out_) {
    *err = "failed to write JSON row.";
    return false;
  }

  first_row_written_ = true;
  current_row_.assign(headers_.size(), RowCell{});
  current_row_index_ = -1;
  return true;
}
