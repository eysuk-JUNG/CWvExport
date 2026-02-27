#include "XlsxWriter.h"

#include <cstdio>
#include <string>

namespace {
bool check_cxlsx_rc(int rc, const char *op, std::string *err) {
  if (rc == CXLSX_OK) {
    return true;
  }
  *err = op;
  *err += " failed: ";
  *err += cxlsx_strerror(rc);
  return false;
}
} // namespace

XlsxWriter::~XlsxWriter() {
  if (wb_ != nullptr) {
    cxlsx_workbook_close(wb_);
    wb_ = nullptr;
  }
}

bool XlsxWriter::Begin(const CWvExportOptions &options,
                       const std::vector<ResolvedColumn> &resolved,
                       std::string *err) {
  (void)resolved;
  large_integer_as_text_ = options.large_integer_as_text;
  wb_ = cxlsx_workbook_new(options.output_path.c_str());
  if (wb_ == nullptr) {
    *err = "cxlsx_workbook_new failed (license/init/path check).";
    return false;
  }
  if (!options.password.empty()) {
    cxlsx_workbook_set_password(wb_, options.password.c_str());
  }
  const char *sheet_name = options.sheet_name.empty() ? options.table_name.c_str()
                                                       : options.sheet_name.c_str();
  ws_ = cxlsx_workbook_add_worksheet(wb_, sheet_name);
  if (ws_ == nullptr) {
    *err = "cxlsx_workbook_add_worksheet failed.";
    return false;
  }
  return true;
}

bool XlsxWriter::WriteHeader(const std::vector<ResolvedColumn> &resolved,
                             std::string *err) {
  for (size_t i = 0; i < resolved.size(); ++i) {
    if (!check_cxlsx_rc(cxlsx_worksheet_write_string(
                            ws_, 0, (int)i, resolved[i].map.header_name.c_str(), nullptr),
                        "cxlsx_worksheet_write_string(header)", err)) {
      return false;
    }
  }
  return true;
}

bool XlsxWriter::WriteCell(int row, int col, ExportValueType value_type, int64_t v_int,
                           double v_double, const char *v_text, int blob_bytes,
                           std::string *err) {
  if (value_type == ExportValueType::Null) {
    return check_cxlsx_rc(cxlsx_worksheet_write_blank(ws_, row, col, nullptr),
                          "cxlsx_worksheet_write_blank", err);
  }
  if (value_type == ExportValueType::Integer) {
    const int64_t max_safe_int = 9007199254740991LL;
    if (large_integer_as_text_ && (v_int > max_safe_int || v_int < -max_safe_int)) {
      std::string text = std::to_string(v_int);
      return check_cxlsx_rc(cxlsx_worksheet_write_string(ws_, row, col, text.c_str(),
                                                         nullptr),
                            "cxlsx_worksheet_write_string(integer-as-text)", err);
    } else {
      return check_cxlsx_rc(
          cxlsx_worksheet_write_number(ws_, row, col, (double)v_int, nullptr),
          "cxlsx_worksheet_write_number(integer)", err);
    }
  }
  if (value_type == ExportValueType::Float) {
    return check_cxlsx_rc(cxlsx_worksheet_write_number(ws_, row, col, v_double, nullptr),
                          "cxlsx_worksheet_write_number(float)", err);
  }
  if (value_type == ExportValueType::Blob) {
    char buf[64];
    snprintf(buf, sizeof(buf), "[BLOB %d bytes]", blob_bytes);
    return check_cxlsx_rc(cxlsx_worksheet_write_string(ws_, row, col, buf, nullptr),
                          "cxlsx_worksheet_write_string(blob-marker)", err);
  }
  return check_cxlsx_rc(
      cxlsx_worksheet_write_string(ws_, row, col, v_text ? v_text : "", nullptr),
      "cxlsx_worksheet_write_string(text)", err);
}

bool XlsxWriter::End(std::string *err) {
  int rc = cxlsx_workbook_close(wb_);
  wb_ = nullptr;
  ws_ = nullptr;
  if (rc != CXLSX_OK) {
    *err = "cxlsx_workbook_close failed: ";
    *err += cxlsx_strerror(rc);
    return false;
  }
  return true;
}
