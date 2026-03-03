#ifndef TESTEXPORT_CSVWRITER_H
#define TESTEXPORT_CSVWRITER_H

#include "IExportWriter.h"

#include <fstream>
#include <string>
#include <vector>

class CsvWriter final : public IExportWriter {
public:
  ~CsvWriter() override;

  bool Begin(const CWvExportOptions &options,
             const std::vector<ResolvedColumn> &resolved,
             std::string *err) override;
  bool WriteHeader(const std::vector<ResolvedColumn> &resolved,
                   std::string *err) override;
  bool WriteCell(int row, int col, ExportValueType value_type, int64_t v_int,
                 double v_double, const char *v_text, int blob_bytes,
                 std::string *err) override;
  bool End(std::string *err) override;

private:
  struct RowCell {
    ExportValueType type = ExportValueType::Null;
    int64_t v_int = 0;
    double v_double = 0.0;
    std::string v_text;
    int blob_bytes = 0;
  };

  static void AppendEscapedCsvCell(const std::string &in, std::string *out);
  static std::string FormatCellValue(const RowCell &cell, bool large_integer_as_text);
  bool WriteCsvRow(const std::vector<std::string> &cells, std::string *err);
  bool FlushCurrentRow(std::string *err);
  void ResetCurrentRow();
  bool WriteEncodedLine(const std::string &line, std::string *err);
  static bool Utf8ToAnsi(const std::string &utf8, std::string *ansi_out);

  std::ofstream out_;
  std::vector<std::string> headers_;
  std::vector<RowCell> current_row_;
  int current_row_index_ = -1;
  bool include_header_ = true;
  bool large_integer_as_text_ = true;
  bool csv_use_utf8_ = true;
  bool csv_use_crlf_ = true;
};

#endif
