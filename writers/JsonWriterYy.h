#ifndef TESTEXPORT_JSONWRITERYY_H
#define TESTEXPORT_JSONWRITERYY_H

#include "IExportWriter.h"

#include <fstream>
#include <string>
#include <vector>

class JsonWriterYy final : public IExportWriter {
public:
  ~JsonWriterYy() override;

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

  bool FlushCurrentRow(std::string *err);
  void ResetCurrentRow();

  std::ofstream out_;
  std::vector<std::string> headers_;
  std::vector<RowCell> current_row_;
  int current_row_index_ = -1;
  bool first_row_written_ = false;
  bool large_integer_as_text_ = true;
  bool include_header_ = true;
};

#endif
