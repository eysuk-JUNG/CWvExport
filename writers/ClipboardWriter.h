#ifndef TESTEXPORT_CLIPBOARDWRITER_H
#define TESTEXPORT_CLIPBOARDWRITER_H

#include "IExportWriter.h"
#include "clipboard/IClipboardBackend.h"

#include <memory>
#include <string>
#include <vector>

class ClipboardWriter final : public IExportWriter {
public:
  ~ClipboardWriter() override = default;

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
  bool AppendLine(const std::vector<std::string> &cells, std::string *err);
  static void AppendEscapedTsvCell(const std::string &in, std::string *out);
  bool CheckSizeLimit(std::string *err) const;

  std::unique_ptr<IClipboardBackend> backend_;
  std::vector<std::string> headers_;
  std::vector<RowCell> current_row_;
  int current_row_index_ = -1;
  bool include_header_ = true;
  bool large_integer_as_text_ = true;
  size_t max_clipboard_bytes_ = 0;
  int max_clipboard_rows_ = 0;
  int data_rows_written_ = 0;
  std::string buffer_;
};

#endif
