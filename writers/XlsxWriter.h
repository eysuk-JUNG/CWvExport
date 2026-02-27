#ifndef TESTEXPORT_XLSXWRITER_H
#define TESTEXPORT_XLSXWRITER_H

#include "IExportWriter.h"

#include "cxlsx/cxlsx.h"

class XlsxWriter final : public IExportWriter {
public:
  ~XlsxWriter() override;

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
  bool large_integer_as_text_ = true;
  cxlsx_workbook *wb_ = nullptr;
  cxlsx_worksheet *ws_ = nullptr;
};

#endif
