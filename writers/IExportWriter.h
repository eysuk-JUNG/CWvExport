#ifndef TESTEXPORT_IEXPORTWRITER_H
#define TESTEXPORT_IEXPORTWRITER_H

#include "../CWvExport.h"
#include "../core/ExportCoreTypes.h"

#include <string>
#include <vector>

class IExportWriter {
public:
  virtual ~IExportWriter() = default;
  virtual bool Begin(const CWvExportOptions &options,
                     const std::vector<ResolvedColumn> &resolved,
                     std::string *err) = 0;
  virtual bool WriteHeader(const std::vector<ResolvedColumn> &resolved,
                           std::string *err) = 0;
  virtual bool WriteCell(int row, int col, ExportValueType value_type,
                         int64_t v_int, double v_double, const char *v_text,
                         int blob_bytes, std::string *err) = 0;
  virtual bool End(std::string *err) = 0;
};

#endif
