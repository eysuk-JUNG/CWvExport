#ifndef TESTEXPORT_EXPORT_CORE_TYPES_H
#define TESTEXPORT_EXPORT_CORE_TYPES_H

#include "../CWvExport.h"

#include <string>

enum class ExportValueType {
  Null = 0,
  Integer = 1,
  Float = 2,
  Text = 3,
  Blob = 4
};

struct DbColumnInfo {
  int cid = -1;
  std::string name;
};

struct ResolvedColumn {
  CWvExportColumn map;
  int actual_cid = -1;
};

#endif
