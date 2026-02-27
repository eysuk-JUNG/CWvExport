#ifndef TESTEXPORT_IDATASOURCEPROVIDER_H
#define TESTEXPORT_IDATASOURCEPROVIDER_H

#include "../core/ExportCoreTypes.h"

#include <string>
#include <vector>

class IDataSourceProvider {
public:
  virtual ~IDataSourceProvider() = default;
  virtual bool OpenReadOnly(const std::string &path, std::string *err) = 0;
  virtual bool LoadTableSchema(const std::string &table_name,
                               std::vector<DbColumnInfo> *cols,
                               std::string *err) = 0;
  virtual bool PrepareSelect(const std::string &table_name,
                             const std::vector<ResolvedColumn> &cols,
                             std::string *err) = 0;
  virtual bool Step(bool *has_row, std::string *err) = 0;
  virtual ExportValueType ValueType(int col) const = 0;
  virtual int64_t ValueInt64(int col) const = 0;
  virtual double ValueDouble(int col) const = 0;
  virtual const char *ValueText(int col) const = 0;
  virtual int ValueBytes(int col) const = 0;
};

#endif
