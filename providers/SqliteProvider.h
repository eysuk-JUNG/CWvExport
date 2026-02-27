#ifndef TESTEXPORT_SQLITEPROVIDER_H
#define TESTEXPORT_SQLITEPROVIDER_H

#include "IDataSourceProvider.h"

#include <sqlite3.h>

class SqliteProvider final : public IDataSourceProvider {
public:
  ~SqliteProvider() override;

  bool OpenReadOnly(const std::string &path, std::string *err) override;
  bool LoadTableSchema(const std::string &table_name,
                       std::vector<DbColumnInfo> *cols,
                       std::string *err) override;
  bool PrepareSelect(const std::string &table_name,
                     const std::vector<ResolvedColumn> &cols,
                     std::string *err) override;
  bool Step(bool *has_row, std::string *err) override;
  ExportValueType ValueType(int col) const override;
  int64_t ValueInt64(int col) const override;
  double ValueDouble(int col) const override;
  const char *ValueText(int col) const override;
  int ValueBytes(int col) const override;

private:
  sqlite3 *db_ = nullptr;
  sqlite3_stmt *stmt_ = nullptr;
};

#endif
