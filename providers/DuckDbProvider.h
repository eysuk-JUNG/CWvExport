#ifndef TESTEXPORT_DUCKDBPROVIDER_H
#define TESTEXPORT_DUCKDBPROVIDER_H

#include "IDataSourceProvider.h"

#include "../../vendor/duckdb-bin/duckdb.h"

#include <string>
#include <vector>

class DuckDbProvider final : public IDataSourceProvider {
public:
  ~DuckDbProvider() override;

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
  void ResetQueryResult();
  static std::string QuoteIdentifier(const std::string &name);

  duckdb_database db_ = nullptr;
  duckdb_connection conn_ = nullptr;
  duckdb_result result_{};
  bool has_result_ = false;
  idx_t current_row_ = 0;
  bool row_active_ = false;
  mutable std::string text_buf_;
};

#endif
