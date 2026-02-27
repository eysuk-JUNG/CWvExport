#ifndef TESTEXPORT_DUCKDBPROVIDER_H
#define TESTEXPORT_DUCKDBPROVIDER_H

#include "IDataSourceProvider.h"

#include "../../vendor/duckdb-bin/duckdb.h"

#include <string>
#include <vector>

class DuckDbProvider final : public IDataSourceProvider {
public:
  ~DuckDbProvider() override;

  void RequestCancel() override;
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
  struct CachedCell {
    ExportValueType type = ExportValueType::Null;
    int64_t v_int = 0;
    double v_double = 0.0;
    std::string v_text;
    int blob_bytes = 0;
  };

  void ResetStatement();
  void ResetQueryResult();
  bool LoadCurrentRowCache(std::string *err);
  static std::string QuoteIdentifier(const std::string &name);

  duckdb_database db_ = nullptr;
  duckdb_connection conn_ = nullptr;
  duckdb_prepared_statement stmt_ = nullptr;
  duckdb_result result_{};
  duckdb_data_chunk current_chunk_ = nullptr;
  bool has_result_ = false;
  idx_t current_chunk_row_ = 0;
  idx_t current_chunk_size_ = 0;
  bool row_active_ = false;
  std::vector<duckdb_type> col_types_;
  std::vector<CachedCell> row_cache_;
};

#endif
